package qbo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// API hosts per environment.
const (
	ProductionBaseURL = "https://quickbooks.api.intuit.com"
	SandboxBaseURL    = "https://sandbox-quickbooks.api.intuit.com"
)

// BaseURL returns the API host for an environment.
func BaseURL(environment string) string {
	if environment == "sandbox" {
		return SandboxBaseURL
	}
	return ProductionBaseURL
}

// TokenSource yields access tokens; force discards any cached token.
type TokenSource interface {
	AccessToken(ctx context.Context, force bool) (string, error)
}

// IntuitTIDHeader is the response header carrying Intuit's transaction id.
// Intuit support asks for it when troubleshooting a request.
const IntuitTIDHeader = "intuit_tid"

// Response is a raw API response, kept verbatim for archival.
type Response struct {
	URL       string
	Status    int
	Body      []byte
	IntuitTID string // value of the intuit_tid response header, if any
}

// APIError is a non-retryable (or retries-exhausted) HTTP failure.
type APIError struct {
	Status    int
	URL       string
	Body      string
	IntuitTID string
}

func (e *APIError) Error() string {
	b := e.Body
	if len(b) > 500 {
		b = b[:500] + "..."
	}
	return fmt.Sprintf("QBO API returned HTTP %d for %s%s: %s", e.Status, e.URL, tidSuffix(e.IntuitTID), b)
}

// tidSuffix formats an intuit_tid for inclusion in an error message.
func tidSuffix(tid string) string {
	if tid == "" {
		return ""
	}
	return " (intuit_tid " + tid + ")"
}

// Client is a rate-limited QBO Accounting API client for one company.
// Intuit allows 500 requests/minute and 10 concurrent requests per realm;
// the defaults (6 req/s, 4 concurrent) stay well under both.
type Client struct {
	BaseURL      string
	RealmID      string
	MinorVersion string
	Tokens       TokenSource
	HTTP         *http.Client

	MaxAttempts int           // total tries per request (default 6)
	BaseBackoff time.Duration // first retry delay (default 1s)
	MaxBackoff  time.Duration // cap per delay (default 60s)

	limiter *rate.Limiter
	sem     chan struct{}
	sleep   func(context.Context, time.Duration) error
}

// NewClient builds a client with the standard limits.
func NewClient(environment, realmID, minorVersion string, tokens TokenSource) *Client {
	return &Client{
		BaseURL: BaseURL(environment), RealmID: realmID, MinorVersion: minorVersion, Tokens: tokens,
		HTTP:        &http.Client{Timeout: 120 * time.Second},
		MaxAttempts: 6, BaseBackoff: time.Second, MaxBackoff: 60 * time.Second,
		limiter: rate.NewLimiter(rate.Limit(6), 4),
		sem:     make(chan struct{}, 4),
		sleep:   sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Get performs GET /v3/company/{realm}/{path}, always appending minorversion.
// It retries 429 and 5xx with exponential backoff + jitter (honouring
// Retry-After) and refreshes the access token once on 401. On failure the
// last Response (if any) is returned alongside the error so it can be archived.
func (c *Client) Get(ctx context.Context, path string, query url.Values) (*Response, error) {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	q.Set("minorversion", c.MinorVersion)
	full := fmt.Sprintf("%s/v3/company/%s/%s?%s", c.BaseURL, url.PathEscape(c.RealmID), path, q.Encode())

	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	var last *Response
	var lastErr error
	forceToken, refreshed := false, false
	for attempt := 1; attempt <= c.MaxAttempts; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return last, err
		}
		token, err := c.Tokens.AccessToken(ctx, forceToken)
		if err != nil {
			return last, err
		}
		forceToken = false
		resp, retryAfter, err := c.do(ctx, full, token)
		if err != nil {
			if ctx.Err() != nil {
				return last, ctx.Err()
			}
			lastErr = err // network error: retry
		} else {
			last = resp
			switch {
			case resp.Status >= 200 && resp.Status < 300:
				return resp, nil
			case resp.Status == http.StatusUnauthorized && !refreshed:
				refreshed, forceToken = true, true
				continue // retry immediately with a fresh token
			case resp.Status == http.StatusTooManyRequests || resp.Status >= 500:
				lastErr = &APIError{Status: resp.Status, URL: full, Body: string(resp.Body), IntuitTID: resp.IntuitTID}
			default:
				return resp, &APIError{Status: resp.Status, URL: full, Body: string(resp.Body), IntuitTID: resp.IntuitTID}
			}
		}
		if attempt == c.MaxAttempts {
			break
		}
		if err := c.sleep(ctx, c.backoff(attempt, retryAfter)); err != nil {
			return last, err
		}
	}
	return last, fmt.Errorf("giving up after %d attempts: %w", c.MaxAttempts, lastErr)
}

// backoff returns the delay before retry number `attempt` (1-based):
// "equal jitter" exponential backoff, never less than Retry-After.
func (c *Client) backoff(attempt int, retryAfter time.Duration) time.Duration {
	d := c.BaseBackoff << (attempt - 1)
	if d > c.MaxBackoff || d <= 0 {
		d = c.MaxBackoff
	}
	d = d/2 + rand.N(d/2+1)
	if retryAfter > d {
		d = retryAfter
	}
	return d
}

func (c *Client) do(ctx context.Context, full, token string) (*Response, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // drop the URL (it is logged elsewhere) from the message
		}
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}
	var retryAfter time.Duration
	if s := resp.Header.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(s); err == nil && secs >= 0 {
			retryAfter = time.Duration(secs) * time.Second
		} else if t, err := http.ParseTime(s); err == nil {
			retryAfter = time.Until(t)
		}
		if retryAfter > 5*time.Minute {
			retryAfter = 5 * time.Minute
		}
	}
	return &Response{URL: full, Status: resp.StatusCode, Body: body, IntuitTID: resp.Header.Get(IntuitTIDHeader)}, retryAfter, nil
}
