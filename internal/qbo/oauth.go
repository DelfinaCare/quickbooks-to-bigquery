// Package qbo talks to Intuit: OAuth2, the rate-limited Accounting API client,
// raw response archival and the extraction strategy.
package qbo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Intuit OAuth2 endpoints. The live values come from Intuit's discovery
// document (see Endpoints); these are the fallback when it cannot be read.
const (
	AuthorizeURL = "https://appcenter.intuit.com/connect/oauth2"
	TokenURL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
	RevokeURL    = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"
	Scope        = "com.intuit.quickbooks.accounting"
)

// Intuit's OpenID discovery documents, one per environment.
const (
	ProductionDiscoveryURL = "https://developer.api.intuit.com/.well-known/openid_configuration"
	SandboxDiscoveryURL    = "https://developer.api.intuit.com/.well-known/openid_sandbox_configuration"
)

// How long a discovery document is trusted, and how soon to try again after
// failing to read one (the fallback endpoints are used meanwhile).
const (
	discoveryTTL      = 24 * time.Hour
	discoveryRetry    = 5 * time.Minute
	discoveryTimeout  = 10 * time.Second
	maxDiscoveryBytes = 1 << 20
)

// Endpoints are the OAuth2 endpoints for one environment.
type Endpoints struct {
	Authorize string
	Token     string
	Revoke    string
}

// FallbackEndpoints are used when the discovery document is unavailable.
var FallbackEndpoints = Endpoints{Authorize: AuthorizeURL, Token: TokenURL, Revoke: RevokeURL}

// DiscoveryURL returns Intuit's discovery document URL for an environment.
func DiscoveryURL(environment string) string {
	if environment == "sandbox" {
		return SandboxDiscoveryURL
	}
	return ProductionDiscoveryURL
}

type cachedEndpoints struct {
	ep      Endpoints
	expires time.Time
}

// ErrInvalidGrant: the refresh token (or auth code) was rejected for good.
var ErrInvalidGrant = errors.New("intuit rejected the grant (invalid_grant)")

// Token is an OAuth2 token pair.
type Token struct {
	AccessToken      string
	RefreshToken     string
	ExpiresAt        time.Time
	RefreshExpiresAt time.Time
}

// OAuth performs token exchanges. Creds resolves the app keys for an
// environment ("production" or "sandbox"): Intuit issues separate key pairs.
//
// Endpoints are read from Intuit's discovery document for the environment and
// cached for a day; if it cannot be fetched, FallbackEndpoints are used and
// the fetch is retried a few minutes later.
type OAuth struct {
	Creds func(environment string) (id, secret string, ok bool)
	HTTP  *http.Client
	Log   *slog.Logger // optional; reports discovery failures

	// DiscoveryURLs overrides the discovery document per environment (tests).
	DiscoveryURLs map[string]string
	// TokenURL and RevokeURL, when set, bypass discovery (tests).
	TokenURL  string
	RevokeURL string

	// trustedHost reports whether a discovered endpoint's host may receive
	// our client secret; defaults to Intuit's domains.
	trustedHost func(host string) bool

	mu    sync.Mutex
	cache map[string]cachedEndpoints
}

func (o *OAuth) client() *http.Client {
	if o.HTTP != nil {
		return o.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func isIntuitHost(host string) bool {
	host = strings.ToLower(host)
	return host == "intuit.com" || strings.HasSuffix(host, ".intuit.com")
}

// Endpoints returns the OAuth2 endpoints for an environment, from Intuit's
// discovery document when it can be read and FallbackEndpoints otherwise.
func (o *OAuth) Endpoints(ctx context.Context, environment string) Endpoints {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	if c, ok := o.cache[environment]; ok && now.Before(c.expires) {
		return c.ep
	}
	docURL := o.DiscoveryURLs[environment]
	if docURL == "" {
		docURL = DiscoveryURL(environment)
	}
	ep, err := o.discover(ctx, docURL)
	c := cachedEndpoints{ep: ep, expires: now.Add(discoveryTTL)}
	if err != nil {
		if o.Log != nil {
			o.Log.Warn("could not read the Intuit discovery document; using built-in OAuth endpoints",
				"environment", environment, "url", docURL, "error", err.Error())
		}
		c = cachedEndpoints{ep: FallbackEndpoints, expires: now.Add(discoveryRetry)}
	}
	if o.cache == nil {
		o.cache = map[string]cachedEndpoints{}
	}
	o.cache[environment] = c
	return c.ep
}

func (o *OAuth) discover(ctx context.Context, docURL string) (Endpoints, error) {
	ctx, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, docURL, nil)
	if err != nil {
		return Endpoints{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := o.client().Do(req)
	if err != nil {
		return Endpoints{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Endpoints{}, fmt.Errorf("HTTP %d%s", resp.StatusCode, tidSuffix(resp.Header.Get(IntuitTIDHeader)))
	}
	var doc struct {
		Authorization string `json:"authorization_endpoint"`
		Token         string `json:"token_endpoint"`
		Revocation    string `json:"revocation_endpoint"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxDiscoveryBytes)).Decode(&doc); err != nil {
		return Endpoints{}, fmt.Errorf("decode: %w", err)
	}
	trusted := o.trustedHost
	if trusted == nil {
		trusted = isIntuitHost
	}
	ep := Endpoints{Authorize: doc.Authorization, Token: doc.Token, Revoke: doc.Revocation}
	for name, v := range map[string]string{"authorization_endpoint": ep.Authorize, "token_endpoint": ep.Token, "revocation_endpoint": ep.Revoke} {
		u, err := url.Parse(v)
		if v == "" || err != nil || u.Scheme != "https" || !trusted(u.Hostname()) {
			return Endpoints{}, fmt.Errorf("%s %q is missing or not an https Intuit URL", name, v)
		}
	}
	return ep, nil
}

// AuthCodeURL builds the Intuit consent-screen URL.
func (o *OAuth) AuthCodeURL(ctx context.Context, environment, redirectURI, state string) (string, error) {
	id, _, ok := o.Creds(environment)
	if !ok {
		return "", fmt.Errorf("no Intuit credentials configured for %s", environment)
	}
	q := url.Values{"client_id": {id}, "redirect_uri": {redirectURI}, "response_type": {"code"}, "scope": {Scope}, "state": {state}}
	return o.Endpoints(ctx, environment).Authorize + "?" + q.Encode(), nil
}

// Exchange trades an authorization code for tokens.
func (o *OAuth) Exchange(ctx context.Context, environment, code, redirectURI string) (*Token, error) {
	return o.token(ctx, environment, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirectURI}})
}

// Refresh trades a refresh token for a new access token (and possibly a
// rotated refresh token). Callers must persist Token.RefreshToken.
func (o *OAuth) Refresh(ctx context.Context, environment, refreshToken string) (*Token, error) {
	return o.token(ctx, environment, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refreshToken}})
}

func (o *OAuth) token(ctx context.Context, environment string, form url.Values) (*Token, error) {
	id, secret, ok := o.Creds(environment)
	if !ok {
		return nil, fmt.Errorf("no Intuit credentials configured for %s", environment)
	}
	endpoint := o.TokenURL
	if endpoint == "" {
		endpoint = o.Endpoints(ctx, environment).Token
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(id, secret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	now := time.Now()
	resp, err := o.client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	tid := tidSuffix(resp.Header.Get(IntuitTIDHeader))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Desc  string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &e)
		if resp.StatusCode == http.StatusBadRequest && e.Error == "invalid_grant" {
			return nil, fmt.Errorf("%w: %s%s", ErrInvalidGrant, e.Desc, tid)
		}
		return nil, fmt.Errorf("token endpoint returned HTTP %d%s: %s %s", resp.StatusCode, tid, e.Error, e.Desc)
	}
	var t struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		RefreshIn    int64  `json:"x_refresh_token_expires_in"`
	}
	if err := json.Unmarshal(body, &t); err != nil || t.AccessToken == "" {
		return nil, fmt.Errorf("token endpoint returned an unusable response%s", tid)
	}
	tok := &Token{AccessToken: t.AccessToken, RefreshToken: t.RefreshToken, ExpiresAt: now.Add(time.Duration(t.ExpiresIn) * time.Second)}
	if t.RefreshIn > 0 {
		tok.RefreshExpiresAt = now.Add(time.Duration(t.RefreshIn) * time.Second)
	}
	return tok, nil
}

// Revoke invalidates a refresh token at Intuit (used on disconnect).
func (o *OAuth) Revoke(ctx context.Context, environment, token string) error {
	id, secret, ok := o.Creds(environment)
	if !ok {
		return fmt.Errorf("no Intuit credentials configured for %s", environment)
	}
	endpoint := o.RevokeURL
	if endpoint == "" {
		endpoint = o.Endpoints(ctx, environment).Revoke
	}
	payload, _ := json.Marshal(map[string]string{"token": token})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.SetBasicAuth(id, secret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := o.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("revoke returned HTTP %d%s", resp.StatusCode, tidSuffix(resp.Header.Get(IntuitTIDHeader)))
	}
	return nil
}
