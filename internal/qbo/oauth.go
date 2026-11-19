// Package qbo talks to Intuit: OAuth2, the rate-limited Accounting API client,
// raw response archival and the extraction strategy.
package qbo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Intuit OAuth2 endpoints.
const (
	AuthorizeURL = "https://appcenter.intuit.com/connect/oauth2"
	TokenURL     = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
	RevokeURL    = "https://developer.api.intuit.com/v2/oauth2/tokens/revoke"
	Scope        = "com.intuit.quickbooks.accounting"
)

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
type OAuth struct {
	Creds     func(environment string) (id, secret string, ok bool)
	HTTP      *http.Client
	TokenURL  string // overridable for tests
	RevokeURL string
}

func (o *OAuth) client() *http.Client {
	if o.HTTP != nil {
		return o.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// AuthCodeURL builds the Intuit consent-screen URL.
func (o *OAuth) AuthCodeURL(environment, redirectURI, state string) (string, error) {
	id, _, ok := o.Creds(environment)
	if !ok {
		return "", fmt.Errorf("no Intuit credentials configured for %s", environment)
	}
	q := url.Values{"client_id": {id}, "redirect_uri": {redirectURI}, "response_type": {"code"}, "scope": {Scope}, "state": {state}}
	return AuthorizeURL + "?" + q.Encode(), nil
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
		endpoint = TokenURL
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
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
			Desc  string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &e)
		if resp.StatusCode == http.StatusBadRequest && e.Error == "invalid_grant" {
			return nil, fmt.Errorf("%w: %s", ErrInvalidGrant, e.Desc)
		}
		return nil, fmt.Errorf("token endpoint returned HTTP %d: %s %s", resp.StatusCode, e.Error, e.Desc)
	}
	var t struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		RefreshIn    int64  `json:"x_refresh_token_expires_in"`
	}
	if err := json.Unmarshal(body, &t); err != nil || t.AccessToken == "" {
		return nil, fmt.Errorf("token endpoint returned an unusable response")
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
		endpoint = RevokeURL
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
		return fmt.Errorf("revoke returned HTTP %d", resp.StatusCode)
	}
	return nil
}
