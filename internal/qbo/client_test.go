package qbo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeTokens struct{ forced atomic.Int32 }

func (f *fakeTokens) AccessToken(_ context.Context, force bool) (string, error) {
	if force {
		f.forced.Add(1)
		return "fresh", nil
	}
	return "cached", nil
}

func testClient(t *testing.T, h http.HandlerFunc) (*Client, *[]time.Duration, *fakeTokens) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	tokens := &fakeTokens{}
	c := NewClient("sandbox", "123", "75", tokens)
	c.BaseURL = srv.URL
	var slept []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	return c, &slept, tokens
}

func TestGetRetries429WithBackoff(t *testing.T) {
	var calls atomic.Int32
	c, slept, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v3/company/123/query" || r.URL.Query().Get("minorversion") != "75" || r.URL.Query().Get("query") != "SELECT 1" {
			t.Errorf("unexpected request %s", r.URL)
		}
		switch calls.Add(1) {
		case 1:
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusTooManyRequests)
		case 2:
			w.WriteHeader(http.StatusTooManyRequests)
		case 3:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte(`{"QueryResponse":{}}`))
		}
	})
	resp, err := c.Query(context.Background(), "SELECT 1")
	if err != nil || resp.Status != 200 || calls.Load() != 4 {
		t.Fatalf("resp=%+v err=%v calls=%d", resp, err, calls.Load())
	}
	s := *slept
	if len(s) != 3 {
		t.Fatalf("expected 3 backoff sleeps, got %v", s)
	}
	if s[0] != 7*time.Second { // Retry-After wins over the 0.5–1s first backoff
		t.Errorf("first sleep should honour Retry-After, got %v", s[0])
	}
	if s[1] < time.Second || s[1] > 2*time.Second || s[2] < 2*time.Second || s[2] > 4*time.Second {
		t.Errorf("backoff is not exponential with jitter: %v", s)
	}
}

func TestGetGivesUpAndReturnsLastResponse(t *testing.T) {
	var calls atomic.Int32
	c, _, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("slow down"))
	})
	c.MaxAttempts = 3
	resp, err := c.Get(context.Background(), "query", nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 429 || calls.Load() != 3 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
	if resp == nil || string(resp.Body) != "slow down" {
		t.Errorf("last response should be returned for archival, got %+v", resp)
	}
}

func TestGetDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	c, slept, _ := testClient(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"Fault":{"type":"ValidationFault"}}`))
	})
	_, err := c.Get(context.Background(), "query", nil)
	if err == nil || calls.Load() != 1 || len(*slept) != 0 || !strings.Contains(err.Error(), "ValidationFault") {
		t.Errorf("err=%v calls=%d slept=%v", err, calls.Load(), *slept)
	}
}

func TestGetRefreshesTokenOnceOn401(t *testing.T) {
	var auths []string
	c, _, tokens := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		auths = append(auths, r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") != "Bearer fresh" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if _, err := c.Get(context.Background(), "query", nil); err != nil {
		t.Fatal(err)
	}
	if strings.Join(auths, ",") != "Bearer cached,Bearer fresh" || tokens.forced.Load() != 1 {
		t.Errorf("auths=%v forced=%d", auths, tokens.forced.Load())
	}
}

func TestBaseURL(t *testing.T) {
	if BaseURL("sandbox") != "https://sandbox-quickbooks.api.intuit.com" || BaseURL("production") != "https://quickbooks.api.intuit.com" {
		t.Error("wrong API hosts")
	}
}

func TestOAuthRefresh(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		_ = r.ParseForm()
		if u != "id" || p != "secret" || r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("bad token request: %s %s %v", u, p, r.Form)
		}
		if r.Form.Get("refresh_token") == "dead" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Token invalid"}`))
			return
		}
		_, _ = w.Write([]byte(`{"access_token":"at","refresh_token":"rt2","expires_in":3600,"x_refresh_token_expires_in":8726400}`))
	}))
	defer srv.Close()
	o := &OAuth{TokenURL: srv.URL, Creds: func(env string) (string, string, bool) { return "id", "secret", env == "sandbox" }}
	tok, err := o.Refresh(context.Background(), "sandbox", "rt1")
	if err != nil || tok.AccessToken != "at" || tok.RefreshToken != "rt2" || time.Until(tok.ExpiresAt) < 59*time.Minute || time.Until(tok.RefreshExpiresAt) < 100*24*time.Hour {
		t.Fatalf("tok=%+v err=%v", tok, err)
	}
	if _, err := o.Refresh(context.Background(), "sandbox", "dead"); !errors.Is(err, ErrInvalidGrant) {
		t.Errorf("expected ErrInvalidGrant, got %v", err)
	}
	if _, err := o.Refresh(context.Background(), "production", "rt1"); err == nil {
		t.Error("missing credentials for an environment must be an error")
	}
	u, _ := o.AuthCodeURL("sandbox", "https://x.test/qbo/callback", "st8")
	for _, frag := range []string{"https://appcenter.intuit.com/connect/oauth2?", "client_id=id", "redirect_uri=https%3A%2F%2Fx.test%2Fqbo%2Fcallback", "response_type=code", "scope=com.intuit.quickbooks.accounting", "state=st8"} {
		if !strings.Contains(u, frag) {
			t.Errorf("auth URL %s lacks %s", u, frag)
		}
	}
}
