package web

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/db"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
)

type fakeStore struct {
	installs map[string]db.Installation
	states   map[string][2]string // token -> {email, env}
	tokens   map[string]string    // realm -> refresh token
}

func (f *fakeStore) List(context.Context) ([]db.Installation, error) {
	var out []db.Installation
	for _, i := range f.installs {
		out = append(out, i)
	}
	return out, nil
}

func (f *fakeStore) Upsert(_ context.Context, realmID, name, env, email string, tok *qbo.Token) error {
	f.installs[realmID] = db.Installation{RealmID: realmID, CompanyName: name, Environment: env, Status: db.StatusActive, ConnectedByEmail: email, ReauthRequiredBy: time.Now().AddDate(5, 0, 0)}
	f.tokens[realmID] = tok.RefreshToken
	return nil
}

func (f *fakeStore) TogglePause(_ context.Context, realmID string) error {
	i, ok := f.installs[realmID]
	if !ok {
		return db.ErrNotFound
	}
	if i.Status == db.StatusActive {
		i.Status = db.StatusPaused
	} else {
		i.Status = db.StatusActive
	}
	f.installs[realmID] = i
	return nil
}

func (f *fakeStore) Delete(_ context.Context, realmID string) (string, string, error) {
	i, ok := f.installs[realmID]
	if !ok {
		return "", "", db.ErrNotFound
	}
	delete(f.installs, realmID)
	return f.tokens[realmID], i.Environment, nil
}

func (f *fakeStore) CreateOAuthState(_ context.Context, token, email, env string, _ time.Duration) error {
	f.states[token] = [2]string{email, env}
	return nil
}

func (f *fakeStore) ConsumeOAuthState(_ context.Context, token string) (string, string, error) {
	s, ok := f.states[token]
	if !ok {
		return "", "", db.ErrNotFound
	}
	delete(f.states, token)
	return s[0], s[1], nil
}

type fakeOAuth struct{ revoked []string }

func (f *fakeOAuth) AuthCodeURL(env, redirect, state string) (string, error) {
	return qbo.AuthorizeURL + "?" + url.Values{"client_id": {"cid-" + env}, "redirect_uri": {redirect}, "response_type": {"code"}, "scope": {qbo.Scope}, "state": {state}}.Encode(), nil
}

func (f *fakeOAuth) Exchange(_ context.Context, env, code, redirect string) (*qbo.Token, error) {
	return &qbo.Token{AccessToken: "at-" + code, RefreshToken: "rt-" + code}, nil
}

func (f *fakeOAuth) Revoke(_ context.Context, _, token string) error {
	f.revoked = append(f.revoked, token)
	return nil
}

func newTestServer(prefix string) (*Server, *fakeStore, *fakeOAuth) {
	store := &fakeStore{installs: map[string]db.Installation{}, states: map[string][2]string{}, tokens: map[string]string{}}
	oauth := &fakeOAuth{}
	return &Server{
		Store: store, OAuth: oauth, BasePath: prefix, RedirectURI: "https://example.com" + prefix + "/callback",
		Environments: []string{"production", "sandbox"}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		CompanyName: func(_ context.Context, env, realmID, at string) (string, error) { return "Acme <Co>", nil },
	}, store, oauth
}

func do(h http.Handler, method, target string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:admin@example.com")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRoutingWithAndWithoutPrefix(t *testing.T) {
	for _, prefix := range []string{"", "/qbo"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			srv, store, oauth := newTestServer(prefix)
			h := srv.Handler()

			// Health checks: always at the root, and under the prefix too.
			for _, p := range []string{"/healthz", prefix + "/healthz"} {
				if rec := do(h, "GET", p, nil); rec.Code != 200 {
					t.Errorf("GET %s = %d", p, rec.Code)
				}
			}

			// Dashboard: links and forms carry the base path.
			store.installs["111"] = db.Installation{RealmID: "111", CompanyName: "Old Co", Environment: "production", Status: db.StatusActive, ReauthRequiredBy: time.Now().AddDate(5, 0, 0)}
			rec := do(h, "GET", prefix+"/", nil)
			body := rec.Body.String()
			if rec.Code != 200 {
				t.Fatalf("GET %s/ = %d", prefix, rec.Code)
			}
			for _, frag := range []string{
				`href="` + prefix + `/connect?env=production"`, `href="` + prefix + `/connect?env=sandbox"`,
				`action="` + prefix + `/installations/111/toggle"`, `action="` + prefix + `/installations/111/delete"`,
				"Old Co", "admin@example.com",
			} {
				if !strings.Contains(body, frag) {
					t.Errorf("dashboard lacks %q", frag)
				}
			}
			if prefix != "" {
				if rec := do(h, "GET", prefix, nil); rec.Code != 200 {
					t.Errorf("GET %s (no trailing slash) = %d", prefix, rec.Code)
				}
				// Nothing application-specific leaks outside the prefix.
				for _, p := range []string{"/", "/connect", "/callback"} {
					if rec := do(h, "GET", p, nil); rec.Code != 404 {
						t.Errorf("GET %s should be 404 with a prefix, got %d", p, rec.Code)
					}
				}
			}

			// Connect: stores a state and redirects to Intuit with our redirect URI.
			rec = do(h, "GET", prefix+"/connect?env=sandbox", nil)
			loc, _ := url.Parse(rec.Header().Get("Location"))
			state := loc.Query().Get("state")
			if rec.Code != http.StatusFound || loc.Host != "appcenter.intuit.com" || loc.Query().Get("client_id") != "cid-sandbox" ||
				loc.Query().Get("redirect_uri") != "https://example.com"+prefix+"/callback" || len(state) != 43 {
				t.Fatalf("connect redirect = %d %s", rec.Code, loc)
			}
			if store.states[state] != [2]string{"admin@example.com", "sandbox"} {
				t.Errorf("state not stored: %v", store.states)
			}
			if rec := do(h, "GET", prefix+"/connect?env=bogus", nil); rec.Code != 400 {
				t.Errorf("bad env = %d", rec.Code)
			}

			// Callback: rejects unknown state, a different user, then succeeds exactly once.
			if rec := do(h, "GET", prefix+"/callback?state=nope&code=c&realmId=222", nil); rec.Code != 400 {
				t.Errorf("unknown state = %d", rec.Code)
			}
			cb := prefix + "/callback?state=" + state + "&code=c1&realmId=222"
			rec = do(h, "GET", cb, nil)
			if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), prefix+"/?msg=") {
				t.Fatalf("callback = %d -> %q", rec.Code, rec.Header().Get("Location"))
			}
			inst := store.installs["222"]
			if inst.CompanyName != "Acme <Co>" || inst.Environment != "sandbox" || inst.ConnectedByEmail != "admin@example.com" || store.tokens["222"] != "rt-c1" {
				t.Errorf("installation = %+v token=%s", inst, store.tokens["222"])
			}
			if rec := do(h, "GET", cb, nil); rec.Code != 400 {
				t.Errorf("state replay = %d, want 400", rec.Code)
			}
			if body := do(h, "GET", prefix+"/", nil).Body.String(); !strings.Contains(body, "Acme &lt;Co&gt;") {
				t.Error("company names must be HTML-escaped")
			}

			// Pause / resume / disconnect all redirect back under the prefix.
			rec = do(h, "POST", prefix+"/installations/222/toggle", nil)
			if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != prefix+"/" || store.installs["222"].Status != db.StatusPaused {
				t.Errorf("toggle = %d -> %q (%s)", rec.Code, rec.Header().Get("Location"), store.installs["222"].Status)
			}
			if rec := do(h, "GET", prefix+"/installations/222/toggle", nil); rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("GET on a POST route = %d", rec.Code)
			}
			if rec := do(h, "POST", prefix+"/installations/999/toggle", nil); rec.Code != 404 {
				t.Errorf("unknown realm = %d", rec.Code)
			}
			rec = do(h, "POST", prefix+"/installations/222/delete", nil)
			if _, still := store.installs["222"]; still || rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), prefix+"/?msg=") {
				t.Errorf("delete = %d -> %q", rec.Code, rec.Header().Get("Location"))
			}
			if len(oauth.revoked) != 1 || oauth.revoked[0] != "rt-c1" {
				t.Errorf("refresh token should be revoked at Intuit, got %v", oauth.revoked)
			}
		})
	}
}

func TestCallbackRejectsOtherUser(t *testing.T) {
	srv, store, _ := newTestServer("")
	store.states["s1"] = [2]string{"someone-else@example.com", "production"}
	if rec := do(srv.Handler(), "GET", "/callback?state=s1&code=c&realmId=5", nil); rec.Code != http.StatusForbidden {
		t.Errorf("got %d", rec.Code)
	}
	if len(store.installs) != 0 {
		t.Error("nothing should be saved")
	}
}

func TestCrossSitePostsAreRefused(t *testing.T) {
	srv, store, _ := newTestServer("/qbo")
	store.installs["1"] = db.Installation{RealmID: "1", Status: db.StatusActive}
	h := srv.Handler()
	for _, hdr := range []map[string]string{{"Sec-Fetch-Site": "cross-site"}, {"Origin": "https://evil.example"}} {
		if rec := do(h, "POST", "/qbo/installations/1/delete", hdr); rec.Code != http.StatusForbidden {
			t.Errorf("%v: got %d", hdr, rec.Code)
		}
	}
	if _, ok := store.installs["1"]; !ok {
		t.Fatal("cross-site request deleted an installation")
	}
	if rec := do(h, "POST", "/qbo/installations/1/delete", map[string]string{"Sec-Fetch-Site": "same-origin", "Origin": "http://example.com"}); rec.Code != http.StatusSeeOther {
		t.Errorf("same-origin post = %d", rec.Code)
	}
}

func TestAdminEmail(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	if adminEmail(r) != "local-dev" {
		t.Error("fallback should be local-dev")
	}
	r.Header.Set("X-Goog-Authenticated-User-Email", "accounts.google.com:a@b.co")
	if adminEmail(r) != "a@b.co" {
		t.Errorf("got %s", adminEmail(r))
	}
}
