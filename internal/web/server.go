// Package web is the IAP-protected admin UI: connect, pause and disconnect
// QuickBooks companies. It never runs syncs.
package web

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/db"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
)

//go:embed templates/*.html
var templateFS embed.FS

var tmpl = template.Must(template.New("").Funcs(template.FuncMap{
	"ago": func(t *time.Time) string {
		if t == nil {
			return "never"
		}
		return t.UTC().Format("2006-01-02 15:04 UTC")
	},
	"daysUntil": func(t time.Time) int { return int(time.Until(t).Hours() / 24) },
}).ParseFS(templateFS, "templates/*.html"))

// Store is the persistence the UI needs (implemented by *db.Store).
type Store interface {
	List(ctx context.Context) ([]db.Installation, error)
	Upsert(ctx context.Context, realmID, companyName, environment, email string, tok *qbo.Token) error
	TogglePause(ctx context.Context, realmID string) error
	Delete(ctx context.Context, realmID string) (refreshToken, environment string, err error)
	CreateOAuthState(ctx context.Context, token, email, environment string, ttl time.Duration) error
	ConsumeOAuthState(ctx context.Context, token string) (email, environment string, err error)
}

// OAuth is the slice of *qbo.OAuth the UI needs.
type OAuth interface {
	AuthCodeURL(ctx context.Context, environment, redirectURI, state string) (string, error)
	Exchange(ctx context.Context, environment, code, redirectURI string) (*qbo.Token, error)
	Revoke(ctx context.Context, environment, token string) error
}

// Server wires the admin routes.
type Server struct {
	Store        Store
	OAuth        OAuth
	BasePath     string   // normalised WEB_PATH_PREFIX: "" or "/qbo"
	RedirectURI  string   // QBO_REDIRECT_URI registered with Intuit
	Environments []string // environments with credentials configured
	// CompanyName looks up the company's display name right after connecting.
	CompanyName func(ctx context.Context, environment, realmID, accessToken string) (string, error)
	Log         *slog.Logger
}

const stateTTL = 10 * time.Minute

// Handler returns the router. All routes live under BasePath; /healthz is
// additionally served at the root for Cloud Run probes.
func (s *Server) Handler() http.Handler {
	p := s.BasePath
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) }
	mux.HandleFunc("GET /healthz", health)
	if p != "" {
		mux.HandleFunc("GET "+p+"/healthz", health)
		mux.HandleFunc("GET "+p, s.index) // "/qbo" as well as "/qbo/"
	}
	mux.HandleFunc("GET "+p+"/{$}", s.index)
	mux.HandleFunc("GET "+p+"/connect", s.connect)
	mux.HandleFunc("GET "+p+"/callback", s.callback)
	mux.HandleFunc("POST "+p+"/installations/{realm_id}/toggle", s.toggle)
	mux.HandleFunc("POST "+p+"/installations/{realm_id}/delete", s.delete)
	return securityHeaders(sameOriginPosts(mux))
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// sameOriginPosts rejects cross-site form posts (CSRF). IAP authenticates the
// browser with a cookie, so state-changing requests must come from our pages.
func sameOriginPosts(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
				http.Error(w, "cross-site request refused", http.StatusForbidden)
				return
			}
			if origin := r.Header.Get("Origin"); origin != "" {
				if u, err := url.Parse(origin); err != nil || u.Host != r.Host {
					http.Error(w, "cross-origin request refused", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// adminEmail reads the identity IAP verified ("accounts.google.com:a@b.c").
func adminEmail(r *http.Request) string {
	v := r.Header.Get("X-Goog-Authenticated-User-Email")
	if v == "" {
		return "local-dev"
	}
	if i := strings.LastIndex(v, ":"); i >= 0 {
		v = v[i+1:]
	}
	return v
}

func (s *Server) home(w http.ResponseWriter, r *http.Request, flash string) {
	target := s.BasePath + "/"
	if flash != "" {
		target += "?" + url.Values{"msg": {flash}}.Encode()
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, public string, err error) {
	s.Log.Error(public, "error", err, "path", r.URL.Path, "admin", adminEmail(r))
	http.Error(w, public, status)
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.List(r.Context())
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not list installations", err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err = tmpl.ExecuteTemplate(w, "index.html", map[string]any{
		"BasePath":      s.BasePath,
		"Installations": list,
		"Environments":  s.Environments,
		"Admin":         adminEmail(r),
		"Flash":         r.URL.Query().Get("msg"),
	})
	if err != nil {
		s.Log.Error("render index", "error", err)
	}
}

func (s *Server) connect(w http.ResponseWriter, r *http.Request) {
	env := r.URL.Query().Get("env")
	if env == "" {
		env = "production"
	}
	if env != "production" && env != "sandbox" {
		http.Error(w, "env must be production or sandbox", http.StatusBadRequest)
		return
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not generate state", err)
		return
	}
	state := base64.RawURLEncoding.EncodeToString(raw)
	target, err := s.OAuth.AuthCodeURL(r.Context(), env, s.RedirectURI, state)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.Store.CreateOAuthState(r.Context(), state, adminEmail(r), env, stateTTL); err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not store OAuth state", err)
		return
	}
	http.Redirect(w, r, target, http.StatusFound)
}

func (s *Server) callback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	state := q.Get("state")
	if state == "" {
		http.Error(w, "missing state", http.StatusBadRequest)
		return
	}
	// Consume the state first so it can never be replayed, whatever happens next.
	email, env, err := s.Store.ConsumeOAuthState(r.Context(), state)
	if errors.Is(err, db.ErrNotFound) {
		http.Error(w, "unknown or expired OAuth state; start again from the dashboard", http.StatusBadRequest)
		return
	}
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not verify OAuth state", err)
		return
	}
	if email != adminEmail(r) {
		http.Error(w, "this OAuth flow was started by a different user", http.StatusForbidden)
		return
	}
	if e := q.Get("error"); e != "" {
		s.home(w, r, "QuickBooks authorisation was not completed: "+e)
		return
	}
	code, realmID := q.Get("code"), q.Get("realmId")
	if code == "" || realmID == "" {
		http.Error(w, "missing code or realmId", http.StatusBadRequest)
		return
	}
	tok, err := s.OAuth.Exchange(r.Context(), env, code, s.RedirectURI)
	if err != nil {
		s.fail(w, r, http.StatusBadGateway, "token exchange with Intuit failed", err)
		return
	}
	name := ""
	if s.CompanyName != nil {
		if name, err = s.CompanyName(r.Context(), env, realmID, tok.AccessToken); err != nil {
			s.Log.Warn("could not fetch company name", "realm_id", realmID, "error", err.Error())
		}
	}
	if err := s.Store.Upsert(r.Context(), realmID, name, env, email, tok); err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not save the installation", err)
		return
	}
	s.Log.Info("installation connected", "realm_id", realmID, "environment", env, "admin", email)
	s.home(w, r, "Connected "+firstNonEmpty(name, realmID))
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (s *Server) toggle(w http.ResponseWriter, r *http.Request) {
	realmID := r.PathValue("realm_id")
	if err := s.Store.TogglePause(r.Context(), realmID); errors.Is(err, db.ErrNotFound) {
		http.NotFound(w, r)
		return
	} else if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not update the installation", err)
		return
	}
	s.Log.Info("installation pause toggled", "realm_id", realmID, "admin", adminEmail(r))
	s.home(w, r, "")
}

func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	realmID := r.PathValue("realm_id")
	token, env, err := s.Store.Delete(r.Context(), realmID)
	if errors.Is(err, db.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "could not delete the installation", err)
		return
	}
	if token != "" {
		if err := s.OAuth.Revoke(r.Context(), env, token); err != nil {
			s.Log.Warn("could not revoke the refresh token at Intuit", "realm_id", realmID, "error", err.Error())
		}
	}
	s.Log.Info("installation disconnected", "realm_id", realmID, "admin", adminEmail(r))
	s.home(w, r, "Disconnected "+realmID+". Data already in BigQuery was kept.")
}
