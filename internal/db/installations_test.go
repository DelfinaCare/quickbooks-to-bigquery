package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
)

// These tests need a real PostgreSQL:
//
//	TEST_DATABASE_URL=postgres://user:pass@localhost:5432/db?sslmode=disable go test ./internal/db
func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration tests")
	}
	c, _ := NewCipher(bytes.Repeat([]byte{1}, 32))
	ctx := context.Background()
	s, err := Open(ctx, url, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrations must be idempotent: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM qbo_installations WHERE realm_id LIKE 'test-%'`); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRefreshAccessTokenRotatesUnderLock(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const realm = "test-rotate"
	if err := s.Upsert(ctx, realm, "Co", "sandbox", "a@b.co", &qbo.Token{RefreshToken: "rt-0"}); err != nil {
		t.Fatal(err)
	}

	// Simulate Intuit: each refresh token works exactly once and yields the next.
	var mu sync.Mutex
	current, n := "rt-0", 0
	var inFlight, maxInFlight atomic.Int32
	refresh := func(_ context.Context, env, rt string) (*qbo.Token, error) {
		if c := inFlight.Add(1); c > maxInFlight.Load() {
			maxInFlight.Store(c)
		}
		defer inFlight.Add(-1)
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		if env != "sandbox" || rt != current {
			return nil, fmt.Errorf("%w: stale token %s", qbo.ErrInvalidGrant, rt)
		}
		n++
		current = fmt.Sprintf("rt-%d", n)
		return &qbo.Token{AccessToken: fmt.Sprintf("at-%d", n), RefreshToken: current, ExpiresAt: time.Now().Add(time.Hour), RefreshExpiresAt: time.Now().Add(100 * 24 * time.Hour)}, nil
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.RefreshAccessToken(ctx, realm, refresh)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent refresh failed (a rotated token was lost): %v", err)
		}
	}
	if maxInFlight.Load() != 1 {
		t.Errorf("refreshes overlapped (%d at once); the row lock is not serialising them", maxInFlight.Load())
	}
	var enc string
	_ = s.pool.QueryRow(ctx, `SELECT encrypted_refresh_token FROM qbo_installations WHERE realm_id = $1`, realm).Scan(&enc)
	if got, err := s.cipher.Decrypt(enc, realm); err != nil || got != "rt-8" {
		t.Errorf("stored refresh token = %q, %v; want rt-8", got, err)
	}
	inst, _ := s.Get(ctx, realm)
	if inst.Status != StatusActive || inst.RefreshTokenExpiresAt == nil {
		t.Errorf("installation = %+v", inst)
	}
}

func TestRefreshAccessTokenInvalidGrant(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const realm = "test-dead"
	_ = s.Upsert(ctx, realm, "Co", "production", "a@b.co", &qbo.Token{RefreshToken: "rt"})
	_, err := s.RefreshAccessToken(ctx, realm, func(context.Context, string, string) (*qbo.Token, error) {
		return nil, fmt.Errorf("%w: Token invalid", qbo.ErrInvalidGrant)
	})
	if !errors.Is(err, ErrNeedsReauth) {
		t.Fatalf("expected ErrNeedsReauth, got %v", err)
	}
	inst, _ := s.Get(ctx, realm)
	if inst.Status != StatusNeedsReauth || inst.LastError == "" {
		t.Errorf("installation = %+v", inst)
	}
	// A transient failure must NOT flag the installation or lose the token.
	_ = s.Upsert(ctx, realm, "Co", "production", "a@b.co", &qbo.Token{RefreshToken: "rt"})
	_, err = s.RefreshAccessToken(ctx, realm, func(context.Context, string, string) (*qbo.Token, error) {
		return nil, errors.New("intuit is down")
	})
	if inst, _ = s.Get(ctx, realm); err == nil || errors.Is(err, ErrNeedsReauth) || inst.Status != StatusActive {
		t.Errorf("transient failure: err=%v status=%s", err, inst.Status)
	}
	if _, err := s.RefreshAccessToken(ctx, "test-missing", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown realm: %v", err)
	}
}

func TestInstallationLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	const realm = "test-life"
	_ = s.Upsert(ctx, realm, "Co", "sandbox", "a@b.co", &qbo.Token{RefreshToken: "rt-life"})
	if err := s.TogglePause(ctx, realm); err != nil {
		t.Fatal(err)
	}
	if inst, _ := s.Get(ctx, realm); inst.Status != StatusPaused {
		t.Errorf("status = %s", inst.Status)
	}
	_ = s.TogglePause(ctx, realm)
	_ = s.RecordSync(ctx, realm, "error", "boom")
	inst, _ := s.Get(ctx, realm)
	if inst.Status != StatusActive || inst.LastSyncStatus != "error" || inst.LastError != "boom" || inst.LastSyncAt == nil ||
		time.Until(inst.ReauthRequiredBy) < 4*365*24*time.Hour {
		t.Errorf("installation = %+v", inst)
	}
	if list, err := s.List(ctx); err != nil || len(list) == 0 {
		t.Errorf("List = %v, %v", list, err)
	}
	rt, env, err := s.Delete(ctx, realm)
	if err != nil || rt != "rt-life" || env != "sandbox" {
		t.Errorf("Delete = %q %q %v", rt, env, err)
	}
	if err := s.TogglePause(ctx, realm); !errors.Is(err, ErrNotFound) {
		t.Errorf("toggle after delete: %v", err)
	}

	// OAuth states are single-use and expire.
	_ = s.CreateOAuthState(ctx, "test-state-1", "a@b.co", "sandbox", 10*time.Minute)
	email, env, err := s.ConsumeOAuthState(ctx, "test-state-1")
	if err != nil || email != "a@b.co" || env != "sandbox" {
		t.Errorf("consume = %q %q %v", email, env, err)
	}
	if _, _, err := s.ConsumeOAuthState(ctx, "test-state-1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second consume: %v", err)
	}
	_ = s.CreateOAuthState(ctx, "test-state-2", "a@b.co", "sandbox", -time.Minute)
	if _, _, err := s.ConsumeOAuthState(ctx, "test-state-2"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expired state: %v", err)
	}
}
