package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
)

// Installation statuses.
const (
	StatusActive      = "active"
	StatusNeedsReauth = "needs_reauth"
	StatusPaused      = "paused"
)

// ErrNotFound is returned when a realm or OAuth state does not exist.
var ErrNotFound = errors.New("not found")

// Installation is one connected QBO company (without its token).
type Installation struct {
	RealmID               string
	CompanyName           string
	Environment           string
	Status                string
	ConnectedByEmail      string
	TokenRotatedAt        time.Time
	RefreshTokenExpiresAt *time.Time
	ReauthRequiredBy      time.Time
	LastSyncAt            *time.Time
	LastSyncStatus        string
	LastError             string
	CreatedAt             time.Time
}

const installationCols = `realm_id, COALESCE(company_name,''), environment, status, COALESCE(connected_by_email,''),
	token_rotated_at, refresh_token_expires_at, reauth_required_by, last_sync_at,
	COALESCE(last_sync_status,''), COALESCE(last_error,''), created_at`

func scanInstallation(row pgx.Row) (Installation, error) {
	var i Installation
	err := row.Scan(&i.RealmID, &i.CompanyName, &i.Environment, &i.Status, &i.ConnectedByEmail,
		&i.TokenRotatedAt, &i.RefreshTokenExpiresAt, &i.ReauthRequiredBy, &i.LastSyncAt,
		&i.LastSyncStatus, &i.LastError, &i.CreatedAt)
	return i, err
}

// List returns every installation ordered by company name.
func (s *Store) List(ctx context.Context) ([]Installation, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+installationCols+` FROM qbo_installations ORDER BY company_name, realm_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Installation
	for rows.Next() {
		i, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// Get returns one installation.
func (s *Store) Get(ctx context.Context, realmID string) (Installation, error) {
	i, err := scanInstallation(s.pool.QueryRow(ctx, `SELECT `+installationCols+` FROM qbo_installations WHERE realm_id = $1`, realmID))
	if errors.Is(err, pgx.ErrNoRows) {
		return i, ErrNotFound
	}
	return i, err
}

// Upsert stores a freshly authorised installation, replacing any previous
// token and resetting the status to active.
func (s *Store) Upsert(ctx context.Context, realmID, companyName, environment, email string, tok *qbo.Token) error {
	enc, err := s.cipher.Encrypt(tok.RefreshToken, realmID)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO qbo_installations
    (realm_id, company_name, environment, encrypted_refresh_token, status, connected_by_email,
     token_rotated_at, refresh_token_expires_at, reauth_required_by)
VALUES ($1, $2, $3, $4, 'active', $5, NOW(), $6, NOW() + INTERVAL '5 years')
ON CONFLICT (realm_id) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    environment = EXCLUDED.environment,
    encrypted_refresh_token = EXCLUDED.encrypted_refresh_token,
    status = 'active',
    connected_by_email = EXCLUDED.connected_by_email,
    token_rotated_at = NOW(),
    refresh_token_expires_at = EXCLUDED.refresh_token_expires_at,
    reauth_required_by = EXCLUDED.reauth_required_by,
    last_error = NULL,
    updated_at = NOW()`,
		realmID, companyName, environment, enc, email, nullTime(tok.RefreshExpiresAt))
	return err
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// TogglePause flips active <-> paused. Other statuses are left untouched.
func (s *Store) TogglePause(ctx context.Context, realmID string) error {
	tag, err := s.pool.Exec(ctx, `
UPDATE qbo_installations
SET status = CASE status WHEN 'active' THEN 'paused' WHEN 'paused' THEN 'active' ELSE status END, updated_at = NOW()
WHERE realm_id = $1`, realmID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

// Delete removes an installation and returns its refresh token so the caller
// can revoke it at Intuit (empty if it could not be decrypted).
func (s *Store) Delete(ctx context.Context, realmID string) (refreshToken, environment string, err error) {
	var enc string
	err = s.pool.QueryRow(ctx, `DELETE FROM qbo_installations WHERE realm_id = $1 RETURNING encrypted_refresh_token, environment`, realmID).Scan(&enc, &environment)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", ErrNotFound
	}
	if err != nil {
		return "", "", err
	}
	refreshToken, _ = s.cipher.Decrypt(enc, realmID)
	return refreshToken, environment, nil
}

// RecordSync stores the outcome of a sync run.
func (s *Store) RecordSync(ctx context.Context, realmID, status, lastError string) error {
	var le *string
	if lastError != "" {
		le = &lastError
	}
	_, err := s.pool.Exec(ctx, `
UPDATE qbo_installations SET last_sync_at = NOW(), last_sync_status = $2, last_error = $3, updated_at = NOW()
WHERE realm_id = $1`, realmID, status, le)
	return err
}

// CreateOAuthState records a CSRF state token and prunes expired ones.
func (s *Store) CreateOAuthState(ctx context.Context, token, email, environment string, ttl time.Duration) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM qbo_oauth_states WHERE expires_at < NOW()`); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO qbo_oauth_states (state_token, initiated_by_email, environment, expires_at)
VALUES ($1, $2, $3, NOW() + $4 * INTERVAL '1 second')`, token, email, environment, int(ttl.Seconds()))
	return err
}

// ConsumeOAuthState atomically deletes a state token and returns who created
// it. ErrNotFound means it is unknown, already used, or expired.
func (s *Store) ConsumeOAuthState(ctx context.Context, token string) (email, environment string, err error) {
	var expires time.Time
	err = s.pool.QueryRow(ctx, `DELETE FROM qbo_oauth_states WHERE state_token = $1 RETURNING initiated_by_email, environment, expires_at`, token).Scan(&email, &environment, &expires)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && time.Now().After(expires)) {
		return "", "", ErrNotFound
	}
	return email, environment, err
}

// RefreshFunc exchanges a refresh token at Intuit.
type RefreshFunc func(ctx context.Context, environment, refreshToken string) (*qbo.Token, error)

// ErrNeedsReauth means Intuit rejected the stored refresh token; an admin
// must reconnect the company from the web UI.
var ErrNeedsReauth = errors.New("installation needs re-authorisation")

// RefreshAccessToken is the token-rotation guardrail. Intuit rotates refresh
// tokens and invalidates the old one, so the exchange happens while holding a
// row lock (SELECT ... FOR UPDATE) and the rotated token is persisted before
// the transaction commits: concurrent callers queue up and each sees the
// newest token.
func (s *Store) RefreshAccessToken(ctx context.Context, realmID string, refresh RefreshFunc) (*qbo.Token, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	var enc, environment string
	err = tx.QueryRow(ctx, `SELECT encrypted_refresh_token, environment FROM qbo_installations WHERE realm_id = $1 FOR UPDATE`, realmID).Scan(&enc, &environment)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	current, err := s.cipher.Decrypt(enc, realmID)
	if err != nil {
		return nil, err
	}
	tok, err := refresh(ctx, environment, current)
	if errors.Is(err, qbo.ErrInvalidGrant) {
		if _, uerr := tx.Exec(ctx, `UPDATE qbo_installations SET status = 'needs_reauth', last_error = $2, updated_at = NOW() WHERE realm_id = $1`, realmID, err.Error()); uerr != nil {
			return nil, uerr
		}
		if cerr := tx.Commit(ctx); cerr != nil {
			return nil, cerr
		}
		return nil, fmt.Errorf("%w: %v", ErrNeedsReauth, err)
	}
	if err != nil {
		return nil, err
	}
	// From here on we hold a token Intuit may already have rotated: persist
	// it even if the caller's context is cancelled.
	pctx := context.WithoutCancel(ctx)
	if tok.RefreshToken != "" && tok.RefreshToken != current {
		newEnc, err := s.cipher.Encrypt(tok.RefreshToken, realmID)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(pctx, `UPDATE qbo_installations SET encrypted_refresh_token = $1, token_rotated_at = NOW(), refresh_token_expires_at = $3, updated_at = NOW() WHERE realm_id = $2`, newEnc, realmID, nullTime(tok.RefreshExpiresAt)); err != nil {
			return nil, fmt.Errorf("persist rotated refresh token: %w", err)
		}
	}
	if err := tx.Commit(pctx); err != nil {
		return nil, fmt.Errorf("commit rotated refresh token: %w", err)
	}
	return tok, nil
}

// SetCompanyName updates the display name.
func (s *Store) SetCompanyName(ctx context.Context, realmID, name string) error {
	_, err := s.pool.Exec(ctx, `UPDATE qbo_installations SET company_name = $2, updated_at = NOW() WHERE realm_id = $1`, realmID, name)
	return err
}
