package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the PostgreSQL-backed installation store.
type Store struct {
	pool   *pgxpool.Pool
	cipher *Cipher
}

// Open connects to PostgreSQL over TCP.
func Open(ctx context.Context, databaseURL string, c *Cipher) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database config: %w", err)
	}
	cfg.MaxConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Store{pool: pool, cipher: c}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// migrations are idempotent and run in order on every start.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS qbo_installations (
    realm_id                 VARCHAR(64) PRIMARY KEY,
    company_name             TEXT,
    environment              VARCHAR(16) NOT NULL DEFAULT 'production', -- 'sandbox' | 'production'
    encrypted_refresh_token  TEXT NOT NULL,
    status                   VARCHAR(32) NOT NULL DEFAULT 'active',     -- 'active' | 'needs_reauth' | 'paused'
    connected_by_email       TEXT,
    token_rotated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    refresh_token_expires_at TIMESTAMPTZ,                               -- rolling expiry reported by Intuit
    reauth_required_by       TIMESTAMPTZ NOT NULL,                      -- connection time + 5 years (Intuit hard cap)
    last_sync_at             TIMESTAMPTZ,
    last_sync_status         VARCHAR(32),
    last_error               TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`,
	`CREATE TABLE IF NOT EXISTS qbo_oauth_states (
    state_token        VARCHAR(128) PRIMARY KEY,
    initiated_by_email TEXT NOT NULL,
    environment        VARCHAR(16) NOT NULL DEFAULT 'production',
    expires_at         TIMESTAMPTZ NOT NULL
)`,
}

// Migrate creates the schema. An advisory lock serialises concurrent starters.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7426001)`); err != nil {
		return err
	}
	for i, m := range migrations {
		if _, err := tx.Exec(ctx, m); err != nil {
			return fmt.Errorf("migration %d: %w", i, err)
		}
	}
	return tx.Commit(ctx)
}
