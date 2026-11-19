// Package syncer orchestrates one sync run: plan -> extract -> archive -> map -> load.
package syncer

import (
	"context"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// ReportSnapshot is one row of the _report_snapshots table.
type ReportSnapshot struct {
	RealmID          string    `json:"realm_id"`
	ReportName       string    `json:"report_name"`
	AccountingMethod string    `json:"accounting_method"`
	PeriodStart      string    `json:"period_start"` // YYYY-MM-DD
	PeriodEnd        string    `json:"period_end"`
	GCSURI           string    `json:"gcs_uri,omitempty"`
	ExtractedAt      time.Time `json:"extracted_at"`
	RawPayload       string    `json:"raw_payload_string"`
}

// Batch identifies the staging area for one entity of one realm in one run.
type Batch struct {
	Entity  *schema.Entity
	RunID   string
	RealmID string
}

// Warehouse is the destination. internal/bq implements it on BigQuery;
// LocalWarehouse writes NDJSON files for offline debugging.
type Warehouse interface {
	// SyncState returns the stored state per entity for a realm.
	SyncState(ctx context.Context, realmID string) (map[string]qbo.EntityState, error)
	// Stage appends mapped NDJSON rows to the batch's staging table.
	Stage(ctx context.Context, b Batch, ndjson []byte) error
	// Merge upserts the staged rows into the entity table and drops the
	// staging table. With reconcile=true the staged rows are a complete
	// snapshot: target rows of the realm that are absent get is_deleted=true.
	Merge(ctx context.Context, b Batch, reconcile bool, now time.Time) error
	// Discard drops the staging table without merging.
	Discard(ctx context.Context, b Batch) error
	WriteIngestErrors(ctx context.Context, errs []schema.IngestError) error
	WriteReportSnapshots(ctx context.Context, snaps []ReportSnapshot) error
	// SaveState upserts sync-state rows; called only after successful merges.
	SaveState(ctx context.Context, states []qbo.EntityState) error
}
