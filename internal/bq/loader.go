package bq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/syncer"
)

// Loader implements syncer.Warehouse on BigQuery.
type Loader struct {
	Client   *bigquery.Client
	Project  string
	Dataset  string
	Location string
	Log      *slog.Logger

	mu      sync.Mutex
	dataset bool                       // dataset verified
	tables  map[string]bigquery.Schema // live schema of tables ensured this run
}

var _ syncer.Warehouse = (*Loader)(nil)

// NewLoader connects to BigQuery.
func NewLoader(ctx context.Context, project, dataset, location string, log *slog.Logger) (*Loader, error) {
	var opts []option.ClientOption
	if host := os.Getenv("BIGQUERY_EMULATOR_HOST"); host != "" {
		// Local development against goccy/bigquery-emulator.
		if !strings.Contains(host, "://") {
			host = "http://" + host
		}
		opts = append(opts, option.WithEndpoint(host), option.WithoutAuthentication())
	}
	c, err := bigquery.NewClient(ctx, project, opts...)
	if err != nil {
		return nil, err
	}
	c.Location = location // empty: BigQuery infers the job location from the dataset
	return &Loader{Client: c, Project: project, Dataset: dataset, Location: location, Log: log, tables: map[string]bigquery.Schema{}}, nil
}

func hasStatus(err error, code int) bool {
	var ge *googleapi.Error
	return errors.As(err, &ge) && ge.Code == code
}

func (l *Loader) ref(table string) string { return tableRef(l.Project, l.Dataset, table) }

func (l *Loader) ensureDataset(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.dataset {
		return nil
	}
	ds := l.Client.Dataset(l.Dataset)
	if _, err := ds.Metadata(ctx); err != nil {
		if !hasStatus(err, http.StatusNotFound) {
			return fmt.Errorf("dataset %s: %w", l.Dataset, err)
		}
		err := ds.Create(ctx, &bigquery.DatasetMetadata{Location: l.Location, Description: "QuickBooks Online mirror (qbo-to-bigquery)"})
		if err != nil && !hasStatus(err, http.StatusConflict) {
			return fmt.Errorf("create dataset %s: %w", l.Dataset, err)
		}
	}
	l.dataset = true
	return nil
}

// ensureTable creates the table if missing, otherwise adds any new columns
// from desired. It returns the table's live schema, which is a superset of
// desired and is what staging tables and MERGE statements are built from so
// that STRUCT types always line up exactly.
func (l *Loader) ensureTable(ctx context.Context, name string, desired bigquery.Schema, md *bigquery.TableMetadata) (bigquery.Schema, error) {
	if err := l.ensureDataset(ctx); err != nil {
		return nil, err
	}
	l.mu.Lock()
	cached, ok := l.tables[name]
	l.mu.Unlock()
	if ok {
		return cached, nil
	}
	t := l.Client.Dataset(l.Dataset).Table(name)
	var live bigquery.Schema
	for attempt := 0; ; attempt++ {
		cur, err := t.Metadata(ctx)
		if hasStatus(err, http.StatusNotFound) {
			if md == nil {
				md = &bigquery.TableMetadata{}
			}
			md.Schema = desired
			if err := t.Create(ctx, md); err != nil {
				if hasStatus(err, http.StatusConflict) && attempt < 3 {
					continue // created concurrently; re-read
				}
				return nil, fmt.Errorf("create table %s: %w", name, err)
			}
			l.Log.Info("created BigQuery table", "table", name)
			live = desired
			break
		}
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		merged, added, err := EvolveSchema(cur.Schema, desired)
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", name, err)
		}
		if len(added) == 0 {
			live = cur.Schema
			break
		}
		if _, err := t.Update(ctx, bigquery.TableMetadataToUpdate{Schema: merged}, cur.ETag); err != nil {
			if hasStatus(err, http.StatusPreconditionFailed) && attempt < 3 {
				continue // someone else evolved it; re-read
			}
			return nil, fmt.Errorf("evolve table %s (adding %v): %w", name, added, err)
		}
		l.Log.Info("added columns to BigQuery table", "table", name, "columns", added)
		live = merged
		break
	}
	l.mu.Lock()
	l.tables[name] = live
	l.mu.Unlock()
	return live, nil
}

func (l *Loader) ensureEntityTable(ctx context.Context, e *schema.Entity) (bigquery.Schema, error) {
	return l.ensureTable(ctx, e.Table, TableSchema(e), &bigquery.TableMetadata{
		Description: "QuickBooks Online " + e.Name + " (one row per realm_id + id; see is_deleted)",
		Clustering:  &bigquery.Clustering{Fields: []string{"realm_id", "id"}},
	})
}

func (l *Loader) query(ctx context.Context, sql string, params ...bigquery.QueryParameter) error {
	q := l.Client.Query(sql)
	q.Parameters = params
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	st, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return st.Err()
}

// load appends NDJSON to a table, creating it with the given schema if needed.
func (l *Loader) load(ctx context.Context, table string, sch bigquery.Schema, data []byte, expires time.Time) error {
	t := l.Client.Dataset(l.Dataset).Table(table)
	if !expires.IsZero() {
		// Staging tables self-destruct in case a crashed run never drops them.
		err := t.Create(ctx, &bigquery.TableMetadata{Schema: sch, ExpirationTime: expires})
		if err != nil && !hasStatus(err, http.StatusConflict) {
			return fmt.Errorf("create %s: %w", table, err)
		}
	}
	src := bigquery.NewReaderSource(bytes.NewReader(data))
	src.SourceFormat = bigquery.JSON
	src.Schema = sch
	ld := t.LoaderFrom(src)
	ld.WriteDisposition = bigquery.WriteAppend
	ld.CreateDisposition = bigquery.CreateIfNeeded
	job, err := ld.Run(ctx)
	if err != nil {
		return fmt.Errorf("load into %s: %w", table, err)
	}
	st, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("load into %s: %w", table, err)
	}
	if err := st.Err(); err != nil {
		return fmt.Errorf("load into %s: %w (%d row errors, first: %v)", table, err, len(st.Errors), firstErr(st.Errors))
	}
	return nil
}

func firstErr(errs []*bigquery.Error) any {
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func (l *Loader) drop(ctx context.Context, table string) error {
	err := l.Client.Dataset(l.Dataset).Table(table).Delete(ctx)
	if err != nil && !hasStatus(err, http.StatusNotFound) {
		return err
	}
	return nil
}

const stagingTTL = 24 * time.Hour

// Stage implements syncer.Warehouse.
func (l *Loader) Stage(ctx context.Context, b syncer.Batch, ndjson []byte) error {
	live, err := l.ensureEntityTable(ctx, b.Entity)
	if err != nil {
		return err
	}
	return l.load(ctx, StagingTable(b.Entity.Table, b.RunID, b.RealmID), live, ndjson, time.Now().Add(stagingTTL))
}

// Merge implements syncer.Warehouse.
func (l *Loader) Merge(ctx context.Context, b syncer.Batch, reconcile bool, now time.Time) error {
	live, err := l.ensureEntityTable(ctx, b.Entity)
	if err != nil {
		return err
	}
	stg := StagingTable(b.Entity.Table, b.RunID, b.RealmID)
	// A reconciling merge of an empty snapshot still needs a staging table.
	err = l.Client.Dataset(l.Dataset).Table(stg).Create(ctx, &bigquery.TableMetadata{Schema: live, ExpirationTime: time.Now().Add(stagingTTL)})
	if err != nil && !hasStatus(err, http.StatusConflict) {
		return fmt.Errorf("create %s: %w", stg, err)
	}
	cols := make([]string, len(live))
	for i, f := range live {
		cols[i] = f.Name
	}
	sql, err := MergeSQL(l.ref(b.Entity.Table), l.ref(stg), cols, reconcile)
	if err != nil {
		return err
	}
	params := []bigquery.QueryParameter{{Name: "realm_id", Value: b.RealmID}}
	if reconcile {
		params = append(params, bigquery.QueryParameter{Name: "now", Value: now.UTC()})
	}
	if err := l.query(ctx, sql, params...); err != nil {
		return fmt.Errorf("merge into %s: %w", b.Entity.Table, err)
	}
	if err := l.drop(ctx, stg); err != nil {
		l.Log.Warn("could not drop staging table; it expires on its own", "table", stg, "error", err.Error())
	}
	return nil
}

// Discard implements syncer.Warehouse.
func (l *Loader) Discard(ctx context.Context, b syncer.Batch) error {
	return l.drop(ctx, StagingTable(b.Entity.Table, b.RunID, b.RealmID))
}

// SyncState implements syncer.Warehouse.
func (l *Loader) SyncState(ctx context.Context, realmID string) (map[string]qbo.EntityState, error) {
	if _, err := l.ensureTable(ctx, SyncStateTable, syncStateSchema, nil); err != nil {
		return nil, err
	}
	q := l.Client.Query("SELECT entity, high_watermark, last_full_sync_at, last_run_id FROM " + l.ref(SyncStateTable) + " WHERE realm_id = @realm_id")
	q.Parameters = []bigquery.QueryParameter{{Name: "realm_id", Value: realmID}}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]qbo.EntityState{}
	for {
		var row struct {
			Entity         string                 `bigquery:"entity"`
			HighWatermark  bigquery.NullTimestamp `bigquery:"high_watermark"`
			LastFullSyncAt bigquery.NullTimestamp `bigquery:"last_full_sync_at"`
			LastRunID      bigquery.NullString    `bigquery:"last_run_id"`
		}
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		st := qbo.EntityState{RealmID: realmID, Entity: row.Entity, LastRunID: row.LastRunID.StringVal}
		if row.HighWatermark.Valid {
			st.HighWatermark = row.HighWatermark.Timestamp
		}
		if row.LastFullSyncAt.Valid {
			st.LastFullSyncAt = row.LastFullSyncAt.Timestamp
		}
		out[row.Entity] = st
	}
}

// SaveState implements syncer.Warehouse.
func (l *Loader) SaveState(ctx context.Context, states []qbo.EntityState) error {
	if len(states) == 0 {
		return nil
	}
	if _, err := l.ensureTable(ctx, SyncStateTable, syncStateSchema, nil); err != nil {
		return err
	}
	return l.query(ctx, SaveStateSQL(l.ref(SyncStateTable), states))
}

func toNDJSON[T any](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// WriteIngestErrors implements syncer.Warehouse.
func (l *Loader) WriteIngestErrors(ctx context.Context, errs []schema.IngestError) error {
	if len(errs) == 0 {
		return nil
	}
	if _, err := l.ensureTable(ctx, IngestErrorsTable, ingestErrorsSchema, nil); err != nil {
		return err
	}
	data, err := toNDJSON(errs)
	if err != nil {
		return err
	}
	return l.load(ctx, IngestErrorsTable, ingestErrorsSchema, data, time.Time{})
}

// WriteReportSnapshots implements syncer.Warehouse.
func (l *Loader) WriteReportSnapshots(ctx context.Context, snaps []syncer.ReportSnapshot) error {
	if len(snaps) == 0 {
		return nil
	}
	if _, err := l.ensureTable(ctx, ReportSnapshotsTable, reportSnapshotsSchema, nil); err != nil {
		return err
	}
	data, err := toNDJSON(snaps)
	if err != nil {
		return err
	}
	stg := StagingTable("report_snapshots", snaps[0].RealmID, fmt.Sprint(time.Now().UnixNano()))
	if err := l.load(ctx, stg, reportSnapshotsSchema, data, time.Now().Add(stagingTTL)); err != nil {
		return err
	}
	if err := l.query(ctx, ReportMergeSQL(l.ref(ReportSnapshotsTable), l.ref(stg))); err != nil {
		return fmt.Errorf("merge into %s: %w", ReportSnapshotsTable, err)
	}
	if err := l.drop(ctx, stg); err != nil {
		l.Log.Warn("could not drop staging table; it expires on its own", "table", stg, "error", err.Error())
	}
	return nil
}
