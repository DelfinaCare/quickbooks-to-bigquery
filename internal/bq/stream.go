package bq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/syncer"
)

// StreamLoader implements syncer.Warehouse with the BigQuery Storage Write
// API's change-data-capture mode instead of staging tables + MERGE.
//
// Entity, _sync_state and _report_snapshots tables get a NOT ENFORCED primary
// key and a max_staleness; rows are written to the table's default stream as
// UPSERTs with a _CHANGE_SEQUENCE_NUMBER derived from (updated_at,
// _extracted_at), and BigQuery applies them in the background.
//
// Consequences:
//   - Staged rows are held in memory until Merge (the default stream commits
//     every append immediately, so there is nothing to discard on the server).
//   - Mutating DML (MERGE/UPDATE/DELETE) on these tables is rejected while
//     unapplied changes exist.
//   - Tombstones keep the last known field values, so deletions and
//     reconciling refreshes first read the affected rows back with a query.
type StreamLoader struct {
	*Loader
	Writer       *managedwriter.Client
	MaxStaleness time.Duration

	smu     sync.Mutex
	pending map[string][][]byte // staging name -> NDJSON chunks
	streams map[string]*cdcStream
}

type cdcStream struct {
	ms    *managedwriter.ManagedStream
	codec *RowCodec
}

var _ syncer.Warehouse = (*StreamLoader)(nil)

// appendBytes caps one AppendRows request well under the 10 MB API limit.
const appendBytes = 8 << 20

// NewStreamLoader wraps a Loader with a Storage Write API client.
func NewStreamLoader(ctx context.Context, l *Loader, maxStaleness time.Duration, opts ...option.ClientOption) (*StreamLoader, error) {
	w, err := managedwriter.NewClient(ctx, l.Project, opts...)
	if err != nil {
		return nil, err
	}
	return &StreamLoader{Loader: l, Writer: w, MaxStaleness: maxStaleness,
		pending: map[string][][]byte{}, streams: map[string]*cdcStream{}}, nil
}

// Close flushes and closes every open stream.
func (s *StreamLoader) Close() error {
	s.smu.Lock()
	defer s.smu.Unlock()
	var errs []error
	for _, st := range s.streams {
		errs = append(errs, st.ms.Close())
	}
	s.streams = map[string]*cdcStream{}
	return errors.Join(append(errs, s.Writer.Close())...)
}

func staleness(d time.Duration) *bigquery.IntervalValue {
	if d <= 0 {
		return nil
	}
	return &bigquery.IntervalValue{Hours: int32(d / time.Hour), Minutes: int32(d % time.Hour / time.Minute), Seconds: int32(d % time.Minute / time.Second)}
}

// ensureCDCTable ensures the table exists with the given primary key and
// max_staleness (0 leaves it unset: queries then always merge pending
// changes, which is what a small table read by the sync itself wants), and
// opens (once per run) its default stream with
// a descriptor built from the live schema.
func (s *StreamLoader) ensureCDCTable(ctx context.Context, name string, desired bigquery.Schema, pk []string, stale time.Duration, md *bigquery.TableMetadata) (*cdcStream, error) {
	s.smu.Lock()
	st, ok := s.streams[name]
	s.smu.Unlock()
	if ok {
		return st, nil
	}
	if md == nil {
		md = &bigquery.TableMetadata{}
	}
	md.TableConstraints = &bigquery.TableConstraints{PrimaryKey: &bigquery.PrimaryKey{Columns: pk}}
	md.MaxStaleness = staleness(stale)
	live, err := s.ensureTable(ctx, name, desired, md)
	if err != nil {
		return nil, err
	}
	// Existing tables (created by the MERGE loader) need the key and option.
	t := s.Client.Dataset(s.Dataset).Table(name)
	cur, err := t.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", name, err)
	}
	var upd bigquery.TableMetadataToUpdate
	changed := false
	if cur.TableConstraints == nil || cur.TableConstraints.PrimaryKey == nil {
		upd.TableConstraints = md.TableConstraints
		changed = true
	} else if !slices.Equal(cur.TableConstraints.PrimaryKey.Columns, pk) {
		return nil, fmt.Errorf("table %s has primary key %v, want %v", name, cur.TableConstraints.PrimaryKey.Columns, pk)
	}
	if want := staleness(stale); want != nil && (cur.MaxStaleness == nil || cur.MaxStaleness.String() != want.String()) {
		upd.MaxStaleness = want
		changed = true
	}
	if changed {
		if _, err := t.Update(ctx, upd, cur.ETag); err != nil {
			return nil, fmt.Errorf("enable CDC on %s: %w", name, err)
		}
		s.Log.Info("enabled Storage Write API CDC on table", "table", name, "primary_key", pk, "max_staleness", stale.String())
	}
	codec, err := NewRowCodec(live, true)
	if err != nil {
		return nil, fmt.Errorf("table %s: %w", name, err)
	}
	ms, err := s.Writer.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(s.Project, s.Dataset, name)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(codec.Descriptor),
		// The backend closes idle connections, so the first append after a
		// quiet spell fails with io.EOF unless retried (seen in the
		// benchmark). At-least-once is safe: a replayed CDC row carries the
		// same _CHANGE_SEQUENCE_NUMBER and resolves to the same final row.
		managedwriter.EnableWriteRetries(true))
	if err != nil {
		return nil, fmt.Errorf("open write stream for %s: %w", name, err)
	}
	st = &cdcStream{ms: ms, codec: codec}
	s.smu.Lock()
	if prev, ok := s.streams[name]; ok {
		s.smu.Unlock()
		_ = ms.Close()
		return prev, nil
	}
	s.streams[name] = st
	s.smu.Unlock()
	return st, nil
}

type change struct {
	row        map[string]any
	changeType string
	seq        string
}

// write appends rows to the table's default stream and waits until every
// request is acknowledged (at which point the rows are durable).
func (s *StreamLoader) write(ctx context.Context, table string, st *cdcStream, changes []change) error {
	var results []*managedwriter.AppendResult
	var batch [][]byte
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		r, err := st.ms.AppendRows(ctx, batch)
		if err != nil {
			return err
		}
		results = append(results, r)
		batch, size = nil, 0
		return nil
	}
	for _, c := range changes {
		b, err := st.codec.Encode(c.row, c.changeType, c.seq)
		if err != nil {
			return fmt.Errorf("%s: encode row %v: %w", table, c.row["id"], err)
		}
		if size+len(b) > appendBytes {
			if err := flush(); err != nil {
				return fmt.Errorf("append to %s: %w", table, err)
			}
		}
		batch = append(batch, b)
		size += len(b)
	}
	if err := flush(); err != nil {
		return fmt.Errorf("append to %s: %w", table, err)
	}
	for _, r := range results {
		if _, err := r.GetResult(ctx); err != nil {
			full, _ := r.FullResponse(ctx)
			if full != nil && len(full.GetRowErrors()) > 0 {
				return fmt.Errorf("append to %s: %w (%d row errors, first: %v)", table, err, len(full.GetRowErrors()), full.GetRowErrors()[0])
			}
			return fmt.Errorf("append to %s: %w", table, err)
		}
	}
	return nil
}

func (s *StreamLoader) ensureEntity(ctx context.Context, e *schema.Entity) (*cdcStream, error) {
	return s.ensureCDCTable(ctx, e.Table, TableSchema(e), []string{"realm_id", "id"}, s.MaxStaleness, &bigquery.TableMetadata{
		Description: "QuickBooks Online " + e.Name + " (one row per realm_id + id; see is_deleted)",
		Clustering:  &bigquery.Clustering{Fields: []string{"realm_id", "id"}},
	})
}

// Stage implements syncer.Warehouse. Rows stay in memory until Merge.
func (s *StreamLoader) Stage(ctx context.Context, b syncer.Batch, ndjson []byte) error {
	if _, err := s.ensureEntity(ctx, b.Entity); err != nil {
		return err
	}
	key := StagingTable(b.Entity.Table, b.RunID, b.RealmID)
	s.smu.Lock()
	s.pending[key] = append(s.pending[key], bytes.Clone(ndjson))
	s.smu.Unlock()
	return nil
}

// Discard implements syncer.Warehouse.
func (s *StreamLoader) Discard(_ context.Context, b syncer.Batch) error {
	s.smu.Lock()
	delete(s.pending, StagingTable(b.Entity.Table, b.RunID, b.RealmID))
	s.smu.Unlock()
	return nil
}

func decodeNDJSON(chunks [][]byte) ([]map[string]any, error) {
	var rows []map[string]any
	for _, c := range chunks {
		dec := json.NewDecoder(bytes.NewReader(c))
		dec.UseNumber()
		for {
			var r map[string]any
			if err := dec.Decode(&r); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			rows = append(rows, r)
		}
	}
	return rows, nil
}

func newer(a, b map[string]any) bool {
	ua, ub := rowTime(a, "updated_at"), rowTime(b, "updated_at")
	if !ua.Equal(ub) {
		return ua.After(ub)
	}
	return !rowTime(a, "_extracted_at").Before(rowTime(b, "_extracted_at"))
}

// Merge implements syncer.Warehouse: it turns the staged rows into UPSERTs.
func (s *StreamLoader) Merge(ctx context.Context, b syncer.Batch, reconcile bool, now time.Time) error {
	st, err := s.ensureEntity(ctx, b.Entity)
	if err != nil {
		return err
	}
	key := StagingTable(b.Entity.Table, b.RunID, b.RealmID)
	s.smu.Lock()
	chunks := s.pending[key]
	delete(s.pending, key)
	s.smu.Unlock()
	rows, err := decodeNDJSON(chunks)
	if err != nil {
		return err
	}
	// De-duplicate per id, newest first (like the MERGE source's QUALIFY).
	latest := map[string]map[string]any{}
	for _, r := range rows {
		if r["realm_id"] != b.RealmID {
			return fmt.Errorf("batch for realm %s holds a row of realm %v", b.RealmID, r["realm_id"])
		}
		id, _ := r["id"].(string)
		if prev, ok := latest[id]; !ok || newer(r, prev) {
			latest[id] = r
		}
	}
	var deleted, seen []string
	for id, r := range latest {
		seen = append(seen, id)
		if del, _ := r["is_deleted"].(bool); del {
			deleted = append(deleted, id)
		}
	}
	// Read back the rows whose previous values we need: tombstones keep the
	// last known field values, and a reconciling refresh flags every live row
	// that is absent from the snapshot.
	var prior map[string]map[string]any
	if len(deleted) > 0 || reconcile {
		prior, err = s.lookup(ctx, b, seen, deleted, reconcile)
		if err != nil {
			return fmt.Errorf("read back %s: %w", b.Entity.Table, err)
		}
	}
	changes := planChanges(latest, prior, reconcile, now)
	if len(changes) == 0 {
		return nil
	}
	return s.write(ctx, b.Entity.Table, st, changes)
}

// planChanges turns de-duplicated staged rows (latest, by id) and the rows
// read back from the table (prior, by id) into UPSERTs that reproduce
// MergeSQL's semantics.
func planChanges(latest, prior map[string]map[string]any, reconcile bool, now time.Time) []change {
	changes := make([]change, 0, len(latest)+len(prior))
	for id, r := range latest {
		upd, ext := rowTime(r, "updated_at"), rowTime(r, "_extracted_at")
		if p, ok := prior[id]; ok {
			if del, _ := r["is_deleted"].(bool); del {
				// Keep the last known values; only the envelope changes.
				merged := make(map[string]any, len(p))
				for k, v := range p {
					merged[k] = v
				}
				for _, k := range []string{"is_deleted", "updated_at", "_extracted_at", "_raw_gcs_uri"} {
					merged[k] = r[k]
				}
				r = merged
			} else if pd, _ := p["is_deleted"].(bool); pd && reconcile {
				// A snapshot always wins (MERGE used WHEN MATCHED AND TRUE):
				// order it after a synthetic reconcile tombstone.
				if pu := rowTime(p, "updated_at"); !upd.After(pu) {
					upd = pu.Add(time.Microsecond)
				}
			}
		}
		changes = append(changes, change{row: r, changeType: ChangeUpsert, seq: SequenceNumber(upd, ext)})
	}
	if reconcile {
		ts := schema.FormatTimestamp(now)
		for id, p := range prior {
			if _, inSnapshot := latest[id]; inSnapshot {
				continue
			}
			if pd, _ := p["is_deleted"].(bool); pd {
				continue
			}
			p["is_deleted"], p["updated_at"], p["_extracted_at"] = true, ts, ts
			changes = append(changes, change{row: p, changeType: ChangeUpsert, seq: SequenceNumber(now, now)})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].row["id"].(string) < changes[j].row["id"].(string) })
	return changes
}

// lookup returns current rows (as NDJSON-shaped maps) for ids in deleted,
// plus, with reconcile, every live row absent from seen and every deleted row
// present in seen.
func (s *StreamLoader) lookup(ctx context.Context, b syncer.Batch, seen, deleted []string, reconcile bool) (map[string]map[string]any, error) {
	cond := "id IN UNNEST(@deleted)"
	if reconcile {
		cond += " OR (NOT is_deleted AND id NOT IN UNNEST(@seen)) OR (is_deleted AND id IN UNNEST(@seen))"
	}
	q := s.Client.Query("SELECT TO_JSON_STRING(t) AS j FROM " + s.ref(b.Entity.Table) + " AS t WHERE realm_id = @realm_id AND (" + cond + ")")
	q.Parameters = []bigquery.QueryParameter{
		{Name: "realm_id", Value: b.RealmID},
		{Name: "deleted", Value: append([]string{}, deleted...)},
		{Name: "seen", Value: append([]string{}, seen...)},
	}
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]any{}
	for {
		var row struct {
			J string `bigquery:"j"`
		}
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(bytes.NewReader([]byte(row.J)))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
		id, _ := m["id"].(string)
		out[id] = m
	}
}

// SyncState implements syncer.Warehouse.
func (s *StreamLoader) SyncState(ctx context.Context, realmID string) (map[string]qbo.EntityState, error) {
	if _, err := s.ensureCDCTable(ctx, SyncStateTable, syncStateSchema, []string{"realm_id", "entity"}, 0, nil); err != nil {
		return nil, err
	}
	return s.Loader.SyncState(ctx, realmID)
}

// SaveState implements syncer.Warehouse.
func (s *StreamLoader) SaveState(ctx context.Context, states []qbo.EntityState) error {
	if len(states) == 0 {
		return nil
	}
	st, err := s.ensureCDCTable(ctx, SyncStateTable, syncStateSchema, []string{"realm_id", "entity"}, 0, nil)
	if err != nil {
		return err
	}
	now := time.Now()
	changes := make([]change, 0, len(states))
	for _, x := range states {
		row := map[string]any{"realm_id": x.RealmID, "entity": x.Entity, "last_run_id": x.LastRunID, "updated_at": now}
		if !x.HighWatermark.IsZero() {
			row["high_watermark"] = x.HighWatermark
		}
		if !x.LastFullSyncAt.IsZero() {
			row["last_full_sync_at"] = x.LastFullSyncAt
		}
		changes = append(changes, change{row: row, changeType: ChangeUpsert, seq: SequenceNumber(now, now)})
	}
	return s.write(ctx, SyncStateTable, st, changes)
}

// WriteReportSnapshots implements syncer.Warehouse.
func (s *StreamLoader) WriteReportSnapshots(ctx context.Context, snaps []syncer.ReportSnapshot) error {
	if len(snaps) == 0 {
		return nil
	}
	st, err := s.ensureCDCTable(ctx, ReportSnapshotsTable, reportSnapshotsSchema,
		[]string{"realm_id", "report_name", "accounting_method", "period_start", "period_end"}, s.MaxStaleness, nil)
	if err != nil {
		return err
	}
	changes := make([]change, 0, len(snaps))
	for _, x := range snaps {
		row := map[string]any{"realm_id": x.RealmID, "report_name": x.ReportName, "accounting_method": x.AccountingMethod,
			"period_start": x.PeriodStart, "period_end": x.PeriodEnd, "extracted_at": x.ExtractedAt, "raw_payload_string": x.RawPayload}
		if x.GCSURI != "" {
			row["gcs_uri"] = x.GCSURI
		}
		changes = append(changes, change{row: row, changeType: ChangeUpsert, seq: SequenceNumber(x.ExtractedAt, x.ExtractedAt)})
	}
	return s.write(ctx, ReportSnapshotsTable, st, changes)
}
