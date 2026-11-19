package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// LocalWarehouse writes what would be loaded into BigQuery to NDJSON files
// under Dir (`sync --local-out`). It appends instead of merging, so it is a
// debugging aid rather than a mirror:
//
//	tables/<table>.ndjson  _ingest_errors.ndjson  _report_snapshots.ndjson  _sync_state.json
type LocalWarehouse struct {
	Dir string
	mu  sync.Mutex
}

func (w *LocalWarehouse) appendFile(name string, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	p := filepath.Join(w.Dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func ndjson[T any](rows []T) ([]byte, error) {
	var out []byte
	for _, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			return nil, err
		}
		out = append(append(out, b...), '\n')
	}
	return out, nil
}

func (w *LocalWarehouse) statePath() string { return filepath.Join(w.Dir, "_sync_state.json") }

func (w *LocalWarehouse) readState() (map[string]map[string]qbo.EntityState, error) {
	all := map[string]map[string]qbo.EntityState{}
	b, err := os.ReadFile(w.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return all, nil
	}
	if err != nil {
		return nil, err
	}
	return all, json.Unmarshal(b, &all)
}

// SyncState implements Warehouse.
func (w *LocalWarehouse) SyncState(_ context.Context, realmID string) (map[string]qbo.EntityState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	all, err := w.readState()
	if err != nil {
		return nil, err
	}
	if all[realmID] == nil {
		return map[string]qbo.EntityState{}, nil
	}
	return all[realmID], nil
}

// Stage implements Warehouse.
func (w *LocalWarehouse) Stage(_ context.Context, b Batch, data []byte) error {
	return w.appendFile(filepath.Join("tables", b.Entity.Table+".ndjson"), data)
}

// Merge implements Warehouse (rows were already appended by Stage).
func (w *LocalWarehouse) Merge(context.Context, Batch, bool, time.Time) error { return nil }

// Discard implements Warehouse.
func (w *LocalWarehouse) Discard(context.Context, Batch) error { return nil }

// WriteIngestErrors implements Warehouse.
func (w *LocalWarehouse) WriteIngestErrors(_ context.Context, errs []schema.IngestError) error {
	b, err := ndjson(errs)
	if err != nil {
		return err
	}
	return w.appendFile("_ingest_errors.ndjson", b)
}

// WriteReportSnapshots implements Warehouse.
func (w *LocalWarehouse) WriteReportSnapshots(_ context.Context, snaps []ReportSnapshot) error {
	b, err := ndjson(snaps)
	if err != nil {
		return err
	}
	return w.appendFile("_report_snapshots.ndjson", b)
}

// SaveState implements Warehouse.
func (w *LocalWarehouse) SaveState(_ context.Context, states []qbo.EntityState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	all, err := w.readState()
	if err != nil {
		return err
	}
	for _, s := range states {
		if all[s.RealmID] == nil {
			all[s.RealmID] = map[string]qbo.EntityState{}
		}
		all[s.RealmID][s.Entity] = s
	}
	b, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(w.Dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(w.statePath(), b, 0o644)
}
