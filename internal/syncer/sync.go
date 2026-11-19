package syncer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// API is the slice of *qbo.Client the syncer needs.
type API interface {
	Query(ctx context.Context, sql string) (*qbo.Response, error)
	CDC(ctx context.Context, entities []string, since time.Time) (*qbo.Response, error)
	Report(ctx context.Context, name, accountingMethod string, start, end time.Time) (*qbo.Response, error)
}

// Syncer holds everything shared across realms in one run.
type Syncer struct {
	RunID       string
	Registry    *schema.Registry
	Warehouse   Warehouse
	Archiver    qbo.Archiver
	SyncReports bool
	// FullEvery forces a reconciling full refresh this often (0 = never).
	FullEvery time.Duration
	ForceFull bool
	// Entities optionally restricts the run to some entity names.
	Entities []string
	Log      *slog.Logger
	Now      func() time.Time

	// FlushBytes is the staging chunk size (default 32 MiB).
	FlushBytes int
	// Concurrency is the number of entities extracted in parallel (default 4).
	Concurrency int

	seq     atomic.Int64
	unknown sync.Map // "Entity.path" -> struct{}; one warning per path per run
}

// Result summarises one realm.
type Result struct {
	Entities     map[string]int // rows staged per successfully synced entity
	IngestErrors int
	Failed       map[string]error // entity (or "_reports") -> error
}

// Err returns a combined error (unwrappable with errors.Is) if anything failed.
func (r *Result) Err() error {
	if len(r.Failed) == 0 {
		return nil
	}
	names := make([]string, 0, len(r.Failed))
	for n := range r.Failed {
		names = append(names, n)
	}
	sort.Strings(names)
	me := &multiError{}
	parts := make([]string, 0, len(names))
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s: %v", n, r.Failed[n]))
		me.errs = append(me.errs, r.Failed[n])
	}
	me.msg = strings.Join(parts, "; ")
	return me
}

type multiError struct {
	msg  string
	errs []error
}

func (m *multiError) Error() string   { return m.msg }
func (m *multiError) Unwrap() []error { return m.errs }

// NewRunID returns a sortable, path- and table-name-safe run identifier.
func NewRunID(now time.Time, random uint32) string {
	return fmt.Sprintf("%s_%06x", now.UTC().Format("20060102t150405z"), random&0xffffff)
}

func (s *Syncer) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *Syncer) entities() ([]*schema.Entity, error) {
	if len(s.Entities) == 0 {
		return s.Registry.Entities(), nil
	}
	var out []*schema.Entity
	for _, name := range s.Entities {
		e, ok := s.Registry.Entity(name)
		if !ok {
			return nil, fmt.Errorf("unknown entity %q", name)
		}
		out = append(out, e)
	}
	return out, nil
}

// warnUnknown emits the unknown-field telemetry line once per path per run.
func (s *Syncer) warnUnknown(entity, path, gcsURI string) {
	if _, seen := s.unknown.LoadOrStore(path, struct{}{}); seen {
		return
	}
	s.Log.Warn("QBO payload has a field missing from schemas/*.yaml",
		"event", "unknown_qbo_field", "entity", entity, "path", path, "gcs_uri", gcsURI, "run_id", s.RunID)
}

// archive persists a raw response before anything parses it.
func (s *Syncer) archive(ctx context.Context, realmID, entity, endpoint string, resp *qbo.Response, at time.Time) (string, error) {
	return s.Archiver.Archive(ctx, qbo.ArchiveMeta{
		RealmID: realmID, Entity: entity, RunID: s.RunID, Seq: int(s.seq.Add(1)),
		Endpoint: endpoint, RequestURL: resp.URL, HTTPStatus: resp.Status, FetchedAt: at,
	}, resp.Body)
}

// fetch runs an API call and archives whatever came back, success or not.
func (s *Syncer) fetch(ctx context.Context, realmID, entity, endpoint string, call func() (*qbo.Response, error)) (*qbo.Response, string, time.Time, error) {
	resp, err := call()
	at := s.now()
	if resp == nil {
		return nil, "", at, err
	}
	uri, aerr := s.archive(ctx, realmID, entity, endpoint, resp, at)
	if err != nil {
		return nil, uri, at, err
	}
	if aerr != nil {
		return nil, "", at, aerr
	}
	return resp, uri, at, nil
}

// stager maps records and ships them to the staging table in chunks.
type stager struct {
	s      *Syncer
	b      Batch
	buf    bytes.Buffer
	ids    map[string]struct{}
	staged bool
	dry    bool // map only; never write to the warehouse
	errs   []schema.IngestError
}

func (s *Syncer) newStager(realmID string, ent *schema.Entity) *stager {
	return &stager{s: s, b: Batch{Entity: ent, RunID: s.RunID, RealmID: realmID}, ids: map[string]struct{}{}}
}

func (st *stager) add(ctx context.Context, recs []json.RawMessage, uri string, at time.Time) error {
	mc := &schema.MapContext{RunID: st.s.RunID, RealmID: st.b.RealmID, GCSURI: uri, ExtractedAt: at, OnUnknown: st.s.warnUnknown}
	flushAt := st.s.FlushBytes
	if flushAt <= 0 {
		flushAt = 32 << 20
	}
	for _, rec := range recs {
		m, err := st.b.Entity.Map(rec, mc)
		if err != nil {
			st.errs = append(st.errs, schema.IngestError{
				RunID: mc.RunID, RealmID: mc.RealmID, Entity: st.b.Entity.Name, FieldPath: st.b.Entity.Name,
				ExpectedType: "RECORD", ErrorMessage: err.Error(), GCSURI: uri, RawRecordJSON: string(rec), FailedAt: at.UTC(),
			})
			continue
		}
		st.errs = append(st.errs, m.Errors...)
		if m.Row == nil {
			continue
		}
		line, err := json.Marshal(m.Row)
		if err != nil {
			return err
		}
		st.ids[m.ID] = struct{}{}
		st.buf.Write(line)
		st.buf.WriteByte('\n')
		if st.buf.Len() >= flushAt {
			if err := st.flush(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (st *stager) flush(ctx context.Context) error {
	if st.buf.Len() == 0 || st.dry {
		st.buf.Reset()
		return nil
	}
	if err := st.s.Warehouse.Stage(ctx, st.b, st.buf.Bytes()); err != nil {
		return err
	}
	st.staged = true
	st.buf.Reset()
	return nil
}

// finish merges what was staged and records field-level errors.
func (st *stager) finish(ctx context.Context, reconcile bool) error {
	if err := st.flush(ctx); err != nil {
		return err
	}
	if st.staged || reconcile {
		if err := st.s.Warehouse.Merge(ctx, st.b, reconcile, st.s.now()); err != nil {
			return err
		}
	}
	if len(st.errs) > 0 {
		for i, e := range st.errs {
			if i == 20 {
				st.s.Log.Error("further ingest errors suppressed from the log; see _ingest_errors", "entity", e.Entity, "total", len(st.errs))
				break
			}
			st.s.Log.Error("field failed type coercion", "event", "ingest_error", "entity", e.Entity, "record_id", e.RecordID,
				"field_path", e.FieldPath, "expected_type", e.ExpectedType, "observed_value", e.ObservedValue,
				"error", e.ErrorMessage, "gcs_uri", e.GCSURI, "run_id", e.RunID)
		}
		if err := st.s.Warehouse.WriteIngestErrors(ctx, st.errs); err != nil {
			return fmt.Errorf("write _ingest_errors: %w", err)
		}
	}
	return nil
}

const fullAttempts = 3

// fullRefresh pages through the whole entity and merges it as a snapshot.
// It reports whether the snapshot was verified consistent (and so reconciled
// deletes). Offset pagination can skip rows if records are deleted mid-scan,
// so multi-page scans are checked against COUNT(*) and retried on mismatch;
// if they never agree the rows are still upserted, just without flagging
// deletes, and the next run tries again.
func (s *Syncer) fullRefresh(ctx context.Context, api API, realmID string, ent *schema.Entity) (st *stager, reconciled bool, err error) {
	for attempt := 1; ; attempt++ {
		st = s.newStager(realmID, ent)
		pages := 0
		for pos := 1; ; pos += qbo.PageSize {
			resp, uri, at, err := s.fetch(ctx, realmID, ent.Name, "query", func() (*qbo.Response, error) {
				return api.Query(ctx, qbo.QuerySQL(ent, pos))
			})
			if err != nil {
				return st, false, err
			}
			recs, err := qbo.ParseRecords(resp.Body)
			if err != nil {
				return st, false, err
			}
			if err := st.add(ctx, recs[ent.Name], uri, at); err != nil {
				return st, false, err
			}
			pages++
			if len(recs[ent.Name]) < qbo.PageSize {
				break
			}
		}
		consistent := true
		if pages > 1 {
			resp, _, _, err := s.fetch(ctx, realmID, ent.Name, "count", func() (*qbo.Response, error) {
				return api.Query(ctx, qbo.CountSQL(ent))
			})
			if err != nil {
				return st, false, err
			}
			count, err := qbo.ParseCount(resp.Body)
			if err != nil {
				return st, false, err
			}
			consistent = count == len(st.ids)
			if !consistent {
				s.Log.Warn("full scan does not match COUNT(*); data changed while paging",
					"entity", ent.Name, "realm_id", realmID, "scanned", len(st.ids), "count", count, "attempt", attempt)
			}
		}
		if consistent || attempt == fullAttempts {
			return st, consistent, st.finish(ctx, consistent)
		}
		if st.staged {
			if err := s.Warehouse.Discard(ctx, st.b); err != nil {
				return st, false, err
			}
		}
	}
}

// SyncRealm mirrors one company. Entity failures are isolated: the others
// still load and only successful entities advance their watermark.
func (s *Syncer) SyncRealm(ctx context.Context, api API, realmID string) (*Result, error) {
	start := s.now()
	ents, err := s.entities()
	if err != nil {
		return nil, err
	}
	state, err := s.Warehouse.SyncState(ctx, realmID)
	if err != nil {
		return nil, fmt.Errorf("read _sync_state: %w", err)
	}
	plan := qbo.PlanSync(start, ents, state, s.ForceFull, s.FullEvery)
	res := &Result{Entities: map[string]int{}, Failed: map[string]error{}}
	var mu sync.Mutex
	var newStates []qbo.EntityState
	done := func(ent string, st *stager, reconciled bool, err error) {
		mu.Lock()
		defer mu.Unlock()
		if st != nil {
			res.IngestErrors += len(st.errs)
		}
		if err != nil {
			res.Failed[ent] = err
			s.Log.Error("entity sync failed", "realm_id", realmID, "entity", ent, "error", err.Error(), "run_id", s.RunID)
			return
		}
		res.Entities[ent] = len(st.ids)
		ns := qbo.EntityState{RealmID: realmID, Entity: ent, HighWatermark: start.Add(-qbo.WatermarkOverlap).UTC().Truncate(time.Second),
			LastFullSyncAt: state[ent].LastFullSyncAt, LastRunID: s.RunID}
		mode := "cdc"
		if reconciled {
			ns.LastFullSyncAt = start.UTC().Truncate(time.Second)
			mode = "full"
		}
		newStates = append(newStates, ns)
		s.Log.Info("entity synced", "realm_id", realmID, "entity", ent, "mode", mode, "records", len(st.ids), "ingest_errors", len(st.errs), "run_id", s.RunID)
	}

	// Tier 1: change data capture, one call per distinct watermark.
	full := append([]string{}, plan.Full...)
	for _, g := range plan.CDC {
		resp, uri, at, err := s.fetch(ctx, realmID, "_cdc", "cdc", func() (*qbo.Response, error) { return api.CDC(ctx, g.Entities, g.Since) })
		var recs map[string][]json.RawMessage
		if err == nil {
			recs, err = qbo.ParseRecords(resp.Body)
		}
		for _, name := range g.Entities {
			if err != nil {
				done(name, nil, false, fmt.Errorf("cdc: %w", err))
				continue
			}
			if len(recs[name]) >= qbo.CDCCap {
				s.Log.Warn("CDC response hit the per-entity cap; falling back to a full refresh", "realm_id", realmID, "entity", name)
				full = append(full, name)
				continue
			}
			ent, _ := s.Registry.Entity(name)
			st := s.newStager(realmID, ent)
			e := st.add(ctx, recs[name], uri, at)
			if e == nil {
				e = st.finish(ctx, false)
			}
			done(name, st, false, e)
		}
	}

	// Tier 2: full refreshes, a few entities at a time.
	conc := s.Concurrency
	if conc <= 0 {
		conc = 4
	}
	var g errgroup.Group
	g.SetLimit(conc)
	for _, name := range full {
		ent, _ := s.Registry.Entity(name)
		g.Go(func() error {
			st, reconciled, err := s.fullRefresh(ctx, api, realmID, ent)
			done(name, st, reconciled, err)
			return nil
		})
	}
	_ = g.Wait()

	if s.SyncReports && len(s.Entities) == 0 {
		if err := s.syncReports(ctx, api, realmID, start); err != nil {
			res.Failed["_reports"] = err
			s.Log.Error("report sync failed", "realm_id", realmID, "error", err.Error(), "run_id", s.RunID)
		}
	}

	if len(newStates) > 0 {
		sort.Slice(newStates, func(i, j int) bool { return newStates[i].Entity < newStates[j].Entity })
		if err := s.Warehouse.SaveState(ctx, newStates); err != nil {
			return res, fmt.Errorf("save _sync_state: %w", err)
		}
	}
	return res, res.Err()
}

// Reports are snapshotted for the previous calendar year through today.
var reportNames = []string{"ProfitAndLoss", "BalanceSheet"}
var accountingMethods = []string{"Accrual", "Cash"}

func (s *Syncer) syncReports(ctx context.Context, api API, realmID string, now time.Time) error {
	end := now.UTC()
	start := time.Date(end.Year()-1, time.January, 1, 0, 0, 0, 0, time.UTC)
	var snaps []ReportSnapshot
	var errs []error
	for _, name := range reportNames {
		for _, method := range accountingMethods {
			resp, uri, at, err := s.fetch(ctx, realmID, "_report", "report_"+name+"_"+method, func() (*qbo.Response, error) {
				return api.Report(ctx, name, method, start, end)
			})
			if err != nil {
				errs = append(errs, fmt.Errorf("%s/%s: %w", name, method, err))
				continue
			}
			snaps = append(snaps, ReportSnapshot{
				RealmID: realmID, ReportName: name, AccountingMethod: method,
				PeriodStart: start.Format("2006-01-02"), PeriodEnd: end.Format("2006-01-02"),
				GCSURI: uri, ExtractedAt: at.UTC(), RawPayload: string(resp.Body),
			})
		}
	}
	if len(snaps) > 0 {
		if err := s.Warehouse.WriteReportSnapshots(ctx, snaps); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ReplayResult summarises a replay.
type ReplayResult struct {
	Records      map[string]int
	IngestErrors []schema.IngestError
}

// Replay re-maps an archived raw response with the current schemas. With
// dryRun nothing is written; otherwise recovered rows are merged (without
// delete reconciliation and without touching _sync_state).
func (s *Syncer) Replay(ctx context.Context, uri, realmID string, dryRun bool) (*ReplayResult, error) {
	body, err := s.Archiver.Read(ctx, uri)
	if err != nil {
		return nil, err
	}
	recs, err := qbo.ParseRecords(body)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, n := range s.Entities {
		e, ok := s.Registry.Entity(n)
		if !ok {
			return nil, fmt.Errorf("unknown entity %q", n)
		}
		want[e.Name] = true
	}
	out := &ReplayResult{Records: map[string]int{}}
	names := make([]string, 0, len(recs))
	for n := range recs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		ent, ok := s.Registry.Entity(name)
		if !ok || (len(want) > 0 && !want[ent.Name]) {
			continue
		}
		st := s.newStager(realmID, ent)
		st.dry = dryRun
		if err := st.add(ctx, recs[name], uri, s.now()); err != nil {
			return out, err
		}
		out.Records[ent.Name] = len(st.ids)
		out.IngestErrors = append(out.IngestErrors, st.errs...)
		if dryRun {
			continue
		}
		if err := st.finish(ctx, false); err != nil {
			return out, err
		}
	}
	return out, nil
}
