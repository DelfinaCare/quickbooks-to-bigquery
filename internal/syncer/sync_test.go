package syncer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// fakeAPI serves canned responses and records every call.
type fakeAPI struct {
	mu      sync.Mutex
	calls   []string
	data    map[string][]string // entity -> record JSON objects
	cdc     string              // CDC response body
	count   map[string]int      // COUNT(*) override
	failFor string              // entity whose queries fail
}

func (f *fakeAPI) log(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeAPI) Query(_ context.Context, sql string) (*qbo.Response, error) {
	f.log(sql)
	parts := strings.Fields(sql)
	entity := parts[3]
	if entity == f.failFor {
		return &qbo.Response{URL: "u", Status: 400, Body: []byte(`{"Fault":{}}`)}, fmt.Errorf("boom")
	}
	if strings.Contains(sql, "COUNT(*)") {
		n, ok := f.count[entity]
		if !ok {
			n = len(f.data[entity])
		}
		return &qbo.Response{URL: "u", Status: 200, Body: []byte(fmt.Sprintf(`{"QueryResponse":{"totalCount":%d}}`, n))}, nil
	}
	var start int
	for i, p := range parts {
		if p == "STARTPOSITION" {
			fmt.Sscan(parts[i+1], &start)
		}
	}
	recs := f.data[entity]
	lo := min(start-1, len(recs))
	hi := min(lo+qbo.PageSize, len(recs))
	body := fmt.Sprintf(`{"QueryResponse":{"%s":[%s]}}`, entity, strings.Join(recs[lo:hi], ","))
	return &qbo.Response{URL: "u", Status: 200, Body: []byte(body)}, nil
}

func (f *fakeAPI) CDC(_ context.Context, entities []string, since time.Time) (*qbo.Response, error) {
	f.log("CDC " + strings.Join(entities, ",") + " since " + since.UTC().Format(time.RFC3339))
	return &qbo.Response{URL: "u", Status: 200, Body: []byte(f.cdc)}, nil
}

func (f *fakeAPI) Report(_ context.Context, name, method string, start, end time.Time) (*qbo.Response, error) {
	f.log("REPORT " + name + " " + method)
	return &qbo.Response{URL: "u", Status: 200, Body: []byte(`{"Header":{"ReportName":"` + name + `"}}`)}, nil
}

// spyWarehouse wraps LocalWarehouse and records merges.
type spyWarehouse struct {
	*LocalWarehouse
	mu       sync.Mutex
	merges   map[string]bool // entity -> reconcile flag of the last merge
	discards int
}

func (s *spyWarehouse) Merge(_ context.Context, b Batch, reconcile bool, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.merges[b.Entity.Name] = reconcile
	return nil
}

func (s *spyWarehouse) Discard(context.Context, Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discards++
	return nil
}

func newTestSyncer(t *testing.T, entities ...string) (*Syncer, *spyWarehouse, string) {
	t.Helper()
	dir := t.TempDir()
	wh := &spyWarehouse{LocalWarehouse: &LocalWarehouse{Dir: dir}, merges: map[string]bool{}}
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	return &Syncer{
		RunID: "run1", Registry: schema.MustDefault(), Warehouse: wh, Archiver: &qbo.LocalArchiver{Dir: filepath.Join(dir, "raw")},
		Entities: entities, Log: slog.New(slog.NewJSONHandler(io.Discard, nil)), Now: func() time.Time { return now },
	}, wh, dir
}

func readRows(t *testing.T, dir, file string) []map[string]any {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, file))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows []map[string]any
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<24)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, m)
	}
	return rows
}

func invoices(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(`{"Id":"%d","TotalAmt":%d.5,"MetaData":{"LastUpdatedTime":"2026-11-01T00:00:00Z"}}`, i+1, i)
	}
	return out
}

func TestInitialBackfillThenCDC(t *testing.T) {
	s, wh, dir := newTestSyncer(t, "Invoice", "TaxCode")
	api := &fakeAPI{data: map[string][]string{"Invoice": invoices(3), "TaxCode": {`{"Id":"TAX","Name":"TAX"}`}}}
	ctx := context.Background()

	// Run 1: no state -> paginated full backfill, reconciled.
	res, err := s.SyncRealm(ctx, api, "123")
	if err != nil || res.Entities["Invoice"] != 3 || res.Entities["TaxCode"] != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if !wh.merges["Invoice"] || !wh.merges["TaxCode"] {
		t.Errorf("backfill must merge as a reconciled snapshot: %v", wh.merges)
	}
	for _, c := range api.calls {
		if strings.HasPrefix(c, "CDC") {
			t.Errorf("no CDC expected on first run, got %q", c)
		}
	}
	rows := readRows(t, dir, "tables/invoice.ndjson")
	if len(rows) != 3 || rows[0]["total_amt"] != "0.5" || !strings.Contains(rows[0]["_raw_gcs_uri"].(string), "entity=Invoice") {
		t.Errorf("rows = %v", rows)
	}
	if _, err := os.Stat(rows[0]["_raw_gcs_uri"].(string)); err != nil {
		t.Errorf("raw payload must be archived before mapping: %v", err)
	}
	state, _ := wh.SyncState(ctx, "123")
	wantMark := time.Date(2026, 11, 12, 11, 50, 0, 0, time.UTC) // run start minus the 10-minute overlap
	if !state["Invoice"].HighWatermark.Equal(wantMark) || state["Invoice"].LastFullSyncAt.IsZero() {
		t.Errorf("state = %+v", state["Invoice"])
	}

	// Run 2: watermark exists -> one CDC call for Invoice; TaxCode (sync: full) is re-queried.
	api.calls = nil
	api.cdc = `{"CDCResponse":[{"QueryResponse":[{"Invoice":[
		{"Id":"2","TotalAmt":99,"MetaData":{"LastUpdatedTime":"2026-11-12T11:55:00Z"}},
		{"Id":"3","status":"Deleted","MetaData":{"LastUpdatedTime":"2026-11-12T11:56:00Z"}}]}]}]}`
	wh.merges = map[string]bool{}
	if _, err := s.SyncRealm(ctx, api, "123"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(api.calls, " | "); got != "CDC Invoice since 2026-11-12T11:50:00Z | SELECT * FROM TaxCode ORDERBY Id STARTPOSITION 1 MAXRESULTS 1000" {
		t.Errorf("calls = %s", got)
	}
	if reconcile, merged := wh.merges["Invoice"]; !merged || reconcile {
		t.Errorf("CDC must merge incrementally without reconciling deletes: %v", wh.merges)
	}
	rows = readRows(t, dir, "tables/invoice.ndjson")
	last := rows[len(rows)-1]
	if len(rows) != 5 || last["id"] != "3" || last["is_deleted"] != true || last["updated_at"] != "2026-11-12T11:56:00Z" {
		t.Errorf("soft delete row = %v", last)
	}
}

func TestCDCCapFallsBackToFullRefresh(t *testing.T) {
	s, wh, _ := newTestSyncer(t, "Invoice")
	ctx := context.Background()
	_ = wh.SaveState(ctx, []qbo.EntityState{{RealmID: "123", Entity: "Invoice", HighWatermark: s.Now().Add(-time.Hour), LastFullSyncAt: s.Now().Add(-time.Hour)}})
	all := invoices(1000)
	api := &fakeAPI{data: map[string][]string{"Invoice": invoices(2500)}, cdc: `{"CDCResponse":[{"QueryResponse":[{"Invoice":[` + strings.Join(all, ",") + `]}]}]}`}
	res, err := s.SyncRealm(ctx, api, "123")
	if err != nil || res.Entities["Invoice"] != 2500 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	want := []string{"CDC Invoice", "STARTPOSITION 1 ", "STARTPOSITION 1001 ", "STARTPOSITION 2001 ", "COUNT(*)"}
	if len(api.calls) != len(want) {
		t.Fatalf("calls = %v", api.calls)
	}
	for i, frag := range want {
		if !strings.Contains(api.calls[i], frag) {
			t.Errorf("call %d = %q, want it to contain %q", i, api.calls[i], frag)
		}
	}
	if !wh.merges["Invoice"] {
		t.Error("verified multi-page scan should reconcile deletes")
	}
}

func TestInconsistentScanNeverFlagsDeletes(t *testing.T) {
	s, wh, _ := newTestSyncer(t, "Invoice")
	// COUNT(*) keeps disagreeing with what paging returned (records moving mid-scan).
	api := &fakeAPI{data: map[string][]string{"Invoice": invoices(1500)}, count: map[string]int{"Invoice": 1501}}
	res, err := s.SyncRealm(context.Background(), api, "123")
	if err != nil || res.Entities["Invoice"] != 1500 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if reconcile, merged := wh.merges["Invoice"]; !merged || reconcile {
		t.Errorf("rows should be upserted without delete reconciliation, merges=%v", wh.merges)
	}
	state, _ := wh.SyncState(context.Background(), "123")
	if !state["Invoice"].LastFullSyncAt.IsZero() {
		t.Error("an unverified scan must not count as a full sync, so the next run retries")
	}
}

func TestEntityFailureIsIsolated(t *testing.T) {
	s, wh, _ := newTestSyncer(t, "Invoice", "Bill")
	api := &fakeAPI{data: map[string][]string{"Invoice": invoices(2)}, failFor: "Bill"}
	res, err := s.SyncRealm(context.Background(), api, "123")
	if err == nil || res.Failed["Bill"] == nil || res.Entities["Invoice"] != 2 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	state, _ := wh.SyncState(context.Background(), "123")
	if _, ok := state["Bill"]; ok {
		t.Error("a failed entity must not advance its watermark")
	}
	if _, ok := state["Invoice"]; !ok {
		t.Error("a successful entity must still be recorded")
	}
}

func TestIngestErrorsCarryArchiveURI(t *testing.T) {
	s, _, dir := newTestSyncer(t, "Invoice")
	api := &fakeAPI{data: map[string][]string{"Invoice": {`{"Id":"1","TotalAmt":"oops","DocNumber":"7"}`}}}
	res, err := s.SyncRealm(context.Background(), api, "123")
	if err != nil || res.IngestErrors != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	errs := readRows(t, dir, "_ingest_errors.ndjson")
	rows := readRows(t, dir, "tables/invoice.ndjson")
	if len(errs) != 1 || len(rows) != 1 || rows[0]["doc_number"] != "7" {
		t.Fatalf("errs=%v rows=%v", errs, rows)
	}
	e := errs[0]
	if e["field_path"] != "Invoice.TotalAmt" || e["gcs_uri"] != rows[0]["_raw_gcs_uri"] || e["run_id"] != "run1" || e["record_id"] != "1" {
		t.Errorf("ingest error = %v", e)
	}

	// Replaying the archived payload reproduces the error offline (dry run writes nothing).
	rep, err := s.Replay(context.Background(), e["gcs_uri"].(string), "123", true)
	if err != nil || rep.Records["Invoice"] != 1 || len(rep.IngestErrors) != 1 {
		t.Errorf("replay = %+v, %v", rep, err)
	}
	if got := readRows(t, dir, "tables/invoice.ndjson"); len(got) != 1 {
		t.Error("dry-run replay must not write rows")
	}
}

func TestReportsAreSnapshotted(t *testing.T) {
	s, _, dir := newTestSyncer(t)
	s.Entities = nil
	s.SyncReports = true
	if _, err := s.SyncRealm(context.Background(), &fakeAPI{}, "123"); err != nil {
		t.Fatal(err)
	}
	snaps := readRows(t, dir, "_report_snapshots.ndjson")
	if len(snaps) != 4 {
		t.Fatalf("expected 4 snapshots (P&L and balance sheet x accrual and cash), got %d", len(snaps))
	}
	first := snaps[0]
	if first["report_name"] != "ProfitAndLoss" || first["accounting_method"] != "Accrual" || first["period_start"] != "2025-01-01" ||
		first["period_end"] != "2026-11-12" || !strings.Contains(first["gcs_uri"].(string), "entity=_report") {
		t.Errorf("snapshot = %v", first)
	}
}

func TestSyncWithoutArchive(t *testing.T) {
	s, _, dir := newTestSyncer(t, "Invoice")
	s.Archiver = qbo.NopArchiver{}
	api := &fakeAPI{data: map[string][]string{"Invoice": {`{"Id":"1","TotalAmt":"oops"}`}}}
	if _, err := s.SyncRealm(context.Background(), api, "123"); err != nil {
		t.Fatal(err)
	}
	rows, errs := readRows(t, dir, "tables/invoice.ndjson"), readRows(t, dir, "_ingest_errors.ndjson")
	if len(rows) != 1 || len(errs) != 1 {
		t.Fatalf("rows=%v errs=%v", rows, errs)
	}
	if _, has := rows[0]["_raw_gcs_uri"]; has {
		t.Error("_raw_gcs_uri must be NULL when archival is disabled")
	}
	if _, has := errs[0]["gcs_uri"]; has || errs[0]["raw_record_json"] == "" {
		t.Errorf("ingest error should keep the raw record but no uri: %v", errs[0])
	}
}
