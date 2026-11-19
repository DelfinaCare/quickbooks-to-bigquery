package qbo

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

func TestPlanSyncChoosesCDCOrBackfill(t *testing.T) {
	reg := schema.MustDefault()
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	pick := func(names ...string) []*schema.Entity {
		var out []*schema.Entity
		for _, n := range names {
			e, _ := reg.Entity(n)
			out = append(out, e)
		}
		return out
	}
	recent, other := now.Add(-time.Hour), now.Add(-3*time.Hour)
	fresh := now.Add(-24 * time.Hour) // last full refresh
	state := map[string]EntityState{
		"Invoice":  {HighWatermark: recent, LastFullSyncAt: fresh},
		"Bill":     {HighWatermark: recent, LastFullSyncAt: fresh},
		"Customer": {HighWatermark: other, LastFullSyncAt: fresh},
		"Vendor":   {HighWatermark: now.Add(-30 * 24 * time.Hour), LastFullSyncAt: fresh}, // watermark too old for CDC
		"Item":     {HighWatermark: recent, LastFullSyncAt: now.Add(-8 * 24 * time.Hour)}, // weekly reconcile due
		"TaxCode":  {HighWatermark: recent, LastFullSyncAt: fresh},                        // sync: full
		// Payment: no state -> initial backfill
	}
	ents := pick("Invoice", "Bill", "Customer", "Vendor", "Item", "TaxCode", "Payment")
	plan := PlanSync(now, ents, state, false, 7*24*time.Hour)
	wantCDC := []CDCGroup{{Since: other, Entities: []string{"Customer"}}, {Since: recent, Entities: []string{"Bill", "Invoice"}}}
	if !reflect.DeepEqual(plan.CDC, wantCDC) {
		t.Errorf("CDC groups = %+v, want %+v", plan.CDC, wantCDC)
	}
	if got := strings.Join(plan.Full, ","); got != "Item,Payment,TaxCode,Vendor" {
		t.Errorf("full = %s", got)
	}
	// A watermark just inside 29 days still uses CDC; the periodic refresh can be disabled.
	state["Vendor"] = EntityState{HighWatermark: now.Add(-CDCMaxAge + time.Minute)}
	plan = PlanSync(now, pick("Vendor", "Item"), state, false, 0)
	if len(plan.Full) != 0 || len(plan.CDC) != 2 {
		t.Errorf("plan = %+v", plan)
	}
	// --full overrides everything.
	if plan = PlanSync(now, ents, state, true, 0); len(plan.CDC) != 0 || len(plan.Full) != len(ents) {
		t.Errorf("forced plan = %+v", plan)
	}
}

func TestQuerySQL(t *testing.T) {
	reg := schema.MustDefault()
	inv, _ := reg.Entity("Invoice")
	acct, _ := reg.Entity("Account")
	if got := QuerySQL(inv, 1001); got != "SELECT * FROM Invoice ORDERBY Id STARTPOSITION 1001 MAXRESULTS 1000" {
		t.Errorf("got %q", got)
	}
	// Name-list entities must ask for inactive records explicitly.
	if got := QuerySQL(acct, 1); got != "SELECT * FROM Account WHERE Active IN (true, false) ORDERBY Id STARTPOSITION 1 MAXRESULTS 1000" {
		t.Errorf("got %q", got)
	}
	if got := CountSQL(acct); got != "SELECT COUNT(*) FROM Account WHERE Active IN (true, false)" {
		t.Errorf("got %q", got)
	}
}

func TestParseRecords(t *testing.T) {
	q, err := ParseRecords([]byte(`{"QueryResponse":{"Invoice":[{"Id":"1"},{"Id":"2"}],"startPosition":1,"maxResults":2,"totalCount":2},"time":"x"}`))
	if err != nil || len(q["Invoice"]) != 2 || len(q) != 1 {
		t.Errorf("query: %v %v", q, err)
	}
	c, err := ParseRecords([]byte(`{"CDCResponse":[{"QueryResponse":[{},{"Purchase":[{"Id":"5"}],"startPosition":1,"maxResults":1},{"Invoice":[{"Id":"9","status":"Deleted"}]}]}],"time":"x"}`))
	if err != nil || len(c["Purchase"]) != 1 || string(c["Invoice"][0]) != `{"Id":"9","status":"Deleted"}` {
		t.Errorf("cdc: %v %v", c, err)
	}
	one, err := ParseRecords([]byte(`{"CompanyInfo":{"Id":"1","CompanyName":"X"},"time":"2026-11-11T17:13:18.763-08:00"}`))
	if err != nil || len(one["CompanyInfo"]) != 1 || len(one) != 1 {
		t.Errorf("single read: %v %v", one, err)
	}
	if empty, err := ParseRecords([]byte(`{"QueryResponse":{},"time":"x"}`)); err != nil || len(empty) != 0 {
		t.Errorf("empty: %v %v", empty, err)
	}
	_, err = ParseRecords([]byte(`{"Fault":{"Error":[{"Message":"Invalid query","Detail":"QueryValidationError: bad","code":"4001"}],"type":"ValidationFault"}}`))
	if err == nil || !strings.Contains(err.Error(), "QueryValidationError") {
		t.Errorf("fault should surface, got %v", err)
	}
	if n, err := ParseCount([]byte(`{"QueryResponse":{"totalCount":31}}`)); err != nil || n != 31 {
		t.Errorf("count = %d, %v", n, err)
	}
}

func TestArchivePathAndLocalRoundTrip(t *testing.T) {
	meta := ArchiveMeta{RealmID: "9341458687995992", Entity: "Invoice", RunID: "20261112t010203z_ab12cd", Seq: 7, Endpoint: "query",
		FetchedAt: time.Date(2026, 11, 12, 23, 59, 0, 0, time.FixedZone("PST", -8*3600))}
	want := "realm_id=9341458687995992/entity=Invoice/dt=2026-11-13/run_id=20261112t010203z_ab12cd/00007_query.json.gz"
	if got := ObjectPath(meta); got != want {
		t.Errorf("ObjectPath = %s", got)
	}
	if RealmFromURI("gs://b/raw/"+want) != "9341458687995992" {
		t.Error("RealmFromURI failed")
	}
	a := &LocalArchiver{Dir: t.TempDir()}
	body := []byte(`{"QueryResponse":{"Invoice":[{"Id":"1"}]}}`)
	uri, err := a.Archive(context.Background(), meta, body)
	if err != nil || !strings.HasSuffix(uri, want) {
		t.Fatalf("uri=%s err=%v", uri, err)
	}
	got, err := a.Read(context.Background(), uri)
	if err != nil || string(got) != string(body) {
		t.Errorf("round trip = %q, %v", got, err)
	}
}
