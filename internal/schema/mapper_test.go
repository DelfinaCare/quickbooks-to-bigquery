package schema

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

const invoiceFixture = `{
  "AllowOnlineACHPayment": true, "domain": "QBO", "sparse": false, "Id": "130", "SyncToken": "3",
  "MetaData": {"CreateTime": "2026-10-13T14:49:30-07:00", "LastUpdatedTime": "2026-10-15T13:25:20-07:00", "LastModifiedByRef": {"value": "9"}},
  "DocNumber": "1037", "TxnDate": "2026-10-13", "DueDate": "2026-11-12",
  "CurrencyRef": {"value": "USD", "name": "United States Dollar"}, "ExchangeRate": 1.23456789012345,
  "CustomerRef": {"value": "24", "name": "Sonnenschein Family Store"},
  "BillAddr": {"Id": "95", "Line1": "Russ Sonnenschein", "Lat": "37.4238562", "Long": "-122.1141681"},
  "LinkedTxn": [{"TxnId": "100", "TxnType": "Estimate"}],
  "Line": [
    {"Id": "1", "LineNum": 1, "Description": "Rock Fountain", "Amount": 275.0, "DetailType": "SalesItemLineDetail",
     "SalesItemLineDetail": {"ItemRef": {"value": "5", "name": "Rock Fountain"}, "UnitPrice": 275, "Qty": 1, "ServiceDate": "2026-10-13",
       "TaxCodeRef": {"value": "TAX"}, "MarkupInfo": {"PercentBased": true, "Value": 0.1}}},
    {"Id": "2", "LineNum": 2, "Amount": 12.75, "DetailType": "SalesItemLineDetail",
     "SalesItemLineDetail": {"ItemRef": {"value": "11"}, "UnitPrice": 4.25, "Qty": 3}},
    {"Amount": 335.25, "DetailType": "SubTotalLineDetail", "SubTotalLineDetail": {}},
    {"Amount": 26.82, "DetailType": "DiscountLineDetail", "DiscountLineDetail": {"PercentBased": true, "DiscountPercent": 8, "DiscountAccountRef": {"value": "86"}}}
  ],
  "TxnTaxDetail": {"TxnTaxCodeRef": {"value": "2"}, "TotalTax": 26.82,
    "TaxLine": [{"Amount": 26.82, "DetailType": "TaxLineDetail", "TaxLineDetail": {"TaxRateRef": {"value": "3"}, "PercentBased": true, "TaxPercent": 8, "NetAmountTaxable": 335.25}}]},
  "TotalAmt": 362.07, "Balance": 0.1E-1, "ApplyTaxAfterDiscount": false, "EmailStatus": "NotSet",
  "BrandNewField": "surprise", "CustomerMemo": {"value": "Thanks", "Mystery": 1}
}`

var extractedAt = time.Date(2026, 11, 12, 1, 2, 3, 0, time.UTC)

func mapFixture(t *testing.T, entity, raw string) (*Mapped, []string) {
	t.Helper()
	ent, ok := MustDefault().Entity(entity)
	if !ok {
		t.Fatalf("no entity %s", entity)
	}
	var unknown []string
	m, err := ent.Map([]byte(raw), &MapContext{
		RunID: "run1", RealmID: "123", GCSURI: "gs://bucket/raw/x.json.gz", ExtractedAt: extractedAt,
		OnUnknown: func(_, path, uri string) {
			if uri != "gs://bucket/raw/x.json.gz" {
				t.Errorf("unknown-field callback got gcs uri %q", uri)
			}
			unknown = append(unknown, path)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return m, unknown
}

func TestMapInvoice(t *testing.T) {
	m, unknown := mapFixture(t, "Invoice", invoiceFixture)
	if len(m.Errors) != 0 {
		t.Fatalf("unexpected ingest errors: %+v", m.Errors)
	}
	row := m.Row
	want := map[string]any{
		"realm_id": "123", "id": "130", "sync_token": "3", "is_deleted": false,
		"created_at": "2026-10-13T21:49:30Z", "updated_at": "2026-10-15T20:25:20Z", // -07:00 normalised to UTC
		"_extracted_at": "2026-11-12T01:02:03Z", "_raw_gcs_uri": "gs://bucket/raw/x.json.gz",
		"doc_number": "1037", "txn_date": "2026-10-13", "due_date": "2026-11-12",
		"total_amt": "362.07", "balance": "0.01", "exchange_rate": "1.23456789", // exact decimals; 9-digit scale
		"allow_online_ach_payment": true, "apply_tax_after_discount": false, "email_status": "NotSet",
	}
	for k, v := range want {
		if !reflect.DeepEqual(row[k], v) {
			t.Errorf("row[%q] = %#v, want %#v", k, row[k], v)
		}
	}
	lines := row["line"].([]any)
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d", len(lines))
	}
	l0 := lines[0].(map[string]any)
	d0 := l0["sales_item_line_detail"].(map[string]any)
	if l0["line_num"] != int64(1) || l0["amount"] != "275" || d0["unit_price"] != "275" || d0["service_date"] != "2026-10-13" {
		t.Errorf("line 0 mapped wrong: %#v", l0)
	}
	if got := d0["markup_info"].(map[string]any)["value"]; got != "0.1" {
		t.Errorf("markup value = %#v", got)
	}
	if got := d0["item_ref"].(map[string]any); got["value"] != "5" || got["name"] != "Rock Fountain" {
		t.Errorf("item_ref = %#v", got)
	}
	if got := lines[1].(map[string]any)["sales_item_line_detail"].(map[string]any)["unit_price"]; got != "4.25" {
		t.Errorf("unit_price = %#v", got)
	}
	tax := row["txn_tax_detail"].(map[string]any)["tax_line"].([]any)[0].(map[string]any)
	if tax["tax_line_detail"].(map[string]any)["net_amount_taxable"] != "335.25" {
		t.Errorf("nested tax line mapped wrong: %#v", tax)
	}
	// Unknown fields are reported with their path but never fail the record;
	// envelope noise (domain, sparse, MetaData.LastModifiedByRef) is not reported.
	if got := strings.Join(sorted(unknown), ","); got != "Invoice.BrandNewField,Invoice.CustomerMemo.Mystery" {
		t.Errorf("unknown fields = %s", got)
	}
	if _, err := json.Marshal(row); err != nil {
		t.Errorf("row is not JSON-serialisable: %v", err)
	}
}

func sorted(s []string) []string {
	out := append([]string{}, s...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestMapSoftDelete(t *testing.T) {
	m, _ := mapFixture(t, "Invoice", `{"domain":"QBO","status":"Deleted","Id":"77","MetaData":{"LastUpdatedTime":"2026-11-01T10:00:00-08:00"}}`)
	want := map[string]any{
		"realm_id": "123", "id": "77", "is_deleted": true, "updated_at": "2026-11-01T18:00:00Z",
		"_extracted_at": "2026-11-12T01:02:03Z", "_raw_gcs_uri": "gs://bucket/raw/x.json.gz",
	}
	if !m.Deleted || !reflect.DeepEqual(m.Row, want) {
		t.Errorf("tombstone = %#v", m.Row)
	}
	// Without MetaData the deletion time falls back to the extraction time.
	m, _ = mapFixture(t, "Invoice", `{"status":"Deleted","Id":"78"}`)
	if m.Row["updated_at"] != "2026-11-12T01:02:03Z" {
		t.Errorf("updated_at fallback = %v", m.Row["updated_at"])
	}
}

func TestMapIsolatesFieldErrors(t *testing.T) {
	m, _ := mapFixture(t, "Invoice", `{"Id":"9","TotalAmt":"twelve","TxnDate":"13/10/2026","DocNumber":"A-1",
		"Line":[{"Id":"1","Amount":5,"LineNum":"x"},{"Id":"2","Amount":7}],"CustomerRef":"not-an-object"}`)
	if m.Row == nil {
		t.Fatal("record should survive field errors")
	}
	if _, ok := m.Row["total_amt"]; ok {
		t.Error("bad NUMERIC should be left NULL")
	}
	if m.Row["doc_number"] != "A-1" {
		t.Error("good fields must still load")
	}
	if lines := m.Row["line"].([]any); len(lines) != 2 || lines[1].(map[string]any)["amount"] != "7" {
		t.Errorf("lines = %#v", m.Row["line"])
	}
	got := map[string]IngestError{}
	for _, e := range m.Errors {
		got[e.FieldPath] = e
	}
	for path, typ := range map[string]string{"Invoice.TotalAmt": "NUMERIC", "Invoice.TxnDate": "DATE", "Invoice.Line[].LineNum": "INT64", "Invoice.CustomerRef": "RECORD"} {
		e, ok := got[path]
		if !ok {
			t.Errorf("missing ingest error for %s (have %v)", path, m.Errors)
			continue
		}
		if e.ExpectedType != typ || e.GCSURI != "gs://bucket/raw/x.json.gz" || e.RecordID != "9" || e.RunID != "run1" ||
			e.RealmID != "123" || e.Entity != "Invoice" || !strings.Contains(e.RawRecordJSON, `"twelve"`) || !e.FailedAt.Equal(extractedAt) {
			t.Errorf("ingest error for %s incomplete: %+v", path, e)
		}
	}
	if got["Invoice.TotalAmt"].ObservedValue != `"twelve"` {
		t.Errorf("observed value = %s", got["Invoice.TotalAmt"].ObservedValue)
	}
	// A record without an Id cannot be keyed: dropped, with an error row.
	m, _ = mapFixture(t, "Invoice", `{"DocNumber":"1"}`)
	if m.Row != nil || len(m.Errors) != 1 {
		t.Errorf("id-less record: row=%v errors=%v", m.Row, m.Errors)
	}
}

func TestEntityWithoutMetaDataUsesExtractedAt(t *testing.T) {
	m, _ := mapFixture(t, "TaxCode", `{"Id":"TAX","Name":"TAX","Taxable":true}`)
	if m.Row["updated_at"] != "2026-11-12T01:02:03Z" || m.Row["id"] != "TAX" {
		t.Errorf("row = %#v", m.Row)
	}
}

func TestCoerce(t *testing.T) {
	n := func(s string) json.Number { return json.Number(s) }
	ok := []struct {
		kind Kind
		in   any
		want any
	}{
		{Numeric, n("100.0"), "100"}, {Numeric, n("0.1"), "0.1"}, {Numeric, n("1234567890123456789.123456789"), "1234567890123456789.123456789"},
		{Numeric, n("1.0000000005"), "1.000000001"}, {Numeric, n("-0.0"), "0"}, {Numeric, n("1.5E-4"), "0.00015"}, {Numeric, "42.50", "42.5"},
		{Numeric, n("-19.99"), "-19.99"},
		{Int64, n("7"), int64(7)}, {Int64, n("7.0"), int64(7)}, {Int64, "12", int64(12)},
		{Bool, true, true}, {Bool, "False", false},
		{Date, "2026-02-28", "2026-02-28"}, {Date, "2026-02-28-08:00", "2026-02-28"},
		{Timestamp, "2026-10-13T14:49:30-07:00", "2026-10-13T21:49:30Z"}, {Timestamp, "2026-10-13T21:49:30.123Z", "2026-10-13T21:49:30.123Z"},
		{Timestamp, "2026-10-13T14:49:30+0530", "2026-10-13T09:19:30Z"}, {Timestamp, "2026-10-13T14:49:30", "2026-10-13T14:49:30Z"},
		{String, "x", "x"}, {String, n("5"), "5"}, {String, true, "true"},
		{JSON, map[string]any{"any": []any{n("1")}}, `{"any":[1]}`},
	}
	for _, c := range ok {
		got, err := Coerce(c.kind, c.in)
		if err != nil || got != c.want {
			t.Errorf("Coerce(%s, %#v) = %#v, %v; want %#v", c.kind, c.in, got, err, c.want)
		}
	}
	bad := []struct {
		kind Kind
		in   any
	}{
		{Numeric, "abc"}, {Numeric, "1/3"}, {Numeric, true}, {Numeric, n("1e400")}, {Numeric, n("1e30")}, {Numeric, map[string]any{}},
		{Int64, n("1.5")}, {Int64, "x"}, {Bool, "yes"}, {Bool, n("1")},
		{Date, "2026-02-30"}, {Date, "10/13/2026"}, {Date, n("20261013")}, {Date, "2026-10-13junk"},
		{Timestamp, "yesterday"}, {Timestamp, n("1700000000")}, {String, []any{}},
	}
	for _, c := range bad {
		if got, err := Coerce(c.kind, c.in); err == nil {
			t.Errorf("Coerce(%s, %#v) = %#v, expected an error", c.kind, c.in, got)
		}
	}
}
