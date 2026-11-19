package bq

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// decodeRow is Encode's inverse, rendering values the way the mapper writes
// NDJSON, so a round trip can be compared with the original row.
func decodeRow(t *testing.T, c *RowCodec, b []byte) map[string]any {
	t.Helper()
	m := dynamicpb.NewMessage(c.msg)
	if err := proto.Unmarshal(b, m); err != nil {
		t.Fatal(err)
	}
	return toMap(m, c.Schema)
}

func toMap(m protoreflect.Message, sch bigquery.Schema) map[string]any {
	out := map[string]any{}
	fields := m.Descriptor().Fields()
	for _, f := range sch {
		fd := fields.ByName(protoreflect.Name(f.Name))
		if f.Repeated {
			l := m.Get(fd).List()
			if l.Len() == 0 {
				continue
			}
			arr := make([]any, l.Len())
			for i := range arr {
				arr[i] = fromValue(f, l.Get(i))
			}
			out[f.Name] = arr
			continue
		}
		if m.Has(fd) {
			out[f.Name] = fromValue(f, m.Get(fd))
		}
	}
	return out
}

func fromValue(f *bigquery.FieldSchema, v protoreflect.Value) any {
	switch f.Type {
	case bigquery.RecordFieldType:
		return toMap(v.Message(), f.Schema)
	case bigquery.TimestampFieldType:
		return schema.FormatTimestamp(time.UnixMicro(v.Int()))
	case bigquery.DateFieldType:
		return epoch.AddDate(0, 0, int(v.Int())).Format("2006-01-02")
	case bigquery.BooleanFieldType:
		return v.Bool()
	case bigquery.IntegerFieldType:
		return json.Number(v.String())
	}
	return v.String()
}

// normalize re-decodes a value through JSON so both sides compare equally.
func normalize(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return dropEmpty(out)
}

// dropEmpty removes nulls and empty arrays (and unifies numbers and strings), which protobuf cannot distinguish
// from absent fields (BigQuery stores them as NULL / an empty array).
func dropEmpty(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			e = dropEmpty(e)
			if arr, ok := e.([]any); e == nil || ok && len(arr) == 0 {
				delete(x, k)
				continue
			}
			x[k] = e
		}
	case []any:
		for i, e := range x {
			x[i] = dropEmpty(e)
		}
	case json.Number:
		return x.String() // BigQuery renders NUMERIC as a JSON number, the mapper as a string
	case string:
		if ts, err := time.Parse(time.RFC3339Nano, x); err == nil && strings.Contains(x, "T") {
			return schema.FormatTimestamp(ts) // BigQuery keeps trailing zeros in fractions
		}
	}
	return v
}

func roundTrip(t *testing.T, c *RowCodec, row map[string]any) {
	t.Helper()
	b, err := c.Encode(row, ChangeUpsert, SequenceNumber(time.Now(), time.Now()))
	if err != nil {
		t.Fatalf("encode %v: %v", row["id"], err)
	}
	got, want := normalize(t, decodeRow(t, c, b)), normalize(t, row)
	if !reflect.DeepEqual(got, want) {
		gb, _ := json.Marshal(got)
		wb, _ := json.Marshal(want)
		t.Fatalf("round trip of %v differs:\n got %s\nwant %s", row["id"], gb, wb)
	}
}

func TestRowCodecRoundTripsMappedRows(t *testing.T) {
	reg, err := schema.Default()
	if err != nil {
		t.Fatal(err)
	}
	inv, _ := reg.Entity("Invoice")
	c, err := NewRowCodec(TableSchema(inv), true)
	if err != nil {
		t.Fatal(err)
	}
	raw := `{"Id":"130","SyncToken":"2","MetaData":{"CreateTime":"2024-01-02T03:04:05-08:00","LastUpdatedTime":"2024-02-03T04:05:06.123-08:00"},
		"TxnDate":"2024-01-02","DueDate":"2024-02-01","TotalAmt":1234567.891,"DocNumber":"1037",
		"CustomerRef":{"value":"3","name":"Cool Cars"},
		"Line":[{"Id":"1","LineNum":1,"Amount":100.5,"DetailType":"SalesItemLineDetail",
		         "SalesItemLineDetail":{"ItemRef":{"value":"5","name":"Rock Fountain"},"Qty":2,"UnitPrice":50.25}},
		        {"Amount":100.5,"DetailType":"SubTotalLineDetail","SubTotalLineDetail":{}}]}`
	mapped, err := inv.Map([]byte(raw), &schema.MapContext{RealmID: "123", ExtractedAt: time.Unix(1700000000, 123456000)})
	if err != nil || len(mapped.Errors) > 0 {
		t.Fatalf("map: %v %v", err, mapped.Errors)
	}
	roundTrip(t, c, ndjsonRow(t, mapped.Row))

	// A tombstone carries only the envelope.
	tomb, _ := inv.Map([]byte(`{"Id":"131","status":"Deleted","MetaData":{"LastUpdatedTime":"2024-02-03T04:05:06Z"}}`),
		&schema.MapContext{RealmID: "123", ExtractedAt: time.Now()})
	roundTrip(t, c, ndjsonRow(t, tomb.Row))
}

// ndjsonRow returns a row as Merge sees it: decoded back from NDJSON.
func ndjsonRow(t *testing.T, row map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(row)
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out map[string]any
	if err := dec.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestRowCodecRecordedRows round-trips NDJSON produced by
// `qbo-bq sync --local-out DIR` when QBO_ROWS_DIR=DIR/tables is set.
func TestRowCodecRecordedRows(t *testing.T) {
	dir := os.Getenv("QBO_ROWS_DIR")
	if dir == "" {
		t.Skip("QBO_ROWS_DIR not set")
	}
	reg, err := schema.Default()
	if err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.ndjson"))
	n := 0
	for _, f := range files {
		ent, ok := reg.Entity(strings.TrimSuffix(filepath.Base(f), ".ndjson"))
		if !ok {
			continue
		}
		c, err := NewRowCodec(TableSchema(ent), true)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := os.ReadFile(f)
		rows, err := decodeNDJSON([][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			roundTrip(t, c, r)
			n++
		}
	}
	t.Logf("round-tripped %d rows from %d files", n, len(files))
}

func TestSequenceNumberOrdersLikeMerge(t *testing.T) {
	t0 := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cmp := func(a, b string) int { // BigQuery's section-wise unsigned hex comparison
		as, bs := strings.Split(a, "/"), strings.Split(b, "/")
		for i := range as {
			if len(as[i]) != len(bs[i]) {
				return len(as[i]) - len(bs[i])
			}
			if c := strings.Compare(as[i], bs[i]); c != 0 {
				return c
			}
		}
		return 0
	}
	older := SequenceNumber(t0, t0.Add(time.Hour))
	newer := SequenceNumber(t0.Add(time.Microsecond), t0)
	if cmp(older, newer) >= 0 {
		t.Fatalf("updated_at must dominate: %s vs %s", older, newer)
	}
	if cmp(SequenceNumber(t0, t0), SequenceNumber(t0, t0.Add(time.Second))) >= 0 {
		t.Fatal("_extracted_at must break ties")
	}
}

func TestStalenessInterval(t *testing.T) {
	if got := staleness(90 * time.Minute).String(); got != "0-0 0 1:30:0" {
		t.Fatalf("got %q", got)
	}
	if staleness(0) != nil {
		t.Fatal("zero staleness must leave the option unset")
	}
}
