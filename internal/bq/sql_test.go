package bq

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

func TestTableSchemaForEveryEntity(t *testing.T) {
	for _, e := range schema.MustDefault().Entities() {
		s := TableSchema(e)
		if s[0].Name != "realm_id" || !s[0].Required || s[1].Name != "id" || !s[1].Required {
			t.Errorf("%s: envelope must lead the schema", e.Name)
		}
		var walk func(path string, s bigquery.Schema)
		walk = func(path string, s bigquery.Schema) {
			for _, f := range s {
				if f.Type == "" {
					t.Errorf("%s%s: no BigQuery type", path, f.Name)
				}
				if f.Type == bigquery.RecordFieldType {
					if len(f.Schema) == 0 {
						t.Errorf("%s%s: empty RECORD", path, f.Name)
					}
					walk(path+f.Name+".", f.Schema)
				}
			}
		}
		walk(e.Table+".", s)
	}
	inv, _ := schema.MustDefault().Entity("Invoice")
	for _, f := range TableSchema(inv) {
		switch f.Name {
		case "line":
			if !f.Repeated || f.Type != bigquery.RecordFieldType {
				t.Errorf("line should be ARRAY<STRUCT>")
			}
		case "total_amt":
			if f.Type != bigquery.NumericFieldType {
				t.Errorf("total_amt should be NUMERIC")
			}
		case "customer_ref":
			if f.Type != bigquery.RecordFieldType || f.Repeated {
				t.Errorf("customer_ref should be STRUCT")
			}
		}
	}
}

func TestMergeSQL(t *testing.T) {
	cols := []string{"realm_id", "id", "updated_at", "is_deleted", "_extracted_at", "_raw_gcs_uri", "total_amt", "line"}
	sql, err := MergeSQL("`p.d.invoice`", "`p.d._stg_invoice_r1_1`", cols, false)
	if err != nil {
		t.Fatal(err)
	}
	want := "MERGE `p.d.invoice` T\n" +
		"USING (\n  SELECT * FROM `p.d._stg_invoice_r1_1`\n  WHERE realm_id = @realm_id\n" +
		"  QUALIFY ROW_NUMBER() OVER (PARTITION BY realm_id, id ORDER BY updated_at DESC, _extracted_at DESC) = 1\n) S\n" +
		"ON T.realm_id = @realm_id AND T.realm_id = S.realm_id AND T.id = S.id\n" + // constant filter -> cluster pruning
		"WHEN MATCHED AND S.is_deleted THEN\n" +
		"  UPDATE SET T.is_deleted = TRUE, T.updated_at = S.updated_at, T._extracted_at = S._extracted_at, T._raw_gcs_uri = S._raw_gcs_uri\n" +
		"WHEN MATCHED AND S.updated_at >= T.updated_at THEN\n" +
		"  UPDATE SET T.updated_at = S.updated_at, T.is_deleted = S.is_deleted, T._extracted_at = S._extracted_at, T._raw_gcs_uri = S._raw_gcs_uri, T.total_amt = S.total_amt, T.line = S.line\n" +
		"WHEN NOT MATCHED BY TARGET THEN\n" +
		"  INSERT (realm_id, id, updated_at, is_deleted, _extracted_at, _raw_gcs_uri, total_amt, line)\n" +
		"  VALUES (S.realm_id, S.id, S.updated_at, S.is_deleted, S._extracted_at, S._raw_gcs_uri, S.total_amt, S.line)"
	if sql != want {
		t.Errorf("incremental MERGE mismatch:\n%s\n--- want ---\n%s", sql, want)
	}
	full, _ := MergeSQL("`p.d.invoice`", "`p.d.stg`", cols, true)
	for _, frag := range []string{
		"ON T.realm_id = @realm_id AND T.realm_id = S.realm_id AND T.id = S.id\n",
		"WHEN MATCHED AND TRUE THEN",
		"WHEN NOT MATCHED BY SOURCE AND T.realm_id = @realm_id AND NOT T.is_deleted THEN\n  UPDATE SET T.is_deleted = TRUE, T.updated_at = @now, T._extracted_at = @now",
	} {
		if !strings.Contains(full, frag) {
			t.Errorf("reconciling MERGE lacks %q:\n%s", frag, full)
		}
	}
	if strings.Contains(sql, "NOT MATCHED BY SOURCE") {
		t.Error("incremental MERGE must never flag deletes by absence")
	}
	if _, err := MergeSQL("t", "s", []string{"id", "x; DROP TABLE y"}, false); err == nil {
		t.Error("unsafe column names must be rejected")
	}
	// Every real entity produces a MERGE covering all of its columns.
	for _, e := range schema.MustDefault().Entities() {
		var names []string
		for _, f := range TableSchema(e) {
			names = append(names, f.Name)
		}
		sql, err := MergeSQL("`p.d."+e.Table+"`", "`p.d.s`", names, true)
		if err != nil || strings.Count(sql, "S.") < 2*len(names) {
			t.Errorf("%s: bad MERGE (%v)", e.Name, err)
		}
	}
}

func TestEvolveSchema(t *testing.T) {
	str := func(n string) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: n, Type: bigquery.StringFieldType}
	}
	existing := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "legacy", Type: bigquery.StringFieldType}, // no longer in YAML: must be kept
		{Name: "line", Type: bigquery.RecordFieldType, Repeated: true, Schema: bigquery.Schema{str("id")}},
	}
	desired := bigquery.Schema{
		{Name: "id", Type: bigquery.StringFieldType, Required: true},
		{Name: "line", Type: bigquery.RecordFieldType, Repeated: true, Schema: bigquery.Schema{str("id"), str("description"),
			{Name: "detail", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{str("qty")}}}},
		{Name: "new_required", Type: bigquery.NumericFieldType, Required: true},
	}
	merged, added, err := EvolveSchema(existing, desired)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(added, ","); got != "line.description,line.detail,new_required" {
		t.Errorf("added = %s", got)
	}
	if len(merged) != 4 || merged[1].Name != "legacy" || merged[3].Name != "new_required" || merged[3].Required {
		t.Errorf("merged top level wrong: %+v", merged)
	}
	if len(merged[2].Schema) != 3 || len(existing[2].Schema) != 1 {
		t.Errorf("nested merge wrong or mutated its input")
	}
	if _, added, _ := EvolveSchema(merged, desired); len(added) != 0 {
		t.Errorf("evolution must be idempotent, got %v", added)
	}
	_, _, err = EvolveSchema(existing, bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}})
	if err == nil || !strings.Contains(err.Error(), "manual migration") {
		t.Errorf("type change should be refused, got %v", err)
	}
	if _, _, err = EvolveSchema(existing, bigquery.Schema{{Name: "line", Type: bigquery.RecordFieldType}}); err == nil {
		t.Error("repeated -> singular should be refused")
	}
}

func TestSaveStateSQL(t *testing.T) {
	sql := SaveStateSQL("`p.d._sync_state`", []qbo.EntityState{
		{RealmID: "123", Entity: "Invoice", HighWatermark: time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC), LastRunID: "r1"},
		{RealmID: "12'3", Entity: "Bill", HighWatermark: time.Date(2026, 11, 12, 1, 0, 0, 0, time.UTC), LastFullSyncAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), LastRunID: "r1"},
	})
	for _, frag := range []string{
		"SELECT '123' AS realm_id, 'Invoice' AS entity, TIMESTAMP '2026-11-12T01:00:00Z' AS high_watermark, CAST(NULL AS TIMESTAMP) AS last_full_sync_at, 'r1' AS last_run_id",
		"UNION ALL", `'12\'3'`, "TIMESTAMP '2026-11-01T00:00:00Z' AS last_full_sync_at",
		"ON T.realm_id = S.realm_id AND T.entity = S.entity",
	} {
		if !strings.Contains(sql, frag) {
			t.Errorf("state SQL lacks %q:\n%s", frag, sql)
		}
	}
}

func TestStagingTable(t *testing.T) {
	if got := StagingTable("invoice", "20261112t010203z_ab12cd", "9341-45"); got != "_stg_invoice_20261112t010203z_ab12cd_9341_45" {
		t.Errorf("staging table = %s", got)
	}
}
