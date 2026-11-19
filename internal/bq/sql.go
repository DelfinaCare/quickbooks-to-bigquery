// Package bq loads mapped rows into BigQuery: table auto-creation, additive
// schema evolution, and staging + MERGE.
package bq

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

// Bookkeeping tables.
const (
	SyncStateTable       = "_sync_state"
	IngestErrorsTable    = "_ingest_errors"
	ReportSnapshotsTable = "_report_snapshots"
)

var kindToBQ = map[schema.Kind]bigquery.FieldType{
	schema.String:    bigquery.StringFieldType,
	schema.Numeric:   bigquery.NumericFieldType,
	schema.Date:      bigquery.DateFieldType,
	schema.Timestamp: bigquery.TimestampFieldType,
	schema.Bool:      bigquery.BooleanFieldType,
	schema.Int64:     bigquery.IntegerFieldType,
	schema.JSON:      bigquery.StringFieldType, // JSON text; query with PARSE_JSON()
	schema.Record:    bigquery.RecordFieldType,
}

// TableSchema compiles an entity (envelope + payload) to a BigQuery schema.
func TableSchema(e *schema.Entity) bigquery.Schema { return fieldsSchema(e.Columns()) }

func fieldsSchema(fields []*schema.Field) bigquery.Schema {
	out := make(bigquery.Schema, 0, len(fields))
	for _, f := range fields {
		fs := &bigquery.FieldSchema{
			Name: f.BQ, Type: kindToBQ[f.Kind], Repeated: f.Repeated,
			Required: f.Required && !f.Repeated, Description: f.Description,
		}
		if f.Kind == schema.Record {
			fs.Schema = fieldsSchema(f.Node.Fields)
		}
		out = append(out, fs)
	}
	return out
}

// EvolveSchema merges columns and nested fields that exist in desired but not
// in existing. It only ever adds NULLABLE/REPEATED fields; existing fields are
// never dropped, reordered or retyped (a type change is an error to be fixed
// by hand, since BigQuery cannot alter column types in place).
func EvolveSchema(existing, desired bigquery.Schema) (merged bigquery.Schema, added []string, err error) {
	return evolve(existing, desired, "")
}

func evolve(existing, desired bigquery.Schema, path string) (bigquery.Schema, []string, error) {
	merged := make(bigquery.Schema, len(existing))
	index := map[string]int{}
	for i, f := range existing {
		cp := *f
		merged[i] = &cp
		index[strings.ToLower(f.Name)] = i
	}
	var added []string
	for _, want := range desired {
		fp := path + want.Name
		i, ok := index[strings.ToLower(want.Name)]
		if !ok {
			cp := *want
			cp.Required = false
			merged = append(merged, &cp)
			added = append(added, fp)
			continue
		}
		have := merged[i]
		if have.Type != want.Type || have.Repeated != want.Repeated {
			return nil, nil, fmt.Errorf("column %s is %s in BigQuery but %s in the YAML schema; type changes need a manual migration",
				fp, describe(have), describe(want))
		}
		if want.Type == bigquery.RecordFieldType {
			sub, subAdded, err := evolve(have.Schema, want.Schema, fp+".")
			if err != nil {
				return nil, nil, err
			}
			have.Schema = sub
			added = append(added, subAdded...)
		}
	}
	return merged, added, nil
}

func describe(f *bigquery.FieldSchema) string {
	if f.Repeated {
		return "ARRAY<" + string(f.Type) + ">"
	}
	return string(f.Type)
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func tableRef(project, dataset, table string) string {
	return "`" + project + "." + dataset + "." + table + "`"
}

// StagingTable names the staging table for one entity/realm/run.
func StagingTable(table, runID, realmID string) string {
	clean := func(s string) string { return regexp.MustCompile(`[^A-Za-z0-9_]`).ReplaceAllString(s, "_") }
	return "_stg_" + table + "_" + clean(runID) + "_" + clean(realmID)
}

// MergeSQL generates the idempotent upsert from a staging table. columns are
// the target table's top-level column names. Query parameters: @realm_id
// always, and @now when reconcile is true.
//
//   - A batch only ever holds one company, so the join pins T.realm_id to the
//     constant @realm_id. Tables are clustered by (realm_id, id); a constant
//     filter on the leading clustering column lets BigQuery skip every other
//     company's blocks instead of scanning the whole table.
//   - Staged rows are de-duplicated per (realm_id, id), newest first.
//   - Tombstones (is_deleted) only flip the flag, preserving the last known
//     field values of the deleted record.
//   - Incremental loads update only when the staged version is not older.
//   - With reconcile, the staged rows are a full snapshot: every live source
//     row overwrites the target, and target rows of the realm missing from the
//     snapshot are flagged deleted.
func MergeSQL(target, staging string, columns []string, reconcile bool) (string, error) {
	for _, c := range columns {
		if !identRe.MatchString(c) {
			return "", fmt.Errorf("unsafe column name %q", c)
		}
	}
	var sets, vals []string
	for _, c := range columns {
		if c != "realm_id" && c != "id" {
			sets = append(sets, fmt.Sprintf("T.%s = S.%s", c, c))
		}
		vals = append(vals, "S."+c)
	}
	matched := "S.updated_at >= T.updated_at"
	if reconcile {
		matched = "TRUE"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "MERGE %s T\nUSING (\n  SELECT * FROM %s\n  WHERE realm_id = @realm_id\n  QUALIFY ROW_NUMBER() OVER (PARTITION BY realm_id, id ORDER BY updated_at DESC, _extracted_at DESC) = 1\n) S\n", target, staging)
	b.WriteString("ON T.realm_id = @realm_id AND T.realm_id = S.realm_id AND T.id = S.id\n")
	b.WriteString("WHEN MATCHED AND S.is_deleted THEN\n  UPDATE SET T.is_deleted = TRUE, T.updated_at = S.updated_at, T._extracted_at = S._extracted_at, T._raw_gcs_uri = S._raw_gcs_uri\n")
	fmt.Fprintf(&b, "WHEN MATCHED AND %s THEN\n  UPDATE SET %s\n", matched, strings.Join(sets, ", "))
	fmt.Fprintf(&b, "WHEN NOT MATCHED BY TARGET THEN\n  INSERT (%s)\n  VALUES (%s)", strings.Join(columns, ", "), strings.Join(vals, ", "))
	if reconcile {
		b.WriteString("\nWHEN NOT MATCHED BY SOURCE AND T.realm_id = @realm_id AND NOT T.is_deleted THEN\n  UPDATE SET T.is_deleted = TRUE, T.updated_at = @now, T._extracted_at = @now")
	}
	return b.String(), nil
}

func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\n", `\n`, "\r", `\r`).Replace(s) + "'"
}

func sqlTimestamp(t time.Time) string {
	if t.IsZero() {
		return "CAST(NULL AS TIMESTAMP)"
	}
	return "TIMESTAMP " + sqlString(t.UTC().Format(time.RFC3339))
}

// SaveStateSQL upserts _sync_state rows.
func SaveStateSQL(target string, states []qbo.EntityState) string {
	rows := make([]string, 0, len(states))
	for _, s := range states {
		rows = append(rows, fmt.Sprintf("  SELECT %s AS realm_id, %s AS entity, %s AS high_watermark, %s AS last_full_sync_at, %s AS last_run_id",
			sqlString(s.RealmID), sqlString(s.Entity), sqlTimestamp(s.HighWatermark), sqlTimestamp(s.LastFullSyncAt), sqlString(s.LastRunID)))
	}
	return fmt.Sprintf(`MERGE %s T
USING (
%s
) S
ON T.realm_id = S.realm_id AND T.entity = S.entity
WHEN MATCHED THEN
  UPDATE SET T.high_watermark = S.high_watermark, T.last_full_sync_at = S.last_full_sync_at, T.last_run_id = S.last_run_id, T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (realm_id, entity, high_watermark, last_full_sync_at, last_run_id, updated_at)
  VALUES (S.realm_id, S.entity, S.high_watermark, S.last_full_sync_at, S.last_run_id, CURRENT_TIMESTAMP())`,
		target, strings.Join(rows, "\n  UNION ALL\n"))
}

// ReportMergeSQL keeps one snapshot per report/method/period: re-runs on the
// same day replace that day's snapshot instead of piling up hourly copies.
func ReportMergeSQL(target, staging string) string {
	cols := []string{"realm_id", "report_name", "accounting_method", "period_start", "period_end", "gcs_uri", "extracted_at", "raw_payload_string"}
	var vals []string
	for _, c := range cols {
		vals = append(vals, "S."+c)
	}
	return fmt.Sprintf(`MERGE %s T
USING (
  SELECT * FROM %s
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY realm_id, report_name, accounting_method, period_start, period_end ORDER BY extracted_at DESC) = 1
) S
ON T.realm_id = S.realm_id AND T.report_name = S.report_name AND T.accounting_method = S.accounting_method
  AND T.period_start = S.period_start AND T.period_end = S.period_end
WHEN MATCHED THEN
  UPDATE SET T.gcs_uri = S.gcs_uri, T.extracted_at = S.extracted_at, T.raw_payload_string = S.raw_payload_string
WHEN NOT MATCHED THEN
  INSERT (%s)
  VALUES (%s)`, target, staging, strings.Join(cols, ", "), strings.Join(vals, ", "))
}

func req(name string, t bigquery.FieldType) *bigquery.FieldSchema {
	return &bigquery.FieldSchema{Name: name, Type: t, Required: true}
}

func opt(name string, t bigquery.FieldType) *bigquery.FieldSchema {
	return &bigquery.FieldSchema{Name: name, Type: t}
}

var (
	syncStateSchema = bigquery.Schema{
		req("realm_id", bigquery.StringFieldType), req("entity", bigquery.StringFieldType),
		opt("high_watermark", bigquery.TimestampFieldType), opt("last_full_sync_at", bigquery.TimestampFieldType),
		opt("last_run_id", bigquery.StringFieldType), opt("updated_at", bigquery.TimestampFieldType),
	}
	ingestErrorsSchema = bigquery.Schema{
		opt("run_id", bigquery.StringFieldType), opt("realm_id", bigquery.StringFieldType),
		opt("entity", bigquery.StringFieldType), opt("record_id", bigquery.StringFieldType),
		opt("field_path", bigquery.StringFieldType), opt("expected_type", bigquery.StringFieldType),
		opt("observed_value", bigquery.StringFieldType), opt("error_message", bigquery.StringFieldType),
		opt("gcs_uri", bigquery.StringFieldType), opt("raw_record_json", bigquery.StringFieldType),
		opt("failed_at", bigquery.TimestampFieldType),
	}
	reportSnapshotsSchema = bigquery.Schema{
		req("realm_id", bigquery.StringFieldType), req("report_name", bigquery.StringFieldType),
		req("accounting_method", bigquery.StringFieldType), req("period_start", bigquery.DateFieldType),
		req("period_end", bigquery.DateFieldType), opt("gcs_uri", bigquery.StringFieldType),
		req("extracted_at", bigquery.TimestampFieldType), opt("raw_payload_string", bigquery.StringFieldType),
	}
)
