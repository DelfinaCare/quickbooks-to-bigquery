package bq

import (
	"testing"
	"time"
)

func row(id string, deleted bool, updated string, extra map[string]any) map[string]any {
	r := map[string]any{"realm_id": "r1", "id": id, "is_deleted": deleted, "updated_at": updated, "_extracted_at": "2024-03-01T00:00:00Z"}
	for k, v := range extra {
		r[k] = v
	}
	return r
}

func byID(cs []change) map[string]change {
	out := map[string]change{}
	for _, c := range cs {
		out[c.row["id"].(string)] = c
	}
	return out
}

func TestPlanChangesTombstoneKeepsLastKnownValues(t *testing.T) {
	latest := map[string]map[string]any{
		"1": row("1", true, "2024-02-01T00:00:00Z", map[string]any{"_raw_gcs_uri": "gs://b/new"}),
		"2": row("2", true, "2024-02-01T00:00:00Z", nil), // never seen before
		"3": row("3", false, "2024-02-01T00:00:00Z", map[string]any{"doc_number": "42"}),
	}
	prior := map[string]map[string]any{
		"1": row("1", false, "2024-01-01T00:00:00Z", map[string]any{"doc_number": "7", "_raw_gcs_uri": "gs://b/old"}),
	}
	cs := byID(planChanges(latest, prior, false, time.Now()))
	if len(cs) != 3 {
		t.Fatalf("got %d changes", len(cs))
	}
	one := cs["1"].row
	if one["doc_number"] != "7" || one["is_deleted"] != true || one["updated_at"] != "2024-02-01T00:00:00Z" || one["_raw_gcs_uri"] != "gs://b/new" {
		t.Fatalf("tombstone = %v", one)
	}
	if cs["2"].row["is_deleted"] != true {
		t.Fatalf("unknown tombstone must still be written: %v", cs["2"].row)
	}
	for _, c := range cs {
		if c.changeType != ChangeUpsert {
			t.Fatalf("change type %s", c.changeType)
		}
	}
}

func TestPlanChangesReconcile(t *testing.T) {
	now := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	latest := map[string]map[string]any{
		"live":   row("live", false, "2024-02-01T00:00:00Z", nil),
		"revive": row("revive", false, "2024-02-01T00:00:00Z", nil),
	}
	prior := map[string]map[string]any{
		"gone":    row("gone", false, "2024-01-01T00:00:00Z", map[string]any{"doc_number": "9"}),
		"revive":  row("revive", true, "2024-02-20T00:00:00Z", nil),  // flagged by an earlier reconcile
		"deleted": row("deleted", true, "2024-01-05T00:00:00Z", nil), // already a tombstone: untouched
	}
	cs := byID(planChanges(latest, prior, true, now))
	if _, ok := cs["deleted"]; ok || len(cs) != 3 {
		t.Fatalf("changes = %v", cs)
	}
	gone := cs["gone"]
	if gone.row["is_deleted"] != true || gone.row["doc_number"] != "9" || gone.row["updated_at"] != "2024-03-01T12:00:00Z" ||
		gone.seq != SequenceNumber(now, now) {
		t.Fatalf("reconcile tombstone = %+v", gone)
	}
	want := SequenceNumber(time.Date(2024, 2, 20, 0, 0, 0, 1000, time.UTC), time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC))
	if cs["revive"].seq != want || cs["revive"].row["updated_at"] != "2024-02-01T00:00:00Z" {
		t.Fatalf("snapshot row must order after the old tombstone: %+v (want seq %s)", cs["revive"], want)
	}
}
