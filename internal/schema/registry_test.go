package schema

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestEmbeddedSchemasCompile(t *testing.T) {
	reg, err := Default()
	if err != nil {
		t.Fatalf("schemas/*.yaml do not compile: %v", err)
	}
	if n := len(reg.Entities()); n != 29 {
		t.Errorf("expected 29 entities, got %d", n)
	}
	for _, e := range reg.Entities() {
		seen := map[string]bool{}
		for _, c := range e.Columns() {
			if seen[c.BQ] {
				t.Errorf("%s: duplicate column %s", e.Name, c.BQ)
			}
			seen[c.BQ] = true
		}
		assertNoDuplicates(t, e.Name, e.Node)
	}
	if e, ok := reg.Entity("invoice"); !ok || e.Name != "Invoice" {
		t.Errorf("lookup by table name failed")
	}
}

func assertNoDuplicates(t *testing.T, path string, n *Node) {
	t.Helper()
	seen := map[string]bool{}
	for _, f := range n.Fields {
		if seen[f.BQ] {
			t.Errorf("%s: duplicate nested column %s", path, f.BQ)
		}
		seen[f.BQ] = true
		if f.Kind == Record {
			if f.Node == nil {
				t.Fatalf("%s.%s: record without fields", path, f.QBO)
			}
			assertNoDuplicates(t, path+"."+f.QBO, f.Node)
		}
	}
}

func TestSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"CustomerRef": "customer_ref", "TotalAmt": "total_amt", "Id": "id", "APAccountRef": "ap_account_ref",
		"AllowOnlineACHPayment": "allow_online_ach_payment", "V4IDPseudonym": "v4_id_pseudonym", "Line1": "line1",
		"URI": "uri", "value": "value", "POStatus": "po_status", "Vendor1099": "vendor1099", "GSTIN": "gstin",
	} {
		if got := SnakeCase(in); got != want {
			t.Errorf("SnakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoadRejectsBrokenSchemas(t *testing.T) {
	types := "types:\n  Ref:\n    fields:\n      - {qbo: value, type: STRING}\n  Loop:\n    fields:\n      - {qbo: Self, ref: Loop}\n"
	cases := map[string]string{
		"unknown ref":           "entity: Thing\nsync: cdc\nfields:\n  - {qbo: A, ref: Nope}\n",
		"recursive":             "entity: Thing\nsync: cdc\nfields:\n  - {qbo: A, ref: Loop}\n",
		"already produced":      "entity: Thing\nsync: cdc\nfields:\n  - {qbo: TotalAmt, type: NUMERIC}\n  - {qbo: Total, bq: total_amt, type: NUMERIC}\n",
		"unknown type":          "entity: Thing\nsync: cdc\nfields:\n  - {qbo: A, type: FLOAT}\n",
		"sync must be":          "entity: Thing\nfields:\n  - {qbo: A, type: STRING}\n",
		"envelope":              "entity: Thing\nsync: full\nfields:\n  - {qbo: Id, type: STRING}\n",
		"collides":              "entity: Thing\nsync: full\nfields:\n  - {qbo: UpdatedAt, type: STRING}\n",
		"not both":              "entity: Thing\nsync: full\nfields:\n  - {qbo: A, type: STRING, ref: Ref}\n",
		"field typo not found":  "entity: Thing\nsync: full\nfields:\n  - {qbo: A, typo: STRING}\n",
		"include: unknown type": "entity: Thing\nsync: full\ninclude: [Missing]\nfields:\n  - {qbo: A, type: STRING}\n",
	}
	for want, yaml := range cases {
		_, err := Load(fstest.MapFS{"_types.yaml": {Data: []byte(types)}, "thing.yaml": {Data: []byte(yaml)}})
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: expected an error mentioning %q, got %v", want, want, err)
		}
	}
}
