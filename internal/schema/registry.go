// Package schema compiles the declarative YAML files in /schemas into typed
// field trees and maps raw QBO JSON onto them.
package schema

import (
	"bytes"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"

	"github.com/DelfinaCare/qbo-to-bigquery/schemas"
)

// Kind is a column type in the schema DSL.
type Kind string

const (
	String    Kind = "STRING"
	Numeric   Kind = "NUMERIC"
	Date      Kind = "DATE"
	Timestamp Kind = "TIMESTAMP"
	Bool      Kind = "BOOL"
	Int64     Kind = "INT64"
	// JSON stores an opaque value as a JSON-encoded STRING column.
	JSON Kind = "JSON"
	// Record is a nested STRUCT; produced by `ref:`, never written in YAML.
	Record Kind = "RECORD"
)

var scalarKinds = map[Kind]bool{String: true, Numeric: true, Date: true, Timestamp: true, Bool: true, Int64: true, JSON: true}

// Sync modes.
const (
	SyncCDC  = "cdc"  // incremental via the change-data-capture endpoint
	SyncFull = "full" // full refresh on every run
)

// Field is one compiled column (or nested struct member).
type Field struct {
	QBO         string // JSON key in the QBO payload
	BQ          string // BigQuery column name
	Kind        Kind
	Repeated    bool
	Required    bool
	Description string
	Node        *Node // set when Kind == Record
}

// Node is an ordered set of fields with a lookup by QBO key.
type Node struct {
	Fields []*Field
	byQBO  map[string]*Field
}

// Entity is one QBO entity mirrored to one BigQuery table.
type Entity struct {
	Name        string // QBO entity name, e.g. "Invoice"
	Table       string // BigQuery table name, e.g. "invoice"
	Sync        string // SyncCDC or SyncFull
	Description string
	// IncludeInactive adds `WHERE Active IN (true, false)` to full queries.
	// QBO hides inactive name-list records (accounts, customers, ...) by
	// default; only set it for entities whose Active property is queryable.
	IncludeInactive bool
	Node            *Node // payload fields (without the envelope)
}

// Envelope columns are prepended to every entity table.
var Envelope = []*Field{
	{BQ: "realm_id", Kind: String, Required: true, Description: "QBO company (realm) id."},
	{BQ: "id", Kind: String, Required: true, Description: "QBO entity Id."},
	{BQ: "sync_token", Kind: String, Description: "QBO SyncToken (version counter)."},
	{BQ: "created_at", Kind: Timestamp, Description: "MetaData.CreateTime."},
	{BQ: "updated_at", Kind: Timestamp, Required: true, Description: "MetaData.LastUpdatedTime (falls back to _extracted_at)."},
	{BQ: "is_deleted", Kind: Bool, Required: true, Description: "True once QBO reports the record deleted."},
	{BQ: "_extracted_at", Kind: Timestamp, Required: true, Description: "When this version was fetched from QBO."},
	{BQ: "_raw_gcs_uri", Kind: String, Description: "Archived raw API response this row came from."},
}

// envelopeKeys are top-level QBO keys consumed by the envelope (or noise).
var envelopeKeys = map[string]bool{"Id": true, "SyncToken": true, "MetaData": true, "status": true, "domain": true, "sparse": true}

// Columns returns the full top-level column list: envelope + payload.
func (e *Entity) Columns() []*Field {
	return append(append([]*Field{}, Envelope...), e.Node.Fields...)
}

// Registry holds every compiled entity.
type Registry struct {
	entities map[string]*Entity
}

// Entities returns all entities sorted by name.
func (r *Registry) Entities() []*Entity {
	out := make([]*Entity, 0, len(r.entities))
	for _, e := range r.entities {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Entity looks an entity up by QBO name or table name (case-insensitive).
func (r *Registry) Entity(name string) (*Entity, bool) {
	for _, e := range r.entities {
		if strings.EqualFold(e.Name, name) || strings.EqualFold(e.Table, name) {
			return e, true
		}
	}
	return nil, false
}

type rawField struct {
	QBO         string `yaml:"qbo"`
	BQ          string `yaml:"bq"`
	Type        string `yaml:"type"`
	Ref         string `yaml:"ref"`
	Repeated    bool   `yaml:"repeated"`
	Description string `yaml:"description"`
}

type rawType struct {
	Description string     `yaml:"description"`
	Fields      []rawField `yaml:"fields"`
}

type rawTypes struct {
	Types map[string]rawType `yaml:"types"`
}

type rawEntity struct {
	Entity          string     `yaml:"entity"`
	Table           string     `yaml:"table"`
	Sync            string     `yaml:"sync"`
	Description     string     `yaml:"description"`
	IncludeInactive bool       `yaml:"include_inactive"`
	Include         []string   `yaml:"include"`
	Fields          []rawField `yaml:"fields"`
}

// TypesFile is the name of the shared type-template file.
const TypesFile = "_types.yaml"

// Default loads the schemas embedded in the binary.
func Default() (*Registry, error) { return Load(schemas.FS) }

// MustDefault is Default but panics; the embedded schemas are covered by tests.
func MustDefault() *Registry {
	r, err := Default()
	if err != nil {
		panic(err)
	}
	return r
}

func decodeStrict(data []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	return dec.Decode(v)
}

// Load compiles every *.yaml file at the root of fsys.
func Load(fsys fs.FS) (*Registry, error) {
	c := &compiler{raw: map[string]rawType{}, done: map[string]*Node{}, busy: map[string]bool{}}
	if data, err := fs.ReadFile(fsys, TypesFile); err == nil {
		var rt rawTypes
		if err := decodeStrict(data, &rt); err != nil {
			return nil, fmt.Errorf("%s: %w", TypesFile, err)
		}
		c.raw = rt.Types
	}
	files, err := fs.Glob(fsys, "*.yaml")
	if err != nil {
		return nil, err
	}
	reg := &Registry{entities: map[string]*Entity{}}
	tables := map[string]string{}
	for _, f := range files {
		if f == TypesFile {
			continue
		}
		data, err := fs.ReadFile(fsys, f)
		if err != nil {
			return nil, err
		}
		var re rawEntity
		if err := decodeStrict(data, &re); err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		ent, err := c.entity(re)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		if _, dup := reg.entities[ent.Name]; dup {
			return nil, fmt.Errorf("%s: entity %s defined twice", f, ent.Name)
		}
		if other, dup := tables[ent.Table]; dup {
			return nil, fmt.Errorf("%s: table %s already used by %s", f, ent.Table, other)
		}
		tables[ent.Table] = ent.Name
		reg.entities[ent.Name] = ent
	}
	if len(reg.entities) == 0 {
		return nil, fmt.Errorf("no entity schemas found")
	}
	return reg, nil
}

type compiler struct {
	raw  map[string]rawType
	done map[string]*Node
	busy map[string]bool
}

var (
	entityNameRe = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*$`)
	columnNameRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
)

func (c *compiler) entity(re rawEntity) (*Entity, error) {
	if !entityNameRe.MatchString(re.Entity) {
		return nil, fmt.Errorf("entity %q: must be a PascalCase QBO entity name", re.Entity)
	}
	ent := &Entity{Name: re.Entity, Table: re.Table, Sync: re.Sync, Description: re.Description, IncludeInactive: re.IncludeInactive}
	if ent.Table == "" {
		ent.Table = SnakeCase(re.Entity)
	}
	if !columnNameRe.MatchString(ent.Table) || strings.HasPrefix(ent.Table, "_") {
		return nil, fmt.Errorf("invalid table name %q", ent.Table)
	}
	if ent.Sync != SyncCDC && ent.Sync != SyncFull {
		return nil, fmt.Errorf("sync must be %q or %q, got %q", SyncCDC, SyncFull, re.Sync)
	}
	var fields []rawField
	for _, inc := range re.Include {
		t, ok := c.raw[inc]
		if !ok {
			return nil, fmt.Errorf("include: unknown type %q", inc)
		}
		fields = append(fields, t.Fields...)
	}
	fields = append(fields, re.Fields...)
	node, err := c.node(re.Entity, fields)
	if err != nil {
		return nil, err
	}
	for _, f := range node.Fields {
		if envelopeKeys[f.QBO] {
			return nil, fmt.Errorf("%s: %s is handled by the standard envelope; remove it", re.Entity, f.QBO)
		}
		for _, env := range Envelope {
			if env.BQ == f.BQ {
				return nil, fmt.Errorf("%s.%s: column %q collides with an envelope column", re.Entity, f.QBO, f.BQ)
			}
		}
	}
	ent.Node = node
	return ent, nil
}

func (c *compiler) typ(name string) (*Node, error) {
	if n, ok := c.done[name]; ok {
		return n, nil
	}
	rt, ok := c.raw[name]
	if !ok {
		return nil, fmt.Errorf("unknown ref %q", name)
	}
	if c.busy[name] {
		return nil, fmt.Errorf("type %q is recursive; BigQuery structs cannot be", name)
	}
	c.busy[name] = true
	defer delete(c.busy, name)
	n, err := c.node(name, rt.Fields)
	if err != nil {
		return nil, err
	}
	c.done[name] = n
	return n, nil
}

func (c *compiler) node(path string, raws []rawField) (*Node, error) {
	if len(raws) == 0 {
		return nil, fmt.Errorf("%s: no fields", path)
	}
	n := &Node{byQBO: map[string]*Field{}}
	seenBQ := map[string]string{}
	for _, rf := range raws {
		if rf.QBO == "" {
			return nil, fmt.Errorf("%s: field without `qbo`", path)
		}
		fp := path + "." + rf.QBO
		f := &Field{QBO: rf.QBO, BQ: rf.BQ, Repeated: rf.Repeated, Description: rf.Description}
		if f.BQ == "" {
			f.BQ = SnakeCase(rf.QBO)
		}
		if !columnNameRe.MatchString(f.BQ) {
			return nil, fmt.Errorf("%s: invalid BigQuery column name %q", fp, f.BQ)
		}
		switch {
		case rf.Type != "" && rf.Ref != "":
			return nil, fmt.Errorf("%s: set either `type` or `ref`, not both", fp)
		case rf.Ref != "":
			sub, err := c.typ(rf.Ref)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", fp, err)
			}
			f.Kind, f.Node = Record, sub
		case scalarKinds[Kind(rf.Type)]:
			f.Kind = Kind(rf.Type)
		default:
			return nil, fmt.Errorf("%s: unknown type %q", fp, rf.Type)
		}
		if _, dup := n.byQBO[f.QBO]; dup {
			return nil, fmt.Errorf("%s: duplicate qbo key", fp)
		}
		if other, dup := seenBQ[f.BQ]; dup {
			return nil, fmt.Errorf("%s: BigQuery column %q already produced by %s", fp, f.BQ, other)
		}
		seenBQ[f.BQ] = f.QBO
		n.byQBO[f.QBO] = f
		n.Fields = append(n.Fields, f)
	}
	return n, nil
}

// SnakeCase deterministically converts QBO PascalCase keys to snake_case:
// CustomerRef -> customer_ref, APAccountRef -> ap_account_ref, Line1 -> line1.
func SnakeCase(s string) string {
	rs := []rune(s)
	var b strings.Builder
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 {
			prev := rs[i-1]
			nextLower := i+1 < len(rs) && unicode.IsLower(rs[i+1])
			if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
				b.WriteByte('_')
			}
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
