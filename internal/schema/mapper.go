package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// IngestError is one row of the _ingest_errors table: a single field (or
// record) that could not be coerced to its declared type.
type IngestError struct {
	RunID         string    `json:"run_id"`
	RealmID       string    `json:"realm_id"`
	Entity        string    `json:"entity"`
	RecordID      string    `json:"record_id"`
	FieldPath     string    `json:"field_path"`
	ExpectedType  string    `json:"expected_type"`
	ObservedValue string    `json:"observed_value"`
	ErrorMessage  string    `json:"error_message"`
	GCSURI        string    `json:"gcs_uri,omitempty"`
	RawRecordJSON string    `json:"raw_record_json"`
	FailedAt      time.Time `json:"failed_at"`
}

// MapContext carries per-response metadata into the mapper.
type MapContext struct {
	RunID       string
	RealmID     string
	GCSURI      string
	ExtractedAt time.Time
	// OnUnknown is called for every payload key missing from the schema,
	// e.g. path "Invoice.Line[].NewField". May be nil.
	OnUnknown func(entity, path, gcsURI string)
}

// Mapped is the outcome of mapping one QBO record.
type Mapped struct {
	ID      string
	Deleted bool
	// Row is the snake_case BigQuery row; nil if the record had to be dropped.
	Row    map[string]any
	Errors []IngestError
}

// TimestampLayout is the canonical UTC timestamp format written to NDJSON.
const TimestampLayout = "2006-01-02T15:04:05.999999Z"

// FormatTimestamp renders t in the canonical format.
func FormatTimestamp(t time.Time) string { return t.UTC().Format(TimestampLayout) }

// Map converts one raw QBO JSON object to a typed row. Field-level coercion
// failures null the field and are reported in Mapped.Errors; they never fail
// the record. The returned error is only for a payload that is not a JSON object.
func (e *Entity) Map(raw []byte, mc *MapContext) (*Mapped, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return nil, fmt.Errorf("%s: record is not a JSON object: %w", e.Name, err)
	}
	m := &mapper{ent: e, mc: mc, raw: raw}
	out := &Mapped{}
	if id, ok := scalarString(obj["Id"]); ok && id != "" {
		out.ID = id
	} else {
		m.id = ""
		m.fail(e.Name+".Id", "STRING", obj["Id"], "record has no Id; dropped")
		out.Errors = m.errs
		return out, nil
	}
	m.id = out.ID
	extracted := FormatTimestamp(mc.ExtractedAt)
	row := map[string]any{
		"realm_id":      mc.RealmID,
		"id":            out.ID,
		"is_deleted":    false,
		"updated_at":    extracted,
		"_extracted_at": extracted,
	}
	if mc.GCSURI != "" {
		row["_raw_gcs_uri"] = mc.GCSURI
	}
	if st, ok := scalarString(obj["SyncToken"]); ok {
		row["sync_token"] = st
	}
	if md, ok := obj["MetaData"].(map[string]any); ok {
		for key, col := range map[string]string{"CreateTime": "created_at", "LastUpdatedTime": "updated_at"} {
			if v, present := md[key]; present && v != nil {
				if ts, err := coerceTimestamp(v); err == nil {
					row[col] = ts
				} else {
					m.fail(e.Name+".MetaData."+key, "TIMESTAMP", v, err.Error())
				}
			}
		}
	}
	if s, _ := obj["status"].(string); strings.EqualFold(s, "Deleted") {
		// Tombstone: QBO only sends Id + MetaData for deleted records.
		out.Deleted = true
		row["is_deleted"] = true
		out.Row, out.Errors = row, m.errs
		return out, nil
	}
	m.mapNode(e.Node, obj, e.Name, row, true)
	out.Row, out.Errors = row, m.errs
	return out, nil
}

type mapper struct {
	ent  *Entity
	mc   *MapContext
	raw  []byte
	id   string
	errs []IngestError
}

func (m *mapper) fail(path, expected string, observed any, msg string) {
	obs, _ := json.Marshal(observed)
	if len(obs) > 1000 {
		obs = append(obs[:1000], "..."...)
	}
	m.errs = append(m.errs, IngestError{
		RunID: m.mc.RunID, RealmID: m.mc.RealmID, Entity: m.ent.Name, RecordID: m.id,
		FieldPath: path, ExpectedType: expected, ObservedValue: string(obs), ErrorMessage: msg,
		GCSURI: m.mc.GCSURI, RawRecordJSON: string(m.raw), FailedAt: m.mc.ExtractedAt.UTC(),
	})
}

func (m *mapper) mapNode(n *Node, obj map[string]any, path string, out map[string]any, top bool) {
	for key, v := range obj {
		if top && envelopeKeys[key] {
			continue
		}
		fp := path + "." + key
		f, ok := n.byQBO[key]
		if !ok {
			if m.mc.OnUnknown != nil {
				m.mc.OnUnknown(m.ent.Name, fp, m.mc.GCSURI)
			}
			continue
		}
		if v == nil {
			continue
		}
		if !f.Repeated {
			if cv, ok := m.value(f, v, fp); ok {
				out[f.BQ] = cv
			}
			continue
		}
		arr, isArr := v.([]any)
		if !isArr {
			arr = []any{v} // QBO occasionally collapses single-element lists
		}
		vals := make([]any, 0, len(arr))
		for _, el := range arr {
			if el == nil {
				continue
			}
			if cv, ok := m.value(f, el, fp+"[]"); ok {
				vals = append(vals, cv)
			}
		}
		out[f.BQ] = vals
	}
}

func (m *mapper) value(f *Field, v any, path string) (any, bool) {
	if f.Kind == Record {
		obj, ok := v.(map[string]any)
		if !ok {
			m.fail(path, "RECORD", v, "expected a JSON object")
			return nil, false
		}
		sub := map[string]any{}
		m.mapNode(f.Node, obj, path, sub, false)
		return sub, true
	}
	cv, err := Coerce(f.Kind, v)
	if err != nil {
		m.fail(path, string(f.Kind), v, err.Error())
		return nil, false
	}
	return cv, true
}

func scalarString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case bool:
		return strconv.FormatBool(x), true
	}
	return "", false
}

// Coerce validates v (decoded with UseNumber) against a scalar kind and
// returns the value to write to NDJSON.
func Coerce(k Kind, v any) (any, error) {
	switch k {
	case String:
		if s, ok := scalarString(v); ok {
			return s, nil
		}
		return nil, fmt.Errorf("expected a string")
	case Numeric:
		s, ok := scalarString(v)
		if _, isBool := v.(bool); !ok || isBool {
			return nil, fmt.Errorf("expected a number")
		}
		return coerceNumeric(s)
	case Int64:
		s, ok := scalarString(v)
		if _, isBool := v.(bool); !ok || isBool {
			return nil, fmt.Errorf("expected an integer")
		}
		if i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return i, nil
		}
		if r, err := parseDecimal(s); err == nil && r.IsInt() && r.Num().IsInt64() {
			return r.Num().Int64(), nil
		}
		return nil, fmt.Errorf("not a 64-bit integer")
	case Bool:
		switch x := v.(type) {
		case bool:
			return x, nil
		case string:
			switch strings.ToLower(strings.TrimSpace(x)) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			}
		}
		return nil, fmt.Errorf("expected true or false")
	case Date:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected a YYYY-MM-DD string")
		}
		return coerceDate(s)
	case Timestamp:
		return coerceTimestamp(v)
	case JSON:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	}
	return nil, fmt.Errorf("unsupported type %s", k)
}

var (
	decimalRe = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,3})?$`)
	// numericLimit is 10^29: BigQuery NUMERIC holds 29 integer digits.
	numericLimit = new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(29), nil))
	tzSuffixRe   = regexp.MustCompile(`^(Z|[+-]\d{2}:?\d{2})$`)
)

func parseDecimal(s string) (*big.Rat, error) {
	s = strings.TrimSpace(s)
	if len(s) > 80 || !decimalRe.MatchString(s) {
		return nil, fmt.Errorf("not a decimal number")
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("not a decimal number")
	}
	return r, nil
}

// coerceNumeric returns a plain decimal string rounded to 9 fractional digits
// (BigQuery NUMERIC scale), never passing through float64.
func coerceNumeric(s string) (string, error) {
	r, err := parseDecimal(s)
	if err != nil {
		return "", err
	}
	if new(big.Rat).Abs(r).Cmp(numericLimit) >= 0 {
		return "", fmt.Errorf("out of range for NUMERIC")
	}
	out := r.FloatString(9)
	out = strings.TrimRight(out, "0")
	out = strings.TrimSuffix(out, ".")
	if out == "-0" || out == "" {
		out = "0"
	}
	return out, nil
}

func coerceDate(s string) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) >= 10 {
		if _, err := time.Parse("2006-01-02", s[:10]); err == nil {
			// QBO sometimes appends a zone to dates ("2024-01-31-08:00"); the
			// calendar date is what matters.
			if rest := s[10:]; rest == "" || tzSuffixRe.MatchString(rest) {
				return s[:10], nil
			}
		}
	}
	return "", fmt.Errorf("expected YYYY-MM-DD")
}

var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999Z0700",
	"2006-01-02T15:04:05.999999999", // no zone: assume UTC
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02",
}

func coerceTimestamp(v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("expected an ISO-8601 timestamp string")
	}
	s = strings.TrimSpace(s)
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			if y := t.UTC().Year(); y < 1 || y > 9999 {
				break
			}
			return FormatTimestamp(t), nil
		}
	}
	return "", fmt.Errorf("unrecognised timestamp format")
}
