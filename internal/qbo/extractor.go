package qbo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
)

const (
	// PageSize is the maximum MAXRESULTS the query endpoint accepts.
	PageSize = 1000
	// CDCCap is the most objects the CDC endpoint returns per entity. Hitting
	// it means the response may be truncated, so the entity is fully refreshed.
	CDCCap = 1000
	// CDCMaxAge is how far back we trust CDC (Intuit documents 30 days).
	CDCMaxAge = 29 * 24 * time.Hour
	// WatermarkOverlap is subtracted from the run start when saving the next
	// watermark; re-reading a few minutes is free because loads are idempotent.
	WatermarkOverlap = 10 * time.Minute
)

// EntityState is one row of the _sync_state table.
type EntityState struct {
	RealmID        string
	Entity         string
	HighWatermark  time.Time // next CDC changedSince
	LastFullSyncAt time.Time // last successful reconciled full refresh
	LastRunID      string
}

// CDCGroup is a set of entities that share a watermark and so one CDC call.
type CDCGroup struct {
	Since    time.Time
	Entities []string
}

// Plan says how each entity is extracted in this run.
type Plan struct {
	CDC  []CDCGroup
	Full []string
}

// PlanSync picks the extraction tier per entity:
//   - CDC when the entity supports it and has a watermark younger than 29 days;
//   - full refresh (paginated query + delete reconciliation) for the initial
//     backfill, stale watermarks, `sync: full` entities, a forced run, or when
//     the periodic full-refresh interval has elapsed (0 disables that).
func PlanSync(now time.Time, entities []*schema.Entity, state map[string]EntityState, forceFull bool, fullEvery time.Duration) Plan {
	var plan Plan
	groups := map[int64][]string{}
	for _, e := range entities {
		st := state[e.Name]
		switch {
		case forceFull, e.Sync != schema.SyncCDC, st.HighWatermark.IsZero(),
			now.Sub(st.HighWatermark) >= CDCMaxAge,
			fullEvery > 0 && now.Sub(st.LastFullSyncAt) >= fullEvery:
			plan.Full = append(plan.Full, e.Name)
		default:
			k := st.HighWatermark.Unix()
			groups[k] = append(groups[k], e.Name)
		}
	}
	for k, names := range groups {
		sort.Strings(names)
		plan.CDC = append(plan.CDC, CDCGroup{Since: time.Unix(k, 0).UTC(), Entities: names})
	}
	sort.Slice(plan.CDC, func(i, j int) bool { return plan.CDC[i].Since.Before(plan.CDC[j].Since) })
	sort.Strings(plan.Full)
	return plan
}

func whereClause(e *schema.Entity) string {
	if e.IncludeInactive {
		return " WHERE Active IN (true, false)"
	}
	return ""
}

// QuerySQL builds one page of a full extraction. Inactive records are included
// where the schema asks for it; ordering by Id keeps offset pagination stable.
func QuerySQL(e *schema.Entity, startPosition int) string {
	return fmt.Sprintf("SELECT * FROM %s%s ORDERBY Id STARTPOSITION %d MAXRESULTS %d", e.Name, whereClause(e), startPosition, PageSize)
}

// CountSQL counts the records QuerySQL pages through.
func CountSQL(e *schema.Entity) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", e.Name, whereClause(e))
}

// Fault is QBO's error envelope.
type Fault struct {
	Type  string `json:"type"`
	Error []struct {
		Message string `json:"Message"`
		Detail  string `json:"Detail"`
		Code    string `json:"code"`
	} `json:"Error"`
}

func (f *Fault) err() error {
	var parts []string
	for _, e := range f.Error {
		parts = append(parts, fmt.Sprintf("%s: %s (code %s)", e.Message, e.Detail, e.Code))
	}
	return fmt.Errorf("QBO %s: %s", f.Type, strings.Join(parts, "; "))
}

type queryResponse map[string]json.RawMessage

type envelope struct {
	Fault         *Fault        `json:"Fault"`
	QueryResponse queryResponse `json:"QueryResponse"`
	CDCResponse   []struct {
		QueryResponse []queryResponse `json:"QueryResponse"`
	} `json:"CDCResponse"`
}

func decodeEnvelope(body []byte) (*envelope, map[string]json.RawMessage, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, nil, fmt.Errorf("response is not a JSON object: %w", err)
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, nil, fmt.Errorf("unexpected response shape: %w", err)
	}
	if env.Fault != nil {
		return nil, nil, env.Fault.err()
	}
	return &env, top, nil
}

func records(qr queryResponse, into map[string][]json.RawMessage) error {
	for key, raw := range qr {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || trimmed[0] != '[' {
			continue // startPosition, maxResults, totalCount
		}
		var recs []json.RawMessage
		if err := json.Unmarshal(trimmed, &recs); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		into[key] = append(into[key], recs...)
	}
	return nil
}

// ParseRecords extracts entity records, keyed by entity name, from any raw
// response body: a /query page, a /cdc response, or a single-entity read
// (e.g. {"Invoice": {...}}). It is what both sync and replay use.
func ParseRecords(body []byte) (map[string][]json.RawMessage, error) {
	env, top, err := decodeEnvelope(body)
	if err != nil {
		return nil, err
	}
	out := map[string][]json.RawMessage{}
	switch {
	case env.CDCResponse != nil:
		for _, c := range env.CDCResponse {
			for _, qr := range c.QueryResponse {
				if err := records(qr, out); err != nil {
					return nil, err
				}
			}
		}
	case env.QueryResponse != nil:
		if err := records(env.QueryResponse, out); err != nil {
			return nil, err
		}
	default:
		for key, raw := range top {
			if t := bytes.TrimSpace(raw); len(t) > 0 && t[0] == '{' && key != "Header" {
				out[key] = append(out[key], raw)
			}
		}
	}
	return out, nil
}

// ParseCount reads totalCount from a COUNT(*) response.
func ParseCount(body []byte) (int, error) {
	env, _, err := decodeEnvelope(body)
	if err != nil {
		return 0, err
	}
	raw, ok := env.QueryResponse["totalCount"]
	if !ok {
		return 0, fmt.Errorf("response has no totalCount")
	}
	var n int
	return n, json.Unmarshal(raw, &n)
}

// Query runs one /query call.
func (c *Client) Query(ctx context.Context, sql string) (*Response, error) {
	return c.Get(ctx, "query", url.Values{"query": {sql}})
}

// CDC runs one change-data-capture call for several entities.
func (c *Client) CDC(ctx context.Context, entities []string, since time.Time) (*Response, error) {
	return c.Get(ctx, "cdc", url.Values{
		"entities":     {strings.Join(entities, ",")},
		"changedSince": {since.UTC().Format(time.RFC3339)},
	})
}

// Report fetches an accounting report summarised by month.
func (c *Client) Report(ctx context.Context, name, accountingMethod string, start, end time.Time) (*Response, error) {
	return c.Get(ctx, "reports/"+name, url.Values{
		"accounting_method":   {accountingMethod},
		"start_date":          {start.Format("2006-01-02")},
		"end_date":            {end.Format("2006-01-02")},
		"summarize_column_by": {"Month"},
	})
}

// CompanyName reads CompanyInfo.CompanyName (used when onboarding).
func (c *Client) CompanyName(ctx context.Context) (string, error) {
	resp, err := c.Get(ctx, "companyinfo/"+url.PathEscape(c.RealmID), nil)
	if err != nil {
		return "", err
	}
	var v struct {
		CompanyInfo struct {
			CompanyName string `json:"CompanyName"`
		} `json:"CompanyInfo"`
	}
	if err := json.Unmarshal(resp.Body, &v); err != nil {
		return "", err
	}
	return v.CompanyInfo.CompanyName, nil
}
