// Command qbo-bench replays a recorded QBO sandbox company through both
// BigQuery write methods (staging+MERGE vs Storage Write API CDC) so their
// cost and results can be compared. Scratch tool; not part of the app.
package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/bq"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/syncer"
)

//go:embed rec/*.json
var recordings embed.FS

func key(parts ...any) string {
	h := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return "rec/" + hex.EncodeToString(h[:8]) + ".json"
}

func recorded(k string) (*qbo.Response, error) {
	b, err := recordings.ReadFile(k)
	if err != nil {
		return nil, err
	}
	var r qbo.Response
	return &r, json.Unmarshal(b, &r)
}

// fakeAPI serves the recorded company. drop removes "Entity/Id" records from
// full scans (to exercise delete reconciliation); cdcIter > 0 synthesises a
// CDC response with edits and one deletion.
type fakeAPI struct {
	reg     *schema.Registry
	now     time.Time
	drop    map[string]bool
	cdcIter int
	applied int // CDC iterations already reflected in full scans
}

func (f *fakeAPI) Query(_ context.Context, sql string) (*qbo.Response, error) {
	r, err := recorded(key("q", sql))
	if err != nil || (len(f.drop) == 0 && f.applied == 0) {
		return r, err
	}
	var env map[string]json.RawMessage
	var qr map[string]json.RawMessage
	if err := json.Unmarshal(r.Body, &env); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(env["QueryResponse"], &qr); err != nil {
		return nil, err
	}
	for ent, raw := range qr {
		var recs []map[string]any
		if json.Unmarshal(raw, &recs) != nil {
			continue
		}
		gone := map[string]bool{}
		for k := 1; k <= f.applied; k++ {
			for _, c := range f.changes(ent, k, f.now.Add(time.Duration(k-f.applied-1)*time.Hour)) {
				if c["status"] == "Deleted" {
					gone[fmt.Sprint(c["Id"])] = true
					continue
				}
				for i, rec := range recs {
					if rec["Id"] == c["Id"] {
						recs[i] = c
					}
				}
			}
		}
		kept := recs[:0]
		for _, rec := range recs {
			id := fmt.Sprint(rec["Id"])
			if !f.drop[ent+"/"+id] && !gone[id] {
				kept = append(kept, rec)
			}
		}
		qr[ent], _ = json.Marshal(kept)
	}
	env["QueryResponse"], _ = json.Marshal(qr)
	body, _ := json.Marshal(env)
	return &qbo.Response{URL: r.URL, Status: r.Status, Body: body}, nil
}

func (f *fakeAPI) Report(_ context.Context, name, m string, _, _ time.Time) (*qbo.Response, error) {
	return recorded(key("rep", name, m))
}

func (f *fakeAPI) records(entity string) []map[string]any {
	ent, _ := f.reg.Entity(entity)
	r, err := recorded(key("q", qbo.QuerySQL(ent, 1)))
	if err != nil {
		panic(err)
	}
	var env struct{ QueryResponse map[string]json.RawMessage }
	_ = json.Unmarshal(r.Body, &env)
	var recs []map[string]any
	_ = json.Unmarshal(env.QueryResponse[entity], &recs)
	return recs
}

// changes returns CDC iteration k's records for an entity, stamped at now-5m:
// edits to 5 invoices and 2 customers, and one deleted bill.
func (f *fakeAPI) changes(entity string, k int, now time.Time) []map[string]any {
	ts := now.Add(-5 * time.Minute).UTC().Format(time.RFC3339)
	edit := func(field string, n int) []map[string]any {
		recs := f.records(entity)
		var out []map[string]any
		for i := 0; i < n; i++ {
			r := recs[(k*n+i)%len(recs)]
			r[field] = fmt.Sprintf("bench cdc run %d", k)
			md, _ := r["MetaData"].(map[string]any)
			md["LastUpdatedTime"] = ts
			out = append(out, r)
		}
		return out
	}
	switch entity {
	case "Invoice":
		return edit("PrivateNote", 5)
	case "Customer":
		return edit("Notes", 2)
	case "Bill":
		bills := f.records("Bill")
		return []map[string]any{{"Id": bills[k%len(bills)]["Id"], "status": "Deleted", "MetaData": map[string]any{"LastUpdatedTime": ts}}}
	}
	return nil
}

func (f *fakeAPI) CDC(_ context.Context, entities []string, _ time.Time) (*qbo.Response, error) {
	var qrs []map[string]any
	for _, e := range entities {
		if c := f.changes(e, f.cdcIter, f.now); len(c) > 0 {
			qrs = append(qrs, map[string]any{e: c})
		}
	}
	body, _ := json.Marshal(map[string]any{"CDCResponse": []any{map[string]any{"QueryResponse": qrs}}})
	return &qbo.Response{URL: "fake://cdc", Status: 200, Body: body}, nil
}

type event struct {
	Method  string `json:"method"`
	Phase   string `json:"phase"`
	Realm   string `json:"realm"`
	Elapsed int64  `json:"elapsed_ms"`
	Rows    int    `json:"rows"`
	Err     string `json:"err,omitempty"`
	At      string `json:"at"`
}

var outMu sync.Mutex

func emit(e event) {
	e.At = time.Now().UTC().Format(time.RFC3339)
	b, _ := json.Marshal(e)
	outMu.Lock()
	fmt.Println(string(b))
	outMu.Unlock()
}

func main() {
	project := flag.String("project", "delfina-gaia-dev", "")
	prefix := flag.String("dataset-prefix", "qbo_bench2", "")
	methods := flag.String("methods", "merge,storage_write", "")
	realms := flag.Int("realms", 10, "synthetic companies in the initial load")
	cdcRuns := flag.Int("cdc-runs", 3, "")
	wait := flag.Duration("wait", 16*time.Minute, "pause between runs (should exceed max_staleness)")
	stale := flag.Duration("staleness", 15*time.Minute, "")
	flag.Parse()
	ctx := context.Background()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg, err := schema.Default()
	if err != nil {
		panic(err)
	}
	t0 := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	realmID := func(i int) string { return fmt.Sprintf("9000000000%06d", i) }

	var wg sync.WaitGroup
	for _, method := range strings.Split(*methods, ",") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ds := *prefix + "_" + method
			l, err := bq.NewLoader(ctx, *project, ds, "", log.With("method", method))
			if err != nil {
				panic(err)
			}
			var wh syncer.Warehouse = l
			if method == "storage_write" {
				sl, err := bq.NewStreamLoader(ctx, l, *stale)
				if err != nil {
					panic(err)
				}
				defer sl.Close()
				wh = sl
			}
			run := func(phase, realm string, now time.Time, force bool, api *fakeAPI) {
				api.reg, api.now = reg, now
				s := &syncer.Syncer{RunID: syncer.NewRunID(now, 1), Registry: reg, Warehouse: wh, Archiver: qbo.NopArchiver{},
					SyncReports: true, FullEvery: 7 * 24 * time.Hour, ForceFull: force, Log: log.With("method", method),
					Now: func() time.Time { return now }}
				start := time.Now()
				res, err := s.SyncRealm(ctx, api, realm)
				e := event{Method: method, Phase: phase, Realm: realm, Elapsed: time.Since(start).Milliseconds()}
				if res != nil {
					for _, n := range res.Entities {
						e.Rows += n
					}
				}
				if err != nil {
					e.Err = err.Error()
				}
				emit(e)
			}
			emit(event{Method: method, Phase: "start"})
			for i := 1; i <= *realms; i++ {
				run("initial_full", realmID(i), t0, true, &fakeAPI{})
			}
			emit(event{Method: method, Phase: "wait"})
			time.Sleep(*wait)
			for k := 1; k <= *cdcRuns; k++ {
				run(fmt.Sprintf("cdc_%d", k), realmID(1), t0.Add(time.Duration(k)*time.Hour), false, &fakeAPI{cdcIter: k})
				emit(event{Method: method, Phase: "wait"})
				time.Sleep(*wait)
			}
			// Reconciling full refresh with three records gone from QBO.
			drop := map[string]bool{"Account/1": true, "Account/2": true, "Customer/1": true}
			run("reconcile_full", realmID(1), t0.Add(time.Duration(*cdcRuns+1)*time.Hour), true, &fakeAPI{drop: drop, applied: *cdcRuns})
			emit(event{Method: method, Phase: "wait"})
			time.Sleep(*wait)
			emit(event{Method: method, Phase: "done"})
		}()
	}
	wg.Wait()
}
