// Command qbo-bq mirrors QuickBooks Online data into BigQuery.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata" // the scratch image has no /usr/share/zoneinfo

	"cloud.google.com/go/storage"

	"github.com/DelfinaCare/qbo-to-bigquery/internal/bq"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/config"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/db"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/qbo"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/schema"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/syncer"
	"github.com/DelfinaCare/qbo-to-bigquery/internal/web"
)

const usage = `qbo-bq mirrors QuickBooks Online data into BigQuery.

Usage: qbo-bq <command> [flags]

Commands:
  serve         Run the admin web UI (connect / pause / disconnect companies)
  sync          Sync every active company once (run this on a schedule)
  migrate       Create the PostgreSQL tables and exit
  replay        Re-map an archived raw response with the current schemas
  import-token  Register a company from an existing refresh token (no browser)
  schema        Print the compiled table list
  version       Print the release version

Run "qbo-bq <command> -h" for flags. Configuration comes from environment
variables; see README.md.
`

// version is set at build time with -ldflags "-X main.version=v1.2.3". SBOM
// tools (syft, grype) read it from the binary to name the main module.
var version = "dev"

func main() {
	log := newLogger()
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cmds := map[string]func(context.Context, *slog.Logger, []string) error{
		"serve": serve, "sync": runSync, "migrate": migrate, "replay": replay, "import-token": importToken, "schema": printSchema,
	}
	if os.Args[1] == "version" || os.Args[1] == "--version" {
		fmt.Println(version)
		return
	}
	cmd, ok := cmds[os.Args[1]]
	if !ok {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := cmd(ctx, log, os.Args[2:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(2)
		}
		log.Error("command failed", "command", os.Args[1], "error", err.Error())
		os.Exit(1)
	}
}

// newLogger writes JSON lines shaped for Cloud Logging (severity/message).
func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.LevelKey:
				a.Key = "severity"
				if a.Value.String() == "WARN" {
					a.Value = slog.StringValue("WARNING")
				}
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	}))
}

func openStore(ctx context.Context, cfg *config.Config) (*db.Store, error) {
	c, err := db.NewCipher(cfg.TokenEncryptionKey)
	if err != nil {
		return nil, err
	}
	store, err := db.Open(ctx, cfg.DatabaseURL(), c, cfg.DBIAMAuth)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		store.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return store, nil
}

func migrate(ctx context.Context, log *slog.Logger, _ []string) error {
	cfg, err := config.Load(os.Getenv, config.NeedDB)
	if err != nil {
		return err
	}
	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	store.Close()
	log.Info("migrations applied")
	return nil
}

// staticToken serves a token we already hold (right after an OAuth exchange).
type staticToken string

func (t staticToken) AccessToken(context.Context, bool) (string, error) { return string(t), nil }

func serve(ctx context.Context, log *slog.Logger, _ []string) error {
	cfg, err := config.Load(os.Getenv, config.NeedDB, config.NeedQBO, config.NeedWeb)
	if err != nil {
		return err
	}
	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	srv := &web.Server{
		Store: store, OAuth: &qbo.OAuth{Creds: cfg.Credentials}, BasePath: cfg.WebPathPrefix,
		RedirectURI: cfg.QBORedirectURI, Environments: cfg.Environments(), Log: log,
		CompanyName: func(ctx context.Context, env, realmID, accessToken string) (string, error) {
			return qbo.NewClient(env, realmID, cfg.QBOMinorVersion, staticToken(accessToken)).CompanyName(ctx)
		},
	}
	hs := &http.Server{Addr: ":" + cfg.Port, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	log.Info("admin UI listening", "port", cfg.Port, "base_path", cfg.WebPathPrefix)
	if err := hs.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// dbTokens caches an access token in memory and refreshes it through the
// row-locked rotation guardrail shortly before it expires.
type dbTokens struct {
	store   *db.Store
	oauth   *qbo.OAuth
	realmID string
	mu      sync.Mutex
	tok     *qbo.Token
}

func (t *dbTokens) AccessToken(ctx context.Context, force bool) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !force && t.tok != nil && time.Until(t.tok.ExpiresAt) > 5*time.Minute {
		return t.tok.AccessToken, nil
	}
	tok, err := t.store.RefreshAccessToken(ctx, t.realmID, t.oauth.Refresh)
	if err != nil {
		return "", err
	}
	t.tok = tok
	return tok.AccessToken, nil
}

func newRunID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return syncer.NewRunID(time.Now(), binary.BigEndian.Uint32(b[:]))
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// newWarehouse returns the BigQuery warehouse for BQ_WRITE_METHOD. Close it
// with closeWarehouse.
func newWarehouse(ctx context.Context, cfg *config.Config, log *slog.Logger) (syncer.Warehouse, error) {
	loader, err := bq.NewLoader(ctx, cfg.GCPProjectID, cfg.BQDataset, cfg.BQLocation, log)
	if err != nil {
		return nil, fmt.Errorf("bigquery client: %w", err)
	}
	if cfg.BQWriteMethod != "storage_write" {
		return loader, nil
	}
	sl, err := bq.NewStreamLoader(ctx, loader, cfg.BQMaxStaleness)
	if err != nil {
		return nil, fmt.Errorf("bigquery storage write client: %w", err)
	}
	return sl, nil
}

func closeWarehouse(w syncer.Warehouse, log *slog.Logger) {
	if c, ok := w.(interface{ Close() error }); ok {
		if err := c.Close(); err != nil {
			log.Warn("closing BigQuery write streams", "error", err.Error())
		}
	}
}

// destinations builds the archive + warehouse pair: GCS + BigQuery, or local
// files when localOut is set.
func destinations(ctx context.Context, cfg *config.Config, log *slog.Logger, localOut string) (qbo.Archiver, syncer.Warehouse, error) {
	if localOut != "" {
		return &qbo.LocalArchiver{Dir: filepath.Join(localOut, "raw")}, &syncer.LocalWarehouse{Dir: localOut}, nil
	}
	warehouse, err := newWarehouse(ctx, cfg, log)
	if err != nil {
		return nil, nil, err
	}
	if cfg.GCSBucket == "" {
		log.Info("RAW_PAYLOAD_GCS_BUCKET is not set; raw responses will not be archived")
		return qbo.NopArchiver{}, warehouse, nil
	}
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("gcs client: %w", err)
	}
	return &qbo.GCSArchiver{Client: gcs, Bucket: cfg.GCSBucket, Prefix: cfg.GCSPrefix}, warehouse, nil
}

func runSync(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("sync", flag.ContinueOnError)
	realm := fs.String("realm", "", "only sync this realm id (default: every active installation)")
	entities := fs.String("entities", "", "comma-separated entity names to sync (default: all; skips reports)")
	full := fs.Bool("full", false, "force a full refresh instead of CDC")
	localOut := fs.String("local-out", "", "write raw payloads and NDJSON rows to this directory instead of GCS/BigQuery")
	if err := fs.Parse(args); err != nil {
		return err
	}
	need := []config.Feature{config.NeedDB, config.NeedQBO}
	if *localOut == "" {
		need = append(need, config.NeedGCP)
	}
	cfg, err := config.Load(os.Getenv, need...)
	if err != nil {
		return err
	}
	reg, err := schema.Default()
	if err != nil {
		return err
	}
	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	archiver, warehouse, err := destinations(ctx, cfg, log, *localOut)
	if err != nil {
		return err
	}
	defer closeWarehouse(warehouse, log)
	all, err := store.List(ctx)
	if err != nil {
		return err
	}
	s := &syncer.Syncer{
		RunID: newRunID(), Registry: reg, Warehouse: warehouse, Archiver: archiver,
		SyncReports: cfg.SyncReports, FullEvery: cfg.FullRefreshInterval, ForceFull: *full,
		Entities: splitList(*entities), Log: log,
	}
	oauth := &qbo.OAuth{Creds: cfg.Credentials}
	log.Info("sync run starting", "run_id", s.RunID, "installations", len(all))
	failed, synced := 0, 0
	for _, inst := range all {
		if *realm != "" && inst.RealmID != *realm {
			continue
		}
		if inst.Status != db.StatusActive {
			log.Info("skipping installation", "realm_id", inst.RealmID, "status", inst.Status)
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tokens := &dbTokens{store: store, oauth: oauth, realmID: inst.RealmID}
		client := qbo.NewClient(inst.Environment, inst.RealmID, cfg.QBOMinorVersion, tokens)
		started := time.Now()
		var res *syncer.Result
		// Fail fast (and only once) if the stored refresh token is dead.
		_, err := tokens.AccessToken(ctx, false)
		if err == nil {
			res, err = s.SyncRealm(ctx, client, inst.RealmID)
		}
		status, msg := "success", ""
		switch {
		case errors.Is(err, db.ErrNeedsReauth):
			// RefreshAccessToken already flagged the row; other companies carry on.
			failed++
			log.Error("installation needs re-authorisation; reconnect it in the admin UI", "realm_id", inst.RealmID, "company", inst.CompanyName)
			continue
		case err != nil:
			failed++
			status, msg = "error", err.Error()
			if len(msg) > 4000 {
				msg = msg[:4000]
			}
		default:
			synced++
		}
		attrs := []any{"realm_id", inst.RealmID, "company", inst.CompanyName, "status", status, "seconds", int(time.Since(started).Seconds()), "run_id", s.RunID}
		if res != nil {
			attrs = append(attrs, "entities_synced", len(res.Entities), "entities_failed", len(res.Failed), "ingest_errors", res.IngestErrors)
		}
		if err != nil {
			log.Error("realm sync finished with errors", append(attrs, "error", msg)...)
		} else {
			log.Info("realm sync finished", attrs...)
		}
		if rerr := store.RecordSync(context.WithoutCancel(ctx), inst.RealmID, status, msg); rerr != nil {
			log.Error("could not record sync status", "realm_id", inst.RealmID, "error", rerr.Error())
		}
	}
	log.Info("sync run finished", "run_id", s.RunID, "synced", synced, "failed", failed)
	if failed > 0 {
		return fmt.Errorf("%d installation(s) failed", failed)
	}
	return nil
}

func replay(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	uri := fs.String("gcs-uri", "", "archived response to replay: gs://bucket/object or a local file (required)")
	entity := fs.String("entity", "", "only replay this entity (default: every entity found in the payload)")
	realm := fs.String("realm", "", "realm id (default: parsed from the realm_id=… path segment)")
	dry := fs.Bool("dry-run", false, "map and report only; do not write to BigQuery")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *uri == "" {
		return errors.New("--gcs-uri is required")
	}
	reg, err := schema.Default()
	if err != nil {
		return err
	}
	isGCS := strings.HasPrefix(*uri, "gs://")
	realmID := *realm
	if realmID == "" {
		realmID = qbo.RealmFromURI(*uri)
	}
	if realmID == "" && !*dry {
		return errors.New("--realm is required (the URI has no realm_id=… segment)")
	}
	s := &syncer.Syncer{RunID: "replay_" + newRunID(), Registry: reg, Entities: splitList(*entity), Log: log}
	if isGCS {
		gcs, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}
		s.Archiver = &qbo.GCSArchiver{Client: gcs}
	} else {
		s.Archiver = &qbo.LocalArchiver{}
	}
	if !*dry {
		cfg, err := config.Load(os.Getenv, config.NeedGCP)
		if err != nil {
			return err
		}
		if s.Warehouse, err = newWarehouse(ctx, cfg, log); err != nil {
			return err
		}
		defer closeWarehouse(s.Warehouse, log)
	}
	res, err := s.Replay(ctx, *uri, realmID, *dry)
	if res != nil {
		for _, e := range res.IngestErrors {
			log.Error("field failed type coercion", "event", "ingest_error", "entity", e.Entity, "record_id", e.RecordID,
				"field_path", e.FieldPath, "expected_type", e.ExpectedType, "observed_value", e.ObservedValue, "error", e.ErrorMessage)
		}
		log.Info("replay finished", "uri", *uri, "dry_run", *dry, "records", res.Records, "ingest_errors", len(res.IngestErrors))
	}
	if err != nil {
		return err
	}
	if len(res.IngestErrors) > 0 {
		return fmt.Errorf("%d ingest error(s) remain", len(res.IngestErrors))
	}
	return nil
}

func importToken(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("import-token", flag.ContinueOnError)
	realm := fs.String("realm", "", "QBO realm (company) id (required)")
	env := fs.String("env", "production", "production or sandbox")
	name := fs.String("company-name", "", "display name (default: fetched from QBO)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	refresh := strings.TrimSpace(os.Getenv("QBO_REFRESH_TOKEN"))
	if *realm == "" || refresh == "" || (*env != "production" && *env != "sandbox") {
		return errors.New("usage: QBO_REFRESH_TOKEN=… qbo-bq import-token --realm ID [--env production|sandbox]")
	}
	cfg, err := config.Load(os.Getenv, config.NeedDB, config.NeedQBO)
	if err != nil {
		return err
	}
	store, err := openStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	// Store the token first, then refresh through the guardrail so a rotation
	// is never lost.
	if err := store.Upsert(ctx, *realm, *name, *env, "import-token", &qbo.Token{RefreshToken: refresh}); err != nil {
		return err
	}
	tokens := &dbTokens{store: store, oauth: &qbo.OAuth{Creds: cfg.Credentials}, realmID: *realm}
	access, err := tokens.AccessToken(ctx, true)
	if err != nil {
		return fmt.Errorf("the refresh token was stored but could not be used: %w", err)
	}
	if *name == "" {
		company, err := qbo.NewClient(*env, *realm, cfg.QBOMinorVersion, staticToken(access)).CompanyName(ctx)
		if err != nil {
			return err
		}
		if err := store.SetCompanyName(ctx, *realm, company); err != nil {
			return err
		}
		*name = company
	}
	log.Info("installation imported", "realm_id", *realm, "company", *name, "environment", *env)
	return nil
}

func printSchema(_ context.Context, _ *slog.Logger, _ []string) error {
	reg, err := schema.Default()
	if err != nil {
		return err
	}
	for _, e := range reg.Entities() {
		fmt.Printf("%-16s -> %-16s sync=%-4s columns=%d\n", e.Name, e.Table, e.Sync, len(e.Columns()))
	}
	return nil
}
