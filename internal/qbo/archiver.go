package qbo

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
)

// ArchiveMeta describes one raw API response.
type ArchiveMeta struct {
	RealmID    string
	Entity     string // QBO entity, or "_cdc" / "_report" for multi-entity calls
	RunID      string
	Seq        int    // per-run sequence number
	Endpoint   string // short label: "query", "cdc", "report_ProfitAndLoss_Accrual", ...
	RequestURL string
	HTTPStatus int
	FetchedAt  time.Time
}

// Archiver persists raw responses and reads them back for replay.
type Archiver interface {
	// Archive stores body and returns its URI (gs://... or a local path).
	Archive(ctx context.Context, meta ArchiveMeta, body []byte) (string, error)
	// Read returns the decompressed payload stored at uri.
	Read(ctx context.Context, uri string) ([]byte, error)
}

var unsafePathRe = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

// ObjectPath is the Hive-partitioned object name (without bucket/prefix):
// realm_id=…/entity=…/dt=YYYY-MM-DD/run_id=…/00001_query.json.gz
func ObjectPath(m ArchiveMeta) string {
	clean := func(s string) string { return unsafePathRe.ReplaceAllString(s, "_") }
	return fmt.Sprintf("realm_id=%s/entity=%s/dt=%s/run_id=%s/%05d_%s.json.gz",
		clean(m.RealmID), clean(m.Entity), m.FetchedAt.UTC().Format("2006-01-02"), clean(m.RunID), m.Seq, clean(m.Endpoint))
}

var realmInPathRe = regexp.MustCompile(`realm_id=([A-Za-z0-9_.-]+)`)

// RealmFromURI recovers the realm id from an archive path, if present.
func RealmFromURI(uri string) string {
	if m := realmInPathRe.FindStringSubmatch(uri); m != nil {
		return m[1]
	}
	return ""
}

func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// maybeGunzip decompresses b if it starts with the gzip magic number. GCS may
// or may not have transcoded the object already, so sniff rather than assume.
func maybeGunzip(b []byte) ([]byte, error) {
	if len(b) < 2 || b[0] != 0x1f || b[1] != 0x8b {
		return b, nil
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

// NopArchiver disables archival (no RAW_PAYLOAD_GCS_BUCKET configured): rows
// get no _raw_gcs_uri and there is nothing to replay. _ingest_errors still
// carries the offending record's raw JSON.
type NopArchiver struct{}

// Archive implements Archiver.
func (NopArchiver) Archive(context.Context, ArchiveMeta, []byte) (string, error) { return "", nil }

// Read implements Archiver.
func (NopArchiver) Read(context.Context, string) ([]byte, error) {
	return nil, errors.New("raw response archival is disabled")
}

// GCSArchiver writes gzipped responses to Cloud Storage.
type GCSArchiver struct {
	Client *storage.Client
	Bucket string
	Prefix string
}

// Archive implements Archiver.
func (a *GCSArchiver) Archive(ctx context.Context, m ArchiveMeta, body []byte) (string, error) {
	name := ObjectPath(m)
	if a.Prefix != "" {
		name = a.Prefix + "/" + name
	}
	gz, err := gzipBytes(body)
	if err != nil {
		return "", err
	}
	w := a.Client.Bucket(a.Bucket).Object(name).NewWriter(ctx)
	w.ContentType = "application/json"
	w.ContentEncoding = "gzip"
	// Surfaced as x-goog-meta-* headers.
	w.Metadata = map[string]string{
		"realm-id":    m.RealmID,
		"entity":      m.Entity,
		"run-id":      m.RunID,
		"request-url": m.RequestURL,
		"http-status": strconv.Itoa(m.HTTPStatus),
	}
	if _, err := w.Write(gz); err != nil {
		_ = w.Close()
		return "", fmt.Errorf("archive to gs://%s/%s: %w", a.Bucket, name, err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("archive to gs://%s/%s: %w", a.Bucket, name, err)
	}
	return "gs://" + a.Bucket + "/" + name, nil
}

// Read implements Archiver for gs:// URIs.
func (a *GCSArchiver) Read(ctx context.Context, uri string) ([]byte, error) {
	rest, ok := strings.CutPrefix(uri, "gs://")
	bucket, object, found := strings.Cut(rest, "/")
	if !ok || !found || bucket == "" || object == "" {
		return nil, fmt.Errorf("not a gs://bucket/object URI: %q", uri)
	}
	r, err := a.Client.Bucket(bucket).Object(object).ReadCompressed(true).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", uri, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return maybeGunzip(b)
}

// LocalArchiver writes the same layout under a directory. Used by
// `sync --local-out` and tests; URIs are plain file paths.
type LocalArchiver struct{ Dir string }

// Archive implements Archiver.
func (a *LocalArchiver) Archive(_ context.Context, m ArchiveMeta, body []byte) (string, error) {
	p := filepath.Join(a.Dir, filepath.FromSlash(ObjectPath(m)))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", err
	}
	gz, err := gzipBytes(body)
	if err != nil {
		return "", err
	}
	return p, os.WriteFile(p, gz, 0o644)
}

// Read implements Archiver for local paths (gzipped or plain JSON).
func (a *LocalArchiver) Read(_ context.Context, uri string) ([]byte, error) {
	b, err := os.ReadFile(strings.TrimPrefix(uri, "file://"))
	if err != nil {
		return nil, err
	}
	return maybeGunzip(b)
}
