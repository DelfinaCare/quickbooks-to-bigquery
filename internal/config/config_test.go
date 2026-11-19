package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestNormalizePrefix(t *testing.T) {
	for in, want := range map[string]string{"": "", "/": "", "qbo": "/qbo", "/qbo": "/qbo", "/qbo/": "/qbo", " /a/b/ ": "/a/b"} {
		if got := NormalizePrefix(in); got != want {
			t.Errorf("NormalizePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoad(t *testing.T) {
	full := map[string]string{
		"GCP_PROJECT_ID": "proj", "RAW_PAYLOAD_GCS_BUCKET": "bucket", "DB_HOST": "10.0.0.5", "DB_USER": "u", "DB_PASS": "p@ss/word", "DB_NAME": "qbo",
		"TOKEN_ENCRYPTION_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "QBO_CLIENT_ID": "id", "QBO_CLIENT_SECRET": "sec",
		"QBO_REDIRECT_URI": "https://example.com/qbo/callback", "WEB_PATH_PREFIX": "qbo/",
	}
	c, err := Load(env(full), NeedDB, NeedQBO, NeedGCP, NeedWeb)
	if err != nil {
		t.Fatal(err)
	}
	if c.BQDataset != "raw_qbo" || c.GCSPrefix != "raw" || c.DBPort != "5432" || c.DBSSLMode != "prefer" || c.QBOMinorVersion != "75" ||
		!c.SyncReports || c.WebPathPrefix != "/qbo" || c.Port != "8080" || c.FullRefreshInterval != 7*24*time.Hour || len(c.TokenEncryptionKey) != 32 ||
		c.BQWriteMethod != "merge" || c.BQMaxStaleness != 15*time.Minute {
		t.Errorf("defaults wrong: %+v", c)
	}
	if got := c.DatabaseURL(); got != "postgres://u:p%40ss%2Fword@10.0.0.5:5432/qbo?sslmode=prefer" {
		t.Errorf("DatabaseURL = %s", got)
	}
	if _, _, ok := c.Credentials("sandbox"); ok {
		t.Error("sandbox credentials were not configured")
	}
	if got := strings.Join(c.Environments(), ","); got != "production" {
		t.Errorf("Environments = %s", got)
	}

	_, err = Load(env(map[string]string{"TOKEN_ENCRYPTION_KEY": "c2hvcnQ="}), NeedDB, NeedQBO, NeedGCP, NeedWeb)
	if err == nil {
		t.Fatal("expected validation errors")
	}
	for _, frag := range []string{"DB_HOST", "DB_USER", "DB_NAME", "TOKEN_ENCRYPTION_KEY", "QBO_CLIENT_ID", "GCP_PROJECT_ID", "QBO_REDIRECT_URI"} {
		if !strings.Contains(err.Error(), frag) {
			t.Errorf("error should mention %s: %v", frag, err)
		}
	}
	// Raw archival and the BigQuery location are optional.
	delete(full, "RAW_PAYLOAD_GCS_BUCKET")
	if c, err := Load(env(full), NeedGCP); err != nil || c.GCSBucket != "" || c.BQLocation != "" {
		t.Errorf("bucket should be optional: %+v, %v", c, err)
	}
	// Only what a command needs is required.
	if _, err := Load(env(map[string]string{}), NeedQBO); err == nil || strings.Contains(err.Error(), "DB_HOST") {
		t.Errorf("unexpected: %v", err)
	}
}

func TestLoadWriteMethod(t *testing.T) {
	c, err := Load(env(map[string]string{"BQ_WRITE_METHOD": "storage_write", "BQ_MAX_STALENESS_MINUTES": "0"}))
	if err != nil || c.BQWriteMethod != "storage_write" || c.BQMaxStaleness != 0 {
		t.Fatalf("got %+v, %v", c, err)
	}
	if _, err := Load(env(map[string]string{"BQ_WRITE_METHOD": "insert"})); err == nil {
		t.Fatal("unknown write method accepted")
	}
}
