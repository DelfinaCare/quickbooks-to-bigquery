// Package config loads and validates configuration from environment variables.
package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Feature flags select which groups of variables a command requires.
type Feature int

const (
	NeedDB  Feature = iota // PostgreSQL + TOKEN_ENCRYPTION_KEY
	NeedQBO                // Intuit app credentials
	NeedGCP                // BigQuery (GCS archival is optional)
	NeedWeb                // QBO_REDIRECT_URI
)

// Config is the full runtime configuration.
type Config struct {
	GCPProjectID string
	BQDataset    string
	BQLocation   string // optional; empty lets BigQuery infer it from the dataset
	GCSBucket    string // optional; empty disables raw response archival
	GCSPrefix    string
	// BQWriteMethod is "merge" (staging tables + MERGE) or "storage_write"
	// (Storage Write API CDC upserts).
	BQWriteMethod  string
	BQMaxStaleness time.Duration // storage_write only

	DBHost, DBPort, DBUser, DBPass, DBName, DBSSLMode string
	// DBIAMAuth logs in with Cloud SQL IAM database authentication: DBUser is
	// the IAM principal and an OAuth2 access token replaces DBPass.
	DBIAMAuth          bool
	TokenEncryptionKey []byte

	QBOClientID, QBOClientSecret               string // production keys
	QBOSandboxClientID, QBOSandboxClientSecret string // development (sandbox) keys
	QBORedirectURI                             string
	QBOMinorVersion                            string

	SyncReports         bool
	FullRefreshInterval time.Duration
	WebPathPrefix       string
	Port                string
}

// Load reads configuration through getenv (normally os.Getenv).
func Load(getenv func(string) string, need ...Feature) (*Config, error) {
	get := func(k, def string) string {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v
		}
		return def
	}
	c := &Config{
		GCPProjectID:           get("GCP_PROJECT_ID", ""),
		BQDataset:              get("BQ_DATASET", "raw_qbo"),
		BQLocation:             get("BQ_LOCATION", ""),
		BQWriteMethod:          get("BQ_WRITE_METHOD", "merge"),
		GCSBucket:              get("RAW_PAYLOAD_GCS_BUCKET", ""),
		GCSPrefix:              strings.Trim(get("RAW_PAYLOAD_GCS_PREFIX", "raw"), "/"),
		DBHost:                 get("DB_HOST", ""),
		DBPort:                 get("DB_PORT", "5432"),
		DBUser:                 get("DB_USER", ""),
		DBPass:                 getenv("DB_PASS"),
		DBName:                 get("DB_NAME", ""),
		DBSSLMode:              get("DB_SSLMODE", "prefer"),
		QBOClientID:            get("QBO_CLIENT_ID", ""),
		QBOClientSecret:        get("QBO_CLIENT_SECRET", ""),
		QBOSandboxClientID:     get("QBO_SANDBOX_CLIENT_ID", ""),
		QBOSandboxClientSecret: get("QBO_SANDBOX_CLIENT_SECRET", ""),
		QBORedirectURI:         get("QBO_REDIRECT_URI", ""),
		QBOMinorVersion:        get("QBO_MINOR_VERSION", "75"),
		WebPathPrefix:          NormalizePrefix(getenv("WEB_PATH_PREFIX")),
		Port:                   get("PORT", "8080"),
	}
	var problems []string
	bad := func(format string, a ...any) { problems = append(problems, fmt.Sprintf(format, a...)) }

	var err error
	if c.SyncReports, err = strconv.ParseBool(get("SYNC_REPORTS", "true")); err != nil {
		bad("SYNC_REPORTS must be true or false")
	}
	days, err := strconv.Atoi(get("FULL_REFRESH_INTERVAL_DAYS", "7"))
	if err != nil || days < 0 {
		bad("FULL_REFRESH_INTERVAL_DAYS must be a non-negative integer")
	}
	c.FullRefreshInterval = time.Duration(days) * 24 * time.Hour
	if c.DBIAMAuth, err = strconv.ParseBool(get("DB_IAM_AUTH", "false")); err != nil {
		bad("DB_IAM_AUTH must be true or false")
	}
	// The password is a bearer token; never let it go out unencrypted.
	if c.DBIAMAuth && (c.DBSSLMode == "disable" || c.DBSSLMode == "allow" || c.DBSSLMode == "prefer") {
		bad("DB_IAM_AUTH requires DB_SSLMODE=require, verify-ca or verify-full")
	}
	if c.BQWriteMethod != "merge" && c.BQWriteMethod != "storage_write" {
		bad("BQ_WRITE_METHOD must be merge or storage_write")
	}
	mins, err := strconv.Atoi(get("BQ_MAX_STALENESS_MINUTES", "15"))
	if err != nil || mins < 0 {
		bad("BQ_MAX_STALENESS_MINUTES must be a non-negative integer")
	}
	c.BQMaxStaleness = time.Duration(mins) * time.Minute
	if _, err := strconv.Atoi(c.QBOMinorVersion); err != nil {
		bad("QBO_MINOR_VERSION must be an integer")
	}

	for _, f := range need {
		switch f {
		case NeedDB:
			for k, v := range map[string]string{"DB_HOST": c.DBHost, "DB_USER": c.DBUser, "DB_NAME": c.DBName} {
				if v == "" {
					bad("%s is required", k)
				}
			}
			raw := get("TOKEN_ENCRYPTION_KEY", "")
			key, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				key, err = base64.RawURLEncoding.DecodeString(strings.TrimRight(raw, "="))
			}
			if raw == "" || err != nil || len(key) != 32 {
				bad("TOKEN_ENCRYPTION_KEY is required and must be 32 bytes, base64-encoded (generate with: openssl rand -base64 32)")
			}
			c.TokenEncryptionKey = key
		case NeedQBO:
			prod := c.QBOClientID != "" && c.QBOClientSecret != ""
			sandbox := c.QBOSandboxClientID != "" && c.QBOSandboxClientSecret != ""
			if !prod && !sandbox {
				bad("set QBO_CLIENT_ID + QBO_CLIENT_SECRET (production keys) and/or QBO_SANDBOX_CLIENT_ID + QBO_SANDBOX_CLIENT_SECRET (development keys)")
			}
		case NeedGCP:
			if c.GCPProjectID == "" {
				bad("GCP_PROJECT_ID is required")
			}
		case NeedWeb:
			if u, err := url.Parse(c.QBORedirectURI); err != nil || u.Scheme == "" || u.Host == "" {
				bad("QBO_REDIRECT_URI is required and must be an absolute URL")
			}
		}
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("invalid configuration:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return c, nil
}

// NormalizePrefix returns "" or a path with a leading and no trailing slash.
func NormalizePrefix(p string) string {
	p = strings.Trim(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	return "/" + p
}

// Credentials returns the Intuit app keys for an environment.
func (c *Config) Credentials(environment string) (id, secret string, ok bool) {
	if environment == "sandbox" {
		id, secret = c.QBOSandboxClientID, c.QBOSandboxClientSecret
	} else {
		id, secret = c.QBOClientID, c.QBOClientSecret
	}
	return id, secret, id != "" && secret != ""
}

// Environments lists the environments that have credentials configured.
func (c *Config) Environments() []string {
	var out []string
	for _, e := range []string{"production", "sandbox"} {
		if _, _, ok := c.Credentials(e); ok {
			out = append(out, e)
		}
	}
	return out
}

// DatabaseURL builds the pgx connection string. With DBIAMAuth it carries no
// password; db.Open supplies an access token per connection.
func (c *Config) DatabaseURL() string {
	user := url.UserPassword(c.DBUser, c.DBPass)
	if c.DBIAMAuth {
		user = url.User(c.DBUser)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     user,
		Host:     net.JoinHostPort(c.DBHost, c.DBPort),
		Path:     "/" + c.DBName,
		RawQuery: url.Values{"sslmode": {c.DBSSLMode}}.Encode(),
	}
	return u.String()
}
