# Notes for coding agents

Read `README.md` first. This file is the short list of things that are easy to
get wrong here.

## Layout

- `schemas/*.yaml` – the table definitions. Most changes should only touch these.
- `internal/schema` – YAML compiler (`registry.go`) and JSON → row mapper (`mapper.go`).
- `internal/qbo` – OAuth, rate-limited client, GCS archiver, extraction planning/parsing.
- `internal/syncer` – orchestration of one run (`SyncRealm`, `Replay`) behind the `Warehouse` interface.
- `internal/bq` – BigQuery `Warehouse`: `sql.go` is pure SQL/schema generation (unit-tested), `loader.go` is API calls.
- `internal/db` – PostgreSQL installations + the refresh-token rotation guardrail.
- `internal/web` – admin UI.
- `terraform/` – the deployment module. Environment variables set there must match `internal/config`.

## Common tasks

**QBO sends a field we do not map** (`unknown_qbo_field` warning with a `path`
such as `Invoice.Line[].NewThing`): add `{qbo: NewThing, type: …}` to the type
that owns that path, run `go test ./internal/schema`, deploy, then
`qbo-bq sync --full --entities Invoice` to backfill.

**A field fails coercion** (rows in `_ingest_errors`): the row has the raw
record and a `gcs_uri`. Fix the YAML, verify with
`qbo-bq replay --gcs-uri <uri> --dry-run`, then replay without `--dry-run`.
A column's type cannot be changed in place: add a new column name via `bq:`
or migrate the table by hand.

**New entity**: add `schemas/<snake_case>.yaml`. Check in the QBO sandbox
whether the CDC endpoint accepts it (`sync: cdc`) and whether its `Active`
property is queryable (`include_inactive: true`); `JournalCode` and `TaxAgency`
fail both, `TaxCode` loses rows with the Active filter. Update the entity count
in `internal/schema/registry_test.go`.

## Invariants – do not break these

- A refresh token must only be exchanged through `db.Store.RefreshAccessToken`.
  Intuit invalidates the old token on rotation; an exchange outside the row
  lock can strand the installation.
- Archive the raw response before parsing it (`Syncer.fetch`). Archival is
  optional (`NopArchiver` when no bucket is configured), so never assume a
  `gcs_uri` is non-empty.
- `_sync_state` advances only after the entity's `MERGE` succeeded (or, with
  `BQ_WRITE_METHOD=storage_write`, after every append was acknowledged).
- With `storage_write`, tables carry a primary key and reject DML while
  streamed changes are pending; never mix the two write methods on a dataset.
- Only a verified complete snapshot may be merged with `reconcile=true`; that
  flag marks every absent row as deleted.
- Every entity `MERGE` pins `T.realm_id = @realm_id` so BigQuery can prune by
  the clustering; a batch must therefore contain a single realm.
- Money never passes through `float64` (`json.Decoder.UseNumber` + `big.Rat`).
- Staging tables and `MERGE` column lists come from the table's *live* schema,
  so STRUCT layouts always match the target exactly.
- Never remove or retype a field in YAML expecting BigQuery to follow; schema
  evolution is additive only.

## Checks

```bash
gofmt -l . && go vet ./... && go test ./...
TEST_DATABASE_URL=postgres://… go test ./internal/db   # needs PostgreSQL
terraform fmt -check -recursive terraform && (cd terraform/examples/basic && terraform init -backend=false && terraform validate)
```

BigQuery and GCS calls in `internal/bq/loader.go` and `GCSArchiver` are not
covered by unit tests; exercise changes to them against a real project.
