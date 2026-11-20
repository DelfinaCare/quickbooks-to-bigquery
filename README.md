# qbo-to-bigquery

Mirrors QuickBooks Online (QBO) accounting data into strongly-typed BigQuery
tables. One static Go binary (`qbo-bq`) in a `FROM scratch` image, deployed on
Cloud Run as:

- **`qbo-bq serve`** – a small IAP-protected admin UI to connect, pause and
  disconnect QuickBooks companies;
- **`qbo-bq sync`** – a batch job, run hourly by Cloud Scheduler, that pulls
  changes for every connected company and merges them into BigQuery.

```
                 ┌────────────── Cloud Run job: qbo-bq sync ──────────────┐
Intuit QBO API ─▶│ extract ─▶ archive raw ─▶ map (YAML schema) ─▶ stage+MERGE │─▶ BigQuery  raw_qbo.*
   ▲             └─────│───────────│────────────────────────────────────────┘
   │ OAuth2            │           └─▶ GCS  gs://bucket/raw/realm_id=…/entity=…/dt=…/run_id=…/
   │                   ▼
Cloud Run service   PostgreSQL (encrypted refresh tokens, installation status)
qbo-bq serve (IAP)
```

## What you get in BigQuery

One table per QBO entity (29 of them: `invoice`, `bill`, `customer`, `account`,
`journal_entry`, `payment`, …; run `qbo-bq schema` for the list), all companies
together, keyed by `(realm_id, id)` and clustered on those columns.

Every table starts with the same envelope columns:

| column | type | meaning |
| --- | --- | --- |
| `realm_id` | STRING | QBO company id |
| `id` | STRING | QBO entity `Id` |
| `sync_token` | STRING | QBO version counter |
| `created_at`, `updated_at` | TIMESTAMP | `MetaData.CreateTime` / `LastUpdatedTime` (UTC) |
| `is_deleted` | BOOL | set when QBO reports the record deleted; the last known values are kept |
| `_extracted_at` | TIMESTAMP | when this version was fetched |
| `_raw_gcs_uri` | STRING | the archived raw API response this row came from (NULL when archival is off) |

Then the entity's own fields in `snake_case`: money as `NUMERIC` (never routed
through floats), dates as `DATE`, references as `STRUCT<value, name, type>`, and
lines as `ARRAY<STRUCT<…>>`:

```sql
SELECT i.doc_number, i.txn_date, i.customer_ref.name AS customer,
       l.sales_item_line_detail.item_ref.name AS item, l.amount
FROM raw_qbo.invoice AS i, UNNEST(i.line) AS l
WHERE i.realm_id = '1234567890' AND NOT i.is_deleted
  AND l.detail_type = 'SalesItemLineDetail';
```

Bookkeeping tables:

| table | purpose |
| --- | --- |
| `_sync_state` | per `(realm_id, entity)`: CDC watermark and last full refresh |
| `_ingest_errors` | one row per field that failed type coercion, with the raw record and its `gcs_uri` |
| `_report_snapshots` | raw JSON of ProfitAndLoss and BalanceSheet (Accrual and Cash), by month, previous calendar year to date; one snapshot per report per day |

## How a sync works

For each active company, per entity:

1. **CDC (normal case).** If the entity has a watermark younger than 29 days,
   it is fetched through QBO's change-data-capture endpoint. All entities
   sharing a watermark go in **one** API call, so a quiet hourly run costs a
   single request per company. CDC also reports deletions.
2. **Full refresh.** Used for the first backfill, when the watermark is too old,
   when CDC returns its 1,000-record cap for an entity, for the few entities
   CDC does not support (`sync: full` in their YAML), every
   `FULL_REFRESH_INTERVAL_DAYS` as a self-healing reconciliation, or on
   `sync --full`. The whole entity is paged through and merged as a snapshot:
   rows missing from the snapshot are flagged `is_deleted`. Multi-page scans
   are verified against `COUNT(*)`; if the two keep disagreeing (records moving
   while paging) the rows are still upserted but nothing is flagged deleted,
   and the next run retries.
3. Every raw response is **archived to GCS before it is parsed** (when a
   bucket is configured).
4. Records are mapped through the YAML schema. A field that fails coercion is
   loaded as `NULL` and logged to `_ingest_errors`; the rest of the record and
   batch still load. Keys missing from the schema produce a
   `unknown_qbo_field` warning log (once per path per run).
5. Rows go to a staging table and are `MERGE`d (idempotently) into the entity
   table. Only after the merge succeeds does the entity's watermark advance, to
   the run start minus a 10-minute overlap.

A failing entity or company never blocks the others; the job exits non-zero if
anything failed so Cloud Run surfaces it.

**Refresh tokens.** Intuit rotates refresh tokens and invalidates the old one.
Every exchange happens inside a PostgreSQL transaction holding
`SELECT … FOR UPDATE` on the installation row, and the rotated token is written
before the commit, so concurrent processes can never lose a rotation. Tokens are
stored AES-256-GCM encrypted. If Intuit rejects a token (`invalid_grant`) the
company is marked `needs_reauth` and shown with a *Reconnect* button in the UI.

## Deploying to Google Cloud

Prerequisites: a Cloud SQL for PostgreSQL instance with a private IP, the
`cloudsql.iam_authentication` flag on and a database for this app, and an app at
[developer.intuit.com](https://developer.intuit.com) with the
`com.intuit.quickbooks.accounting` scope.

Running the `release` workflow with a `version` (see
[RELEASING.md](RELEASING.md)) publishes
a multi-arch (`linux/amd64`, `linux/arm64`) image to
`ghcr.io/<owner>/<repo>:<version>` and creates a GitHub release with an SPDX
SBOM and `qbo-bq` CLI binaries for Linux and macOS (see
`.github/workflows/release.yml`). `qbo-bq version` prints the version. Cloud Run cannot pull
from GHCR directly: copy the image into Artifact Registry, or point an Artifact
Registry remote repository at `https://ghcr.io`.

Deployment is a Terraform module in [`terraform/`](terraform/README.md):

```hcl
module "qbo_to_bigquery" {
  source = "github.com/DelfinaCare/qbo-to-bigquery//terraform?ref=main"

  project_id = "my-project"
  image      = "us-central1-docker.pkg.dev/my-project/qbo-to-bigquery/qbo-bq:v1"

  bq_dataset         = google_bigquery_dataset.qbo.dataset_id   # you create it
  raw_payload_bucket = google_storage_bucket.qbo_raw.name       # optional; you create it

  db_instance                 = "qbo-postgres"             # Cloud SQL instance name
  db_name                     = "qbo"
  token_encryption_key_secret = "qbo-token-encryption-key" # Secret Manager secret ids

  qbo_client_id            = "…"
  qbo_client_secret_secret = "qbo-client-secret"

  iap_members = ["group:finance-eng@example.com"]
}
```

The module creates only stateless pieces: a least-privilege service account
that logs in to PostgreSQL as an IAM database user (no password to rotate),
the `qbo-manager` service behind IAP, the `qbo-sync-worker` job with Direct VPC
egress, and an hourly schedule. The dataset, the optional raw-payload bucket
(any storage class and lifecycle you like), the image and the secrets are
yours; the one-off commands are in the [module README](terraform/README.md).
Secret values never enter Terraform state.

Afterwards:

1. Add the `redirect_uri` output to your Intuit app.
2. Open the admin UI, click **Connect QuickBooks**, and authorise a company.
3. Wait for the next scheduled run, or start one:
   ```bash
   gcloud run jobs execute qbo-sync-worker --region us-central1
   ```

> Intuit issues **separate keys for sandbox and production**. Production keys
> go in `QBO_CLIENT_ID` / `QBO_CLIENT_SECRET`, development (sandbox) keys in
> `QBO_SANDBOX_CLIENT_ID` / `QBO_SANDBOX_CLIENT_SECRET`. The UI shows a connect
> button for each environment that has keys.

## Configuration

| variable | default | notes |
| --- | --- | --- |
| `GCP_PROJECT_ID` | – | required for `sync` / `replay` |
| `BQ_DATASET` | `raw_qbo` | created if missing (the Terraform module expects it to exist) |
| `BQ_LOCATION` | empty | only needed if the app creates the dataset somewhere other than the default; otherwise BigQuery infers it |
| `RAW_PAYLOAD_GCS_BUCKET` | empty | existing bucket for the raw response archive; empty disables archival |
| `RAW_PAYLOAD_GCS_PREFIX` | `raw` | |
| `DB_HOST`, `DB_USER`, `DB_NAME` | – | required; direct TCP connection |
| `DB_PASS` | – | ignored with `DB_IAM_AUTH` |
| `DB_IAM_AUTH` | `false` | Cloud SQL IAM database authentication: log in as `DB_USER` (e.g. `sa-name@project.iam`) with an access token of the ambient Google credentials; requires `DB_SSLMODE` `require` or stricter |
| `DB_PORT` / `DB_SSLMODE` | `5432` / `prefer` | |
| `TOKEN_ENCRYPTION_KEY` | – | required; `openssl rand -base64 32`. Changing it invalidates stored tokens. |
| `QBO_CLIENT_ID`, `QBO_CLIENT_SECRET` | – | production app keys |
| `QBO_SANDBOX_CLIENT_ID`, `QBO_SANDBOX_CLIENT_SECRET` | – | development app keys (at least one pair is required) |
| `QBO_REDIRECT_URI` | – | required for `serve`; must match the Intuit app exactly |
| `QBO_MINOR_VERSION` | `75` | |
| `SYNC_REPORTS` | `true` | |
| `FULL_REFRESH_INTERVAL_DAYS` | `7` | `0` disables the periodic reconciliation |
| `BQ_WRITE_METHOD` | `merge` | `merge` (staging table + `MERGE`) or `storage_write` (Storage Write API CDC upserts; experimental, see below) |
| `BQ_MAX_STALENESS_MINUTES` | `15` | `storage_write` only: table `max_staleness`; `0` leaves it unset (every query merges pending changes) |
| `WEB_PATH_PREFIX` | empty | e.g. `/qbo` to serve the UI under a path |
| `PORT` | `8080` | |

### `BQ_WRITE_METHOD=storage_write` (experimental)

Instead of a staging table and a `MERGE` per entity, rows are streamed to the
table's default stream through the BigQuery Storage Write API as CDC
`UPSERT`s, ordered by a `_CHANGE_SEQUENCE_NUMBER` built from
`(updated_at, _extracted_at)`, and BigQuery applies them in the background
within `max_staleness`. On first use each entity table, `_sync_state` and
`_report_snapshots` get a `PRIMARY KEY … NOT ENFORCED` and the
`max_staleness` option. Be aware that:

- `MERGE`/`UPDATE`/`DELETE` on those tables fail while streamed changes are
  pending, so do not run both write methods (or manual DML) at once. To switch
  back to `merge`, stop the sync job and wait for the changes to apply.
- Deletions and reconciling full refreshes read the affected rows back with a
  query so tombstones keep their last known values.
- Staged rows for one entity are held in memory until they are written.
- Background apply jobs read the whole table and are billed at on-demand
  rates unless the project has a `BACKGROUND` reservation.

## Commands

```
qbo-bq serve                                  admin UI
qbo-bq sync [--realm ID] [--entities A,B] [--full] [--local-out DIR]
qbo-bq migrate                                create the PostgreSQL tables
qbo-bq replay --gcs-uri gs://… [--entity Invoice] [--realm ID] [--dry-run]
qbo-bq import-token --realm ID [--env sandbox]   (reads QBO_REFRESH_TOKEN)
qbo-bq schema                                 list tables
```

## Schema DSL

Each `schemas/<entity>.yaml` describes one table; `schemas/_types.yaml` holds
reusable nested types.

```yaml
entity: Invoice            # QBO entity name; table defaults to snake_case (invoice)
sync: cdc                  # cdc | full
include: [TxnHeader]       # prepend the fields of a type from _types.yaml
fields:
  - {qbo: DueDate, type: DATE}                    # column due_date
  - {qbo: TotalAmt, type: NUMERIC}                # column total_amt
  - {qbo: CustomerRef, ref: Ref}                  # STRUCT from _types.yaml
  - {qbo: Line, ref: SalesLine, repeated: true}   # ARRAY<STRUCT>
  - {qbo: PrivateNote, bq: memo, type: STRING}    # explicit column name
```

- Scalar types: `STRING`, `NUMERIC`, `DATE`, `TIMESTAMP`, `BOOL`, `INT64`, and
  `JSON` (an opaque value stored as a JSON string; use `PARSE_JSON()`).
- `include_inactive: true` makes full refreshes ask QBO for inactive records
  too (QBO hides them by default for accounts, customers, items, …).
- `Id`, `SyncToken`, `MetaData` and deletion status are handled by the envelope.
- Adding a field is additive: the next sync adds the column (or nested field)
  to the existing table. Type changes are refused and need a manual migration.

To add a field QBO started sending (you will see an `unknown_qbo_field`
warning), add one line to the YAML, then backfill with
`qbo-bq sync --full --entities <Entity>`.

## Debugging a conversion error

```sql
SELECT entity, field_path, expected_type, observed_value, error_message, gcs_uri
FROM raw_qbo._ingest_errors ORDER BY failed_at DESC LIMIT 20;
```

Fix the YAML type, then re-run the exact payload that failed, offline:

```bash
qbo-bq replay --gcs-uri gs://bucket/raw/realm_id=…/…/00012_query.json.gz --dry-run   # expect ingest_errors: 0
qbo-bq replay --gcs-uri gs://bucket/raw/realm_id=…/…/00012_query.json.gz             # merge the recovered rows
```

`--gcs-uri` also accepts a local `.json` / `.json.gz` file. Replay needs raw
archival to be enabled; without a bucket, `_ingest_errors.raw_record_json`
still holds the failing record.

## Local development

```bash
go test ./...                                   # unit tests
TEST_DATABASE_URL='postgres://qbo:qbo@localhost:5432/qbo?sslmode=disable' go test ./...   # + PostgreSQL tests

# Run a real sync against QBO without any GCP resources: raw payloads and the
# rows that would be loaded are written under ./out instead.
export DB_HOST=localhost DB_USER=qbo DB_PASS=qbo DB_NAME=qbo DB_SSLMODE=disable
export TOKEN_ENCRYPTION_KEY=$(openssl rand -base64 32)
export QBO_SANDBOX_CLIENT_ID=… QBO_SANDBOX_CLIENT_SECRET=…
QBO_REFRESH_TOKEN=… go run ./cmd/qbo-bq import-token --realm 1234567890 --env sandbox
go run ./cmd/qbo-bq sync --local-out ./out
```

`BIGQUERY_EMULATOR_HOST` and `STORAGE_EMULATOR_HOST` are honoured for
experiments with emulators (note that `goccy/bigquery-emulator` cannot execute
`MERGE` with a subquery source, which the loader uses).

## Known limitations

- Reports are stored as raw JSON; they are not flattened into rows.
- `Preferences`, `Budget`, `Attachable` and other entities not listed in
  `schemas/` are not mirrored. Adding one is a single YAML file.
- Companies are synced one after another within a run.
- Disconnecting a company keeps its rows in BigQuery.

## License

[MIT](LICENSE)
