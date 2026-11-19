# Terraform module

Deploys qbo-to-bigquery on Google Cloud:

- a runtime service account with `bigquery.jobUser` on the project,
  `bigquery.dataEditor` on your dataset only, object create/view on your bucket
  only, and accessor on the three secrets only
- Cloud Run service `<prefix>-manager` (`qbo-bq serve`) with IAP enabled, and
  IAP access for `iap_members`
- Cloud Run job `<prefix>-sync-worker` (`qbo-bq sync`)
- Cloud Scheduler job that runs the sync job (hourly by default)

Both Cloud Run resources use Direct VPC egress to reach PostgreSQL.

The module creates **nothing that holds data**, so it can be destroyed and
re-created freely. You create, and pass in:

- the **BigQuery dataset** (`bq_dataset`, required);
- optionally a **GCS bucket** for raw API responses (`raw_payload_bucket`), with
  whatever location, storage class and lifecycle you want. Leave it unset and
  raw archival is switched off: rows get no `_raw_gcs_uri` and `qbo-bq replay`
  has nothing to replay, though `_ingest_errors` still stores each failing
  record's raw JSON;
- **PostgreSQL**, the **secrets**, and the **container image**.

Secret values are deliberately kept out of Terraform so they never land in state.

Requires Terraform >= 1.5 and the `google` / `google-beta` providers >= 8.0
(`google-beta` is only used to create the IAP service agent).

## 1. Prerequisites (once)

The module does not enable APIs:

```bash
gcloud services enable run.googleapis.com cloudscheduler.googleapis.com bigquery.googleapis.com storage.googleapis.com secretmanager.googleapis.com iap.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com
```

```bash
gcloud artifacts repositories create qbo-to-bigquery --repository-format=docker --location=us-central1
```

```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/$PROJECT_ID/qbo-to-bigquery/qbo-bq:v1 .
```

Create the secrets (each command reads the value from stdin):

```bash
openssl rand -base64 32 | tr -d '\n' | gcloud secrets create qbo-token-encryption-key --data-file=-
```

```bash
printf '%s' "$DB_PASS" | gcloud secrets create qbo-db-pass --data-file=-
```

```bash
printf '%s' "$QBO_CLIENT_SECRET" | gcloud secrets create qbo-client-secret --data-file=-
```

PostgreSQL needs a database and user for the app, reachable from
`vpc_network` / `vpc_subnetwork`. The tables are created by the app on start.

## 2. Use the module

```hcl
resource "google_bigquery_dataset" "qbo" {
  dataset_id = "raw_qbo"
  location   = "US"
}

resource "google_storage_bucket" "qbo_raw" {
  name                        = "my-project-qbo-raw"
  location                    = "US" # keep it with the dataset
  storage_class               = "COLDLINE"
  uniform_bucket_level_access = true
}

module "qbo_to_bigquery" {
  source = "github.com/DelfinaCare/qbo-to-bigquery//terraform?ref=main"

  project_id = "my-project"
  image      = "us-central1-docker.pkg.dev/my-project/qbo-to-bigquery/qbo-bq:v1"

  bq_dataset         = google_bigquery_dataset.qbo.dataset_id
  raw_payload_bucket = google_storage_bucket.qbo_raw.name # omit to disable raw archival

  db_host                     = "10.1.2.3"
  db_user                     = "qbo"
  db_name                     = "qbo"
  db_password_secret          = "qbo-db-pass"
  token_encryption_key_secret = "qbo-token-encryption-key"

  qbo_client_id            = "…"
  qbo_client_secret_secret = "qbo-client-secret"
  # qbo_environment        = "sandbox" # when these are Intuit development keys

  iap_members = ["group:finance-eng@example.com"]
}

output "redirect_uri" {
  value = module.qbo_to_bigquery.redirect_uri
}
```

A runnable version is in [`examples/basic`](examples/basic).

## 3. After applying

1. Add the `redirect_uri` output to your Intuit app's Redirect URIs.
2. Open the `admin_url` output and connect a company.
3. Wait for the schedule or run `gcloud run jobs execute <job_name> --region <region>`.

To roll out a new version, push a new image tag and change `image`.

## Inputs worth knowing

| input | default | notes |
| --- | --- | --- |
| `cloud_run_region` | `us-central1` | Cloud Run service and job, and the scheduler job |
| `qbo_environment` | `production` | `sandbox` when `qbo_client_id` is a development key |
| `raw_payload_bucket` | `null` | existing bucket; unset disables raw archival |
| `raw_payload_prefix` | `raw` | object prefix in that bucket |
| `vpc_network`, `vpc_subnetwork` | `default` | must route to `db_host`; the subnet must be in `cloud_run_region` |
| `web_path_prefix` | `""` | e.g. `/qbo` |
| `qbo_redirect_uri` | run.app URL + prefix + `/callback` | set when using a custom domain |
| `sync_schedule` | `7 * * * *` | cron, in `sync_schedule_time_zone` |
| `name_prefix` | `qbo` | lets several deployments share a project |

All inputs are documented in [`variables.tf`](variables.tf); outputs in
[`outputs.tf`](outputs.tf).

## Notes

- The identity running Terraform needs permission to act as the two service
  accounts the module creates (`roles/iam.serviceAccountUser`), in addition to
  admin rights on the resource types above.
- IAP on Cloud Run requires the project to belong to an organization.
- One deployment talks to one Intuit environment. `qbo_environment` matters
  because sandbox companies live on a different API host, not just behind
  different keys.
- Each sync writes one small object per API response. Coldline and Archive
  have minimum storage durations and per-operation charges, so pair them with
  a lifecycle rule no shorter than the minimum (90 days for Coldline).
- `raw_payload_bucket` must be known at plan time (a literal or a bucket whose
  name is set in configuration), because it decides whether IAM grants exist.
