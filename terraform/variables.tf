# ---- Required ---------------------------------------------------------------

variable "project_id" {
  description = "Google Cloud project to deploy into."
  type        = string
}

variable "image" {
  description = "Container image for qbo-bq, e.g. us-central1-docker.pkg.dev/PROJECT/qbo-to-bigquery/qbo-bq:v1. Build and push it before applying."
  type        = string
}

variable "bq_dataset" {
  description = "ID of an existing BigQuery dataset in project_id to mirror into. The runtime service account is granted dataEditor on it."
  type        = string
}

variable "db_instance" {
  description = "Name of the existing Cloud SQL for PostgreSQL instance (in project_id) holding installations and encrypted refresh tokens. It must have the cloudsql.iam_authentication flag on: the module creates an IAM database user for the runtime service account, so there is no password to manage."
  type        = string
}

variable "db_name" {
  description = "PostgreSQL database on db_instance."
  type        = string
}

variable "token_encryption_key_secret" {
  description = "ID of an existing Secret Manager secret holding the base64-encoded 32-byte key that encrypts refresh tokens (openssl rand -base64 32). Never rotate it in place: stored tokens become unreadable."
  type        = string
}

variable "qbo_client_id" {
  description = "Intuit app client id. Use the production keys, or the development keys together with qbo_environment = \"sandbox\"."
  type        = string
}

variable "qbo_client_secret_secret" {
  description = "ID of an existing Secret Manager secret holding the client secret that matches qbo_client_id."
  type        = string
}

variable "iap_members" {
  description = "Principals allowed through IAP to the admin UI, e.g. [\"group:finance-eng@example.com\"]."
  type        = list(string)
}

# ---- Optional ---------------------------------------------------------------

variable "qbo_environment" {
  description = "Which Intuit environment qbo_client_id belongs to. It selects the API host (sandbox-quickbooks.api.intuit.com vs quickbooks.api.intuit.com), so it must match the keys."
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "sandbox"], var.qbo_environment)
    error_message = "qbo_environment must be \"production\" or \"sandbox\"."
  }
}

variable "qbo_redirect_uri" {
  description = "OAuth redirect URI registered with Intuit. Defaults to the service's run.app URL + web_path_prefix + /callback; set it when serving from a custom domain."
  type        = string
  default     = null
}

variable "raw_payload_bucket" {
  description = "Name of an existing GCS bucket in which to archive every raw QBO API response. The runtime service account is granted object create/view on it. Leave null to disable raw archival (and with it `qbo-bq replay` and the _raw_gcs_uri column)."
  type        = string
  default     = null
}

variable "raw_payload_prefix" {
  description = "Object prefix inside raw_payload_bucket."
  type        = string
  default     = "raw"
}

variable "cloud_run_region" {
  description = "Region for the Cloud Run service and job, and for the Cloud Scheduler job that triggers the sync."
  type        = string
  default     = "us-central1"
}

variable "name_prefix" {
  description = "Prefix for resource names (service, job, service accounts, scheduler job)."
  type        = string
  default     = "qbo"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{0,11}$", var.name_prefix))
    error_message = "name_prefix must be 1-12 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "db_host" {
  description = "Address the app connects to. Defaults to db_instance's private IP; set it when reaching the instance some other way (e.g. a Private Service Connect endpoint)."
  type        = string
  default     = null
}

variable "db_roles" {
  description = "PostgreSQL roles granted to the runtime service account's database user, e.g. [\"qbo\"] to inherit the tables owned by a former password user. Without one, grant it CREATE on the schema yourself (see README)."
  type        = list(string)
  default     = []
}

variable "db_port" {
  description = "PostgreSQL port."
  type        = number
  default     = 5432
}

variable "db_sslmode" {
  description = "PostgreSQL sslmode. The password is an access token, so the connection must be encrypted."
  type        = string
  default     = "require"

  validation {
    condition     = contains(["require", "verify-ca", "verify-full"], var.db_sslmode)
    error_message = "db_sslmode must be require, verify-ca or verify-full."
  }
}

variable "vpc_network" {
  description = "VPC network (name or self link) with a route to the database, used for Direct VPC egress."
  type        = string
  default     = "default"
}

variable "vpc_subnetwork" {
  description = "Subnetwork in cloud_run_region for Direct VPC egress."
  type        = string
  default     = "default"
}

variable "web_path_prefix" {
  description = "Serve the admin UI under this path, e.g. \"/qbo\". Empty serves it at the root."
  type        = string
  default     = ""
}

variable "sync_schedule" {
  description = "Cron schedule for the sync job."
  type        = string
  default     = "7 * * * *"
}

variable "sync_schedule_time_zone" {
  description = "Time zone of sync_schedule."
  type        = string
  default     = "Etc/UTC"
}

variable "sync_paused" {
  description = "Create the scheduler job paused."
  type        = bool
  default     = false
}

variable "sync_timeout_seconds" {
  description = "Maximum duration of one sync run."
  type        = number
  default     = 3600
}

variable "sync_reports" {
  description = "Snapshot ProfitAndLoss and BalanceSheet reports."
  type        = bool
  default     = true
}

variable "full_refresh_interval_days" {
  description = "Fully re-scan each entity this often to reconcile deletes (0 disables)."
  type        = number
  default     = 7
}

variable "bq_write_method" {
  description = "How rows reach BigQuery: \"merge\" (staging table + MERGE) or \"storage_write\" (Storage Write API CDC upserts). Switching to storage_write adds a primary key and max_staleness to the tables; see the README before switching back."
  type        = string
  default     = "merge"
  validation {
    condition     = contains(["merge", "storage_write"], var.bq_write_method)
    error_message = "bq_write_method must be merge or storage_write."
  }
}

variable "bq_max_staleness_minutes" {
  description = "storage_write only: the tables' max_staleness, i.e. how often BigQuery applies streamed upserts in the background (0 = merge at query time)."
  type        = number
  default     = 15
}

variable "qbo_minor_version" {
  description = "QBO API minor version."
  type        = number
  default     = 75
}

variable "labels" {
  description = "Labels applied to the Cloud Run service and job."
  type        = map(string)
  default     = {}
}
