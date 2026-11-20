# qbo-to-bigquery on Google Cloud: an IAP-protected Cloud Run service for
# onboarding, a Cloud Run job for syncing, and a scheduler to run it.
#
# Nothing here holds data: the BigQuery dataset, the optional raw-payload
# bucket, the Cloud SQL instance and the secrets are created by the caller and
# passed in. The app logs in to Cloud SQL as its own service account (IAM
# database authentication), so there is no database password.

data "google_project" "this" {
  project_id = var.project_id
}

data "google_sql_database_instance" "db" {
  project = var.project_id
  name    = var.db_instance
}

locals {
  service_name = "${var.name_prefix}-manager"
  job_name     = "${var.name_prefix}-sync-worker"

  web_path_prefix = trim(var.web_path_prefix, "/") == "" ? "" : "/${trim(var.web_path_prefix, "/")}"

  # Cloud Run's deterministic URL, so the service can be told its own redirect
  # URI without depending on itself.
  service_url  = "https://${local.service_name}-${data.google_project.this.number}.${var.cloud_run_region}.run.app"
  redirect_uri = coalesce(var.qbo_redirect_uri, "${local.service_url}${local.web_path_prefix}/callback")

  db_host = var.db_host != null ? var.db_host : data.google_sql_database_instance.db.private_ip_address

  sandbox     = var.qbo_environment == "sandbox"
  archive_raw = var.raw_payload_bucket != null

  # The app keeps separate variables per Intuit environment because the
  # environment also decides which API host a company is called on.
  env = merge(
    {
      GCP_PROJECT_ID             = var.project_id
      BQ_DATASET                 = var.bq_dataset
      DB_HOST                    = local.db_host
      DB_PORT                    = tostring(var.db_port)
      DB_USER                    = google_sql_user.runtime.name
      DB_NAME                    = var.db_name
      DB_SSLMODE                 = var.db_sslmode
      DB_IAM_AUTH                = "true"
      QBO_MINOR_VERSION          = tostring(var.qbo_minor_version)
      SYNC_REPORTS               = tostring(var.sync_reports)
      FULL_REFRESH_INTERVAL_DAYS = tostring(var.full_refresh_interval_days)
      BQ_WRITE_METHOD            = var.bq_write_method
      BQ_MAX_STALENESS_MINUTES   = tostring(var.bq_max_staleness_minutes)
    },
    local.sandbox ? { QBO_SANDBOX_CLIENT_ID = var.qbo_client_id } : { QBO_CLIENT_ID = var.qbo_client_id },
    local.archive_raw ? {
      RAW_PAYLOAD_GCS_BUCKET = var.raw_payload_bucket
      RAW_PAYLOAD_GCS_PREFIX = var.raw_payload_prefix
    } : {},
  )

  # Environment variable -> Secret Manager secret id.
  secret_env = merge(
    {
      TOKEN_ENCRYPTION_KEY = var.token_encryption_key_secret
    },
    local.sandbox ? { QBO_SANDBOX_CLIENT_SECRET = var.qbo_client_secret_secret } : { QBO_CLIENT_SECRET = var.qbo_client_secret_secret },
  )
}

# ---- Runtime identity (least privilege) -------------------------------------

resource "google_service_account" "runtime" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-bq-sa"
  display_name = "qbo-to-bigquery runtime"
}

# Running load and query jobs is a project-level permission...
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.runtime.member
}

# ...but data access is limited to the caller's dataset.
resource "google_bigquery_dataset_iam_member" "data_editor" {
  project    = var.project_id
  dataset_id = var.bq_dataset
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.runtime.member
}

# Raw archival is optional: no bucket, no grants, and the app skips it.
resource "google_storage_bucket_iam_member" "raw" {
  for_each = toset(local.archive_raw ? ["roles/storage.objectCreator", "roles/storage.objectViewer"] : [])
  bucket   = var.raw_payload_bucket
  role     = each.value
  member   = google_service_account.runtime.member
}

# ---- Database login (Cloud SQL IAM database authentication) ------------------

# The service account's PostgreSQL role. Cloud SQL names it after the email
# without ".gserviceaccount.com"; the app sends an access token as password.
resource "google_sql_user" "runtime" {
  project        = var.project_id
  instance       = var.db_instance
  name           = trimsuffix(google_service_account.runtime.email, ".gserviceaccount.com")
  type           = "CLOUD_IAM_SERVICE_ACCOUNT"
  database_roles = length(var.db_roles) > 0 ? var.db_roles : null

  # PostgreSQL cannot drop a role that owns objects, and this one owns the
  # app's tables: on destroy, leave it (and the data) in place.
  deletion_policy = "ABANDON"

  lifecycle {
    precondition {
      condition     = local.db_host != null && local.db_host != ""
      error_message = "db_instance has no private IP; set db_host to the address the app should connect to."
    }
  }
}

# cloudsql.instances.login, on this instance only.
resource "google_project_iam_member" "cloudsql_instance_user" {
  project = var.project_id
  role    = "roles/cloudsql.instanceUser"
  member  = google_service_account.runtime.member

  condition {
    title      = "${var.name_prefix}-db-instance"
    expression = "resource.name == \"projects/${var.project_id}/instances/${var.db_instance}\""
  }
}

resource "google_secret_manager_secret_iam_member" "runtime" {
  for_each  = local.secret_env
  project   = var.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.runtime.member
}

# ---- Admin UI: Cloud Run service behind IAP ----------------------------------

resource "google_cloud_run_v2_service" "manager" {
  project             = var.project_id
  name                = local.service_name
  location            = var.cloud_run_region
  ingress             = "INGRESS_TRAFFIC_ALL"
  iap_enabled         = true
  deletion_protection = false # stateless; the data lives in resources the caller owns
  labels              = var.labels

  template {
    service_account = google_service_account.runtime.email

    scaling {
      max_instance_count = 2
    }

    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = var.vpc_network
        subnetwork = var.vpc_subnetwork
      }
    }

    containers {
      image   = var.image
      command = ["/qbo-bq"]
      args    = ["serve"]

      resources {
        limits = {
          cpu    = "1"
          memory = "256Mi"
        }
        # CPU only while serving requests; Cloud Run refuses < 512Mi with
        # always-allocated CPU, which newer API/provider versions default to.
        cpu_idle = true
      }

      dynamic "env" {
        for_each = merge(local.env, {
          WEB_PATH_PREFIX  = local.web_path_prefix
          QBO_REDIRECT_URI = local.redirect_uri
        })
        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.secret_env
        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value
              version = "latest"
            }
          }
        }
      }

      startup_probe {
        http_get {
          path = "/healthz"
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.runtime,
    google_project_iam_member.cloudsql_instance_user,
  ]
}

# IAP calls the service as its own service agent.
resource "google_project_service_identity" "iap" {
  provider = google-beta
  project  = var.project_id
  service  = "iap.googleapis.com"
}

resource "google_cloud_run_v2_service_iam_member" "iap_invoker" {
  project  = var.project_id
  location = var.cloud_run_region
  name     = google_cloud_run_v2_service.manager.name
  role     = "roles/run.invoker"
  member   = google_project_service_identity.iap.member
}

resource "google_iap_web_cloud_run_service_iam_member" "admins" {
  for_each               = toset(var.iap_members)
  project                = var.project_id
  location               = var.cloud_run_region
  cloud_run_service_name = google_cloud_run_v2_service.manager.name
  role                   = "roles/iap.httpsResourceAccessor"
  member                 = each.value
}

# ---- Sync: Cloud Run job ------------------------------------------------------

resource "google_cloud_run_v2_job" "sync" {
  project             = var.project_id
  name                = local.job_name
  location            = var.cloud_run_region
  deletion_protection = false # stateless; the data lives in resources the caller owns
  labels              = var.labels

  template {
    task_count = 1

    template {
      service_account = google_service_account.runtime.email
      max_retries     = 1
      timeout         = "${var.sync_timeout_seconds}s"

      vpc_access {
        egress = "PRIVATE_RANGES_ONLY"
        network_interfaces {
          network    = var.vpc_network
          subnetwork = var.vpc_subnetwork
        }
      }

      containers {
        image   = var.image
        command = ["/qbo-bq"]
        args    = ["sync"]

        resources {
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

        dynamic "env" {
          for_each = local.env
          content {
            name  = env.key
            value = env.value
          }
        }

        dynamic "env" {
          for_each = local.secret_env
          content {
            name = env.key
            value_source {
              secret_key_ref {
                secret  = env.value
                version = "latest"
              }
            }
          }
        }
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.runtime,
    google_project_iam_member.cloudsql_instance_user,
    google_bigquery_dataset_iam_member.data_editor,
    google_storage_bucket_iam_member.raw,
  ]
}

# ---- Schedule -----------------------------------------------------------------

resource "google_service_account" "scheduler" {
  project      = var.project_id
  account_id   = "${var.name_prefix}-scheduler-sa"
  display_name = "qbo-to-bigquery scheduler"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.cloud_run_region
  name     = google_cloud_run_v2_job.sync.name
  role     = "roles/run.invoker"
  member   = google_service_account.scheduler.member
}

resource "google_cloud_scheduler_job" "sync" {
  project          = var.project_id
  region           = var.cloud_run_region
  name             = "${var.name_prefix}-sync"
  description      = "Runs the qbo-to-bigquery sync job"
  schedule         = var.sync_schedule
  time_zone        = var.sync_schedule_time_zone
  paused           = var.sync_paused
  attempt_deadline = "180s"

  retry_config {
    retry_count = 0 # a missed run is simply picked up by the next one
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.cloud_run_region}/jobs/${google_cloud_run_v2_job.sync.name}:run"

    # Google APIs take an OAuth access token, not an OIDC token.
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_invoker]
}
