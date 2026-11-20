# Minimal deployment. Create the secrets and push the image first; see
# ../../README.md.

terraform {
  required_version = ">= 1.5"
}

variable "project_id" {
  type = string
}

variable "image" {
  type = string
}

provider "google" {
  project = var.project_id
}

provider "google-beta" {
  project = var.project_id
}

# The module creates nothing that holds data: the dataset and the bucket are
# yours to configure (location, storage class, retention, ...).
resource "google_bigquery_dataset" "qbo" {
  dataset_id = "raw_qbo"
  location   = "US"
}

resource "google_storage_bucket" "qbo_raw" {
  name                        = "${var.project_id}-qbo-raw"
  location                    = "US"
  storage_class               = "COLDLINE"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }
}

module "qbo_to_bigquery" {
  source = "../.."

  project_id = var.project_id
  image      = var.image

  bq_dataset         = google_bigquery_dataset.qbo.dataset_id
  raw_payload_bucket = google_storage_bucket.qbo_raw.name # omit to disable raw archival

  db_instance                 = "qbo-postgres" # Cloud SQL, with cloudsql.iam_authentication=on
  db_name                     = "qbo"
  token_encryption_key_secret = "qbo-token-encryption-key"

  qbo_client_id            = "your-intuit-client-id"
  qbo_client_secret_secret = "qbo-client-secret"
  # qbo_environment        = "sandbox" # when using Intuit development keys

  iap_members = ["group:finance-eng@example.com"]
}

output "admin_url" {
  value = module.qbo_to_bigquery.admin_url
}

output "redirect_uri" {
  value = module.qbo_to_bigquery.redirect_uri
}

output "db_user" {
  value = module.qbo_to_bigquery.db_user
}
