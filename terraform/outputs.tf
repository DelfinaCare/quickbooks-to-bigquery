output "admin_url" {
  description = "URL of the admin UI (behind IAP)."
  value       = "${google_cloud_run_v2_service.manager.uri}${local.web_path_prefix}/"
}

output "redirect_uri" {
  description = "Add this to the Redirect URIs of your Intuit app."
  value       = local.redirect_uri
}

output "service_name" {
  description = "Cloud Run service running the admin UI."
  value       = google_cloud_run_v2_service.manager.name
}

output "job_name" {
  description = "Cloud Run job that syncs; run it with `gcloud run jobs execute`."
  value       = google_cloud_run_v2_job.sync.name
}

output "service_account_email" {
  description = "Runtime service account of the service and the job."
  value       = google_service_account.runtime.email
}
