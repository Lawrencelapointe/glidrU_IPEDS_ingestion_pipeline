# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

output "raw_bucket_name" {
  description = "Name of the GCS bucket for raw IPEDS data"
  value       = google_storage_bucket.ipeds_raw.name
}

output "raw_bucket_url" {
  description = "GCS URL for the raw data bucket"
  value       = google_storage_bucket.ipeds_raw.url
}

output "staging_dataset_id" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "mart_dataset_id" {
  description = "BigQuery mart dataset ID"
  value       = google_bigquery_dataset.mart.dataset_id
}

output "service_account_roles" {
  description = "IAM roles assigned to the service account"
  value = [
    google_project_iam_member.storage_admin.role,
    google_project_iam_member.bigquery_admin.role,
    google_project_iam_member.bigquery_job_user.role,
  ]
}
