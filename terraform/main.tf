# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# GCS Bucket for raw IPEDS data
resource "google_storage_bucket" "ipeds_raw" {
  name          = "${var.project_id}-ipeds-raw"
  location      = var.location
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 365  # Keep raw files for 1 year
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }

  labels = var.labels
}

# BigQuery datasets
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "ipeds_staging"
  friendly_name = "IPEDS Staging Tables"
  description   = "Staging area for raw IPEDS data before transformation"
  location      = var.location

  default_table_expiration_ms = 7776000000  # 90 days in milliseconds

  labels = var.labels

  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
}

resource "google_bigquery_dataset" "mart" {
  dataset_id    = "ipeds_mart"
  friendly_name = "IPEDS Data Mart"
  description   = "Analytics-ready IPEDS data models"
  location      = var.location

  # No expiration for mart tables
  default_table_expiration_ms = null

  labels = var.labels

  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }
}

# IAM permissions for service account
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${var.service_account_email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.service_account_email}"
}
