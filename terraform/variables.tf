# Copyright (c) 2025 Blue Sky Mind LLC
# All Rights Reserved.
# Proprietary and confidential.

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "credentials_file" {
  description = "Path to the service account key JSON file"
  type        = string
}

variable "region" {
  description = "Default region for resources"
  type        = string
  default     = "us-east4"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "service_account_email" {
  description = "Email of the service account created manually"
  type        = string
}

variable "labels" {
  description = "Default labels for all resources"
  type        = map(string)
  default = {
    project    = "ipeds-pipeline"
    owner      = "blue-sky-mind"
    managed-by = "terraform"
  }
}
