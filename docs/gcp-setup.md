# Google Cloud Platform Setup Guide

Copyright (c) 2025 Blue Sky Mind LLC. All Rights Reserved.

## Overview

This guide walks through setting up the required Google Cloud infrastructure for the IPEDS Pipeline.

## Manual Setup Steps (One-time)

### 1. Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Click "Create Project"
3. Enter project details:
   - Project Name: `GlidrU IPEDS Pipeline`
   - Project ID: Note this down (e.g., `glidru-ipeds-123456`)
4. Select billing account or create one

### 2. Enable Required APIs

Open Cloud Shell or use your local `gcloud` CLI:

```bash
# Set your project
gcloud config set project YOUR-PROJECT-ID

# Enable required APIs
gcloud services enable storage.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
```

### 3. Create Service Account

```bash
# Create service account
gcloud iam service-accounts create ipeds-pipeline-sa \
    --display-name="IPEDS Pipeline Service Account" \
    --description="Service account for IPEDS data ingestion pipeline"

# Download service account key
gcloud iam service-accounts keys create ~/ipeds-pipeline-key.json \
    --iam-account=ipeds-pipeline-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com

# IMPORTANT: Store this key file securely!
```

### 4. Set Up Local Environment

```bash
# Copy the service account key to your project
cp ~/ipeds-pipeline-key.json /path/to/secure/location/

# Update your .env file
echo "GOOGLE_APPLICATION_CREDENTIALS=/path/to/secure/location/ipeds-pipeline-key.json" >> .env
```

## Terraform Infrastructure Setup

### 1. Install Terraform

```bash
# macOS
brew install terraform

# Ubuntu/Debian
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install terraform
```

### 2. Configure Terraform Variables

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars with your values:
# - project_id: Your GCP project ID
# - service_account_email: ipeds-pipeline-sa@YOUR-PROJECT-ID.iam.gserviceaccount.com
```

### 3. Deploy Infrastructure

```bash
# Initialize Terraform
terraform init

# Review planned changes
terraform plan

# Apply infrastructure
terraform apply

# Save the outputs
terraform output > infrastructure-details.txt
```

## What Gets Created

### By Terraform:
- **GCS Bucket**: `{project-id}-ipeds-raw`
  - Versioning enabled
  - 1-year lifecycle policy for automatic cleanup
  - Uniform bucket-level access

- **BigQuery Datasets**:
  - `ipeds_staging`: 90-day table expiration
  - `ipeds_mart`: No expiration

- **IAM Permissions**:
  - Storage Admin (for GCS operations)
  - BigQuery Admin (for dataset/table management)
  - BigQuery Job User (for running queries)

### Resource Naming Convention:
- Buckets: `{project-id}-ipeds-{purpose}`
- Datasets: `ipeds_{layer}`
- Labels: `project=ipeds-pipeline`, `owner=blue-sky-mind`

## Verification Steps

1. **Check GCS Bucket**:
   ```bash
   gsutil ls gs://YOUR-PROJECT-ID-ipeds-raw/
   ```

2. **Check BigQuery Datasets**:
   ```bash
   bq ls --project_id=YOUR-PROJECT-ID
   ```

3. **Test Service Account**:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/ipeds-pipeline-key.json
   python -c "from google.cloud import storage; print('GCS OK')"
   python -c "from google.cloud import bigquery; print('BQ OK')"
   ```

## Update config.ini

After Terraform completes, update your `config/config.ini`:

```ini
[paths]
raw_bucket = gs://YOUR-PROJECT-ID-ipeds-raw
staging_dataset = ipeds_staging
mart_dataset = ipeds_mart
```

## Cost Considerations

- **Storage**: ~$0.02/GB/month for standard storage
- **BigQuery**: 
  - Storage: $0.02/GB/month
  - Queries: First 1TB/month free, then $5/TB
- **Estimated monthly cost**: <$10 for typical IPEDS data volumes

## Security Best Practices

1. **Never commit** service account keys to Git
2. **Rotate keys** quarterly
3. **Use least privilege** - only grant necessary permissions
4. **Enable audit logs** for compliance tracking
5. **Store keys encrypted** at rest

## Troubleshooting

### "Permission Denied" Errors
- Verify service account has correct roles
- Check `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Ensure APIs are enabled

### Terraform State
- Store state file securely (consider GCS backend for team collaboration)
- Never commit `terraform.tfstate` to Git

## Next Steps

1. Run `poetry install` in project root
2. Run tests: `poetry run pytest`
3. Proceed with pipeline development
