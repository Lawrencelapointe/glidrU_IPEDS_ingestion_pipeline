# GlidrU IPEDS Pipeline

Copyright (c) 2025 Blue Sky Mind LLC. All Rights Reserved.

## Overview

This is a proprietary ingestion pipeline for downloading and processing IPEDS (Integrated Postsecondary Education Data System) data into Google BigQuery. The pipeline is a critical component of **GlidrU** – the first AI-augmented college-selection platform helping high-school student-athletes discover universities that match their athletic, academic, and financial goals.

## Project Status

**PRIVATE PROJECT** - This software is proprietary to Blue Sky Mind LLC and is not for public distribution.

**Current Sprint**: Sprint 3 - BigQuery Loading & DBT Transformations

## GlidrU Mission

GlidrU aims to build an AI-powered platform that:
- **Curates** ~4,000 US colleges down to relevant subsets per user (sport, division, geography, budget)
- **Leverages GenAI** to gather hard-to-scrape details (coach philosophy, walk-on culture, program ethos)
- **Provides data-driven recommendations** using ML scoring algorithms

Initial sports focus: Soccer, Volleyball, Baseball/Softball, Lacrosse, and Golf

## Architecture

### Design Principles
- **Single-responsibility components**: Each module handles one specific task
- **Configuration-driven**: All parameters in `config.ini`, secrets in `.env`
- **Container-first**: Packaged as a Docker image with Python 3.12 and mdbtools
- **Type-safe**: Full type hints and Pydantic validation
- **Testable**: Comprehensive unit test coverage

### Data Architecture
- **Source**: IPEDS Access Database files (annual releases)
- **Storage**: Google Cloud Storage for raw files
- **Warehouse**: BigQuery with year-agnostic staging tables
- **Transformation**: DBT for creating analytics-ready marts
- **Loading**: BigQuery Storage Write API for optimal performance

## Quick Start

### Prerequisites

- Python 3.12+
- Poetry (for dependency management)
- Docker (for containerized deployment)
- Google Cloud Project with billing enabled
- Terraform (for infrastructure provisioning)

### Setup

1. **Clone this repository** (internal access only)

2. **Set up Google Cloud infrastructure**:
   - Follow the detailed guide in `docs/gcp-setup.md`
   - This includes manual GCP project setup and Terraform deployment

3. **Configure local environment**:
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Add your service account key path to .env
   # GOOGLE_APPLICATION_CREDENTIALS=/path/to/ipeds-pipeline-key.json
   ```

4. **Install dependencies**:
   ```bash
   poetry install
   ```

5. **Update configuration**:
   - Edit `config/config.ini` with your GCP project details
   - Set bucket names, dataset names, and IPEDS URLs

### Running the Pipeline

```bash
# Download IPEDS data (Sprint 2)
glidru-ipeds download --year 2023

# Extract tables from MDB files (Sprint 2)
glidru-ipeds extract --year 2023

# Load to BigQuery (Sprint 3)
glidru-ipeds load --year 2023

# Run DBT transformations (Sprint 3)
glidru-ipeds transform
```

### Running Tests

```bash
poetry run pytest
```

## Components

### Core Components (Sprint 1)
- **ConfigManager**: Centralized configuration and environment management
- **Models**: Pydantic models for type safety and validation

### Data Ingestion (Sprint 2 - Completed)
- **Downloader**: Downloads IPEDS MDB files from NCES
  - Supports provisional and final releases
  - Automatic retries and progress tracking
- **Extractor**: Extracts tables from MDB using mdbtools
  - Converts to Parquet format for efficient storage
  - Generates extraction manifests

### Data Loading & Transformation (Sprint 3 - In Progress)
- **BQLoader**: Loads Parquet files to BigQuery
  - Uses Storage Write API for performance
  - Adds year column to support multi-year analysis
  - Generates load manifests
- **DBTRunner**: Orchestrates DBT transformations
  - Creates staging models from raw tables
  - Builds initial marts (dim_institution, fact_cost_aid)

## Project Structure

```
glidru_ipeds_pipeline/
├── config/              # Configuration files
│   └── config.ini      # Main configuration
├── src/                # Source code
│   ├── __init__.py
│   ├── cli.py         # CLI entry point
│   ├── core/          # Core utilities
│   ├── downloaders/   # IPEDS downloaders
│   ├── extractors/    # MDB extractors
│   ├── loaders/       # BigQuery loaders
│   └── models/        # Pydantic models
├── dbt/               # DBT project (Sprint 3)
│   ├── models/
│   │   ├── staging/   # Staging transformations
│   │   └── marts/     # Analytics-ready tables
│   └── macros/        # DBT macros
├── terraform/         # Infrastructure as Code
├── docs/              # Documentation
│   ├── glidrU_background.md  # Platform vision
│   ├── gcp-setup.md          # GCP setup guide
│   └── sprint-*.md           # Sprint plans
├── tests/             # Unit tests
├── Dockerfile         # Container definition
├── poetry.lock        # Locked dependencies
└── pyproject.toml     # Project metadata
```

## Development Roadmap

- [x] **Sprint 1**: Core infrastructure (ConfigManager, models, testing)
- [x] **Sprint 2**: IPEDS download and extraction pipeline
- [ ] **Sprint 3**: BigQuery loading and DBT transformations (Current)
- [ ] **Sprint 4**: Data quality and monitoring
- [ ] **Sprint 5**: Orchestration and automation

## Infrastructure

The pipeline runs on Google Cloud Platform:
- **Storage**: GCS bucket for raw IPEDS files
- **Data Warehouse**: BigQuery datasets (staging & marts)
- **Compute**: Local execution or containerized deployment
- **Infrastructure**: Managed via Terraform (see `terraform/`)

## License

This is proprietary software. See LICENSE file for details.

---

For internal use by Blue Sky Mind LLC only.
