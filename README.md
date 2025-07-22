# Windsurf IPEDS Pipeline

Copyright (c) 2025 Blue Sky Mind LLC. All Rights Reserved.

## Overview

This is a proprietary, reusable ingestion pipeline for downloading and processing IPEDS (Integrated Postsecondary Education Data System) data into Google BigQuery. The pipeline is designed to run once per year to ingest the latest IPEDS Access Database files.

## Project Status

**PRIVATE PROJECT** - This software is proprietary to Blue Sky Mind LLC and is not for public distribution.

## Architecture Principles

- **Single-responsibility components**: Each module handles one specific task
- **Configuration-driven**: All parameters in `config.ini`, secrets in `.env`
- **Container-first**: Packaged as a Docker image with Python 3.12 and mdbtools
- **Type-safe**: Full type hints and Pydantic validation
- **Testable**: Comprehensive unit test coverage

## Quick Start

### Prerequisites

- Python 3.12+
- Poetry (for dependency management)
- Docker (for containerized deployment)
- Google Cloud service account with BigQuery permissions

### Setup

1. Clone this repository (internal access only)
2. Copy `.env.example` to `.env` and fill in your credentials
3. Install dependencies:
   ```bash
   poetry install
   ```

### Configuration

Edit `config/config.ini` to set:
- GCS bucket names
- BigQuery dataset names
- IPEDS data source URLs

### Running Tests

```bash
poetry run pytest
```

## Components

- **ConfigManager**: Handles all configuration and environment variables
- **Downloader** (Sprint 2): Downloads IPEDS MDB files from NCES
- **Extractor** (Sprint 2): Extracts tables from MDB using mdbtools
- **BQLoader** (Sprint 3): Loads data into BigQuery staging tables
- **DBTRunner** (Sprint 3): Transforms staging data into analytics-ready marts

## License

This is proprietary software. See LICENSE file for details.

---

For internal use by Blue Sky Mind LLC only.
