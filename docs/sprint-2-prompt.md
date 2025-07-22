# GlidrU IPEDS Pipeline - Sprint 2: Downloader & Extractor Implementation

## Project Context

You are continuing work on the GlidrU IPEDS Pipeline, a production-grade data ingestion system that downloads IPEDS (Integrated Postsecondary Education Data System) Access Database files from NCES and loads them into Google BigQuery for analytics.

**Project Owner**: Blue Sky Mind LLC (Commercial, All Rights Reserved)  
**Status**: Private repository, not for publication  
**Technology Stack**: Python 3.12, Poetry, Docker, Google Cloud Platform (GCS, BigQuery)

## Sprint 1 Completion Summary

The following has been completed and tested:

### Infrastructure
- **GCP Project**: `glidru-ipeds-pipeline` 
- **Service Account**: `ipeds-pipeline-sa@glidru-ipeds-pipeline.iam.gserviceaccount.com`
- **GCS Bucket**: `gs://glidru-ipeds-pipeline-ipeds-raw/`
- **BigQuery Datasets**: 
  - `ipeds_staging` (90-day table expiration)
  - `ipeds_mart` (no expiration)
- **Region**: `us-east4`
- **Authentication**: Service account JSON key at `/home/lcl/.gcp/ipeds-pipeline-key.json`

### Codebase Structure
```
glidrU_IPEDS_ingestion_pipeline/
├── config/
│   ├── config.ini          # Non-secret configuration
│   └── .env               # Secret configuration (gitignored)
├── src/
│   ├── __init__.py
│   └── core/
│       ├── __init__.py
│       └── config_manager.py  # Configuration management (tested, 86% coverage)
├── tests/
│   └── test_config_manager.py  # All 11 tests passing
├── terraform/              # GCP infrastructure as code
├── docs/
│   └── gcp-setup.md       # Manual setup instructions
├── Dockerfile             # Python 3.12-slim + mdbtools
├── pyproject.toml         # Poetry configuration
└── README.md
```

### Key Design Decisions from Sprint 1
1. **Configuration Pattern**: Dual-file approach (config.ini for non-secrets, .env for secrets)
2. **No Hardcoded Values**: Everything configurable
3. **Single Responsibility**: Each class has one clear purpose
4. **Object-Oriented**: Use classes and inheritance appropriately
5. **Testable**: Dependency injection, mocking-friendly design
6. **Container-First**: Designed to run in Docker
7. **Type Hints**: Full type annotations throughout

## Sprint 2 Objectives

Implement two core components:

### 1. Downloader Component
Download IPEDS Access Database files from NCES and store in GCS.

### 2. Extractor Component  
Extract tables from Access Database (.mdb) files and convert to Parquet format.

## Detailed Requirements

### Downloader Component (`src/downloaders/ipeds_downloader.py`)

**Purpose**: Download IPEDS database files from NCES website

**Key Features**:
1. Download complete Access database files for specified academic years
2. Support both provisional and final release data
3. Implement retry logic with exponential backoff
4. Track download metadata (timestamp, file size, checksum)
5. Upload to GCS bucket with proper naming convention
6. Resume capability for interrupted downloads
7. Validate downloaded files

**IPEDS Data Source Information**:
- Base URL: `https://nces.ed.gov/ipeds/datacenter/data/`
- File naming pattern: `IPEDS{YEAR}{SUFFIX}.zip` where:
  - YEAR: Academic year (e.g., 2023)
  - SUFFIX: '_rv' for revised, '_pv' for provisional, or empty for final
- Example: `IPEDS2023.zip` (final 2023 data)
- Files contain Microsoft Access databases (.accdb or .mdb)

**Implementation Guidelines**:
```python
from abc import ABC, abstractmethod
from typing import Optional, List, Dict
import requests
from google.cloud import storage
from pydantic import BaseModel
from datetime import datetime
import hashlib

class DownloadMetadata(BaseModel):
    """Metadata for a downloaded file."""
    filename: str
    source_url: str
    download_timestamp: datetime
    file_size_bytes: int
    md5_checksum: str
    academic_year: int
    data_version: str  # 'final', 'provisional', 'revised'
    gcs_path: str

class BaseDownloader(ABC):
    """Abstract base class for all downloaders."""
    
    @abstractmethod
    def download(self, year: int, version: str = 'final') -> DownloadMetadata:
        """Download data for specified year and version."""
        pass
    
    @abstractmethod
    def list_available(self) -> List[Dict[str, any]]:
        """List available datasets to download."""
        pass

class IPEDSDownloader(BaseDownloader):
    """Downloads IPEDS Access database files from NCES."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(
            self.config.config.paths.raw_bucket.replace('gs://', '')
        )
    
    # Implement download logic with:
    # - Progress tracking
    # - Retry on failure (3 attempts with exponential backoff)
    # - Checksum validation
    # - Atomic uploads (use temporary name, then rename)
    # - Logging at INFO level for major steps, DEBUG for details
```

**GCS Organization**:
```
gs://glidru-ipeds-pipeline-ipeds-raw/
├── downloads/
│   ├── 2023/
│   │   ├── IPEDS2023.zip
│   │   └── metadata.json
│   ├── 2022/
│   │   ├── IPEDS2022.zip
│   │   └── metadata.json
│   └── ...
```

### Extractor Component (`src/extractors/mdb_extractor.py`)

**Purpose**: Extract tables from Access Database files to Parquet format

**Key Features**:
1. Use mdbtools to list and extract tables from .mdb/.accdb files
2. Convert each table to Parquet format
3. Infer schema and handle data types appropriately
4. Generate extraction metadata
5. Handle large tables efficiently (streaming/chunking)
6. Support table filtering (include/exclude patterns)

**Implementation Guidelines**:
```python
import subprocess
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional, Dict
import tempfile
import json

class ExtractionMetadata(BaseModel):
    """Metadata for an extracted table."""
    source_file: str
    table_name: str
    extraction_timestamp: datetime
    row_count: int
    column_count: int
    columns: List[Dict[str, str]]  # name, type
    parquet_size_bytes: int
    gcs_path: str

class MDBExtractor:
    """Extracts tables from Microsoft Access databases using mdbtools."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self._validate_mdbtools()
    
    def _validate_mdbtools(self):
        """Ensure mdbtools is installed and working."""
        try:
            subprocess.run(['mdb-ver'], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("mdbtools not found. Please install mdbtools.")
    
    def list_tables(self, mdb_path: Path) -> List[str]:
        """List all tables in the MDB file."""
        # Use: mdb-tables -1 {file}
        pass
    
    def extract_table(self, 
                     mdb_path: Path, 
                     table_name: str,
                     output_path: Optional[Path] = None) -> ExtractionMetadata:
        """Extract a single table to Parquet format."""
        # Steps:
        # 1. Use mdb-export to get CSV data
        # 2. Read CSV into pandas DataFrame
        # 3. Infer and fix data types
        # 4. Save as Parquet
        # 5. Generate metadata
        pass
    
    def extract_all_tables(self, 
                          mdb_path: Path,
                          include_pattern: Optional[str] = None,
                          exclude_pattern: Optional[str] = None) -> List[ExtractionMetadata]:
        """Extract all tables matching criteria."""
        pass
```

**Parquet Schema Considerations**:
- Use nullable types by default
- Convert Access date/time to proper timestamps
- Handle Access currency type → decimal
- Boolean fields → bool
- Text fields → string (with proper encoding)
- Memo fields → string (large)

**GCS Organization for Extracted Data**:
```
gs://glidru-ipeds-pipeline-ipeds-raw/
├── extracted/
│   ├── 2023/
│   │   ├── tables/
│   │   │   ├── HD2023.parquet
│   │   │   ├── IC2023.parquet
│   │   │   └── ...
│   │   └── metadata/
│   │       └── extraction_manifest.json
│   └── ...
```

## Testing Requirements

### Unit Tests
Create comprehensive test files:
- `tests/test_ipeds_downloader.py`
- `tests/test_mdb_extractor.py`

Test Coverage Goals:
- Minimum 80% code coverage
- Mock all external dependencies (requests, GCS, subprocess)
- Test error handling and edge cases
- Validate retry logic
- Test metadata generation

### Integration Tests (Optional for Sprint 2)
- `tests/integration/test_downloader_integration.py`
- `tests/integration/test_extractor_integration.py`

## CLI Commands to Implement

Using Typer, add these commands to a new `src/cli.py`:

```python
import typer
from typing import Optional

app = typer.Typer()

@app.command()
def download(
    year: int = typer.Argument(..., help="Academic year to download"),
    version: str = typer.Option("final", help="Data version: final, provisional, revised"),
    force: bool = typer.Option(False, help="Force re-download if file exists")
):
    """Download IPEDS data for a specific year."""
    pass

@app.command()
def extract(
    file_path: str = typer.Argument(..., help="Path to MDB file in GCS"),
    table: Optional[str] = typer.Option(None, help="Specific table to extract"),
    output_dir: Optional[str] = typer.Option(None, help="Output directory in GCS")
):
    """Extract tables from an IPEDS Access database."""
    pass

@app.command()
def list_tables(
    file_path: str = typer.Argument(..., help="Path to MDB file")
):
    """List all tables in an IPEDS Access database."""
    pass
```

## Security and Best Practices

1. **No Credentials in Code**: Use ConfigManager for all secrets
2. **Atomic Operations**: Use temporary files/names, then rename
3. **Idempotency**: Operations should be safe to retry
4. **Comprehensive Logging**: Use Python logging module
5. **Error Messages**: Clear, actionable error messages
6. **Resource Cleanup**: Always clean up temporary files
7. **Memory Efficiency**: Stream large files, don't load entirely into memory

## Development Workflow

1. Create feature branch: `git checkout -b feature/sprint-2-downloader-extractor`
2. Implement components with TDD approach
3. Run tests: `poetry run pytest`
4. Run type checking: `poetry run mypy src/`
5. Run linting: `poetry run ruff check src/`
6. Format code: `poetry run black src/`

## Docker Considerations

The Dockerfile already includes mdbtools. Ensure any new system dependencies are added:
```dockerfile
RUN apt-get update && apt-get install -y \
    mdbtools \
    # Add any new dependencies here
    && rm -rf /var/lib/apt/lists/*
```

## Configuration Updates

Add any new configuration to `config/config.ini`:
```ini
[downloader]
retry_attempts = 3
retry_delay_seconds = 5
chunk_size_mb = 10
timeout_seconds = 300

[extractor]
max_table_size_gb = 5
default_compression = snappy
```

## Success Criteria for Sprint 2

1. ✅ Can download IPEDS data files for any available year
2. ✅ Downloads are resumable and validated
3. ✅ Can list all tables in an MDB file
4. ✅ Can extract specific tables or all tables to Parquet
5. ✅ All data and metadata stored in GCS with proper structure
6. ✅ Comprehensive unit tests with >80% coverage
7. ✅ CLI commands working for all operations
8. ✅ Proper error handling and logging throughout
9. ✅ Code passes type checking and linting

## Next Sprint Preview (Sprint 3)

After Sprint 2, the next phase will implement:
- **Loader**: Load Parquet files into BigQuery staging tables
- **Transformer**: Create mart layer with business logic
- **Orchestrator**: Coordinate the full pipeline execution
- **Monitoring**: Add observability and alerting

## Questions to Consider

1. Should we implement parallel downloads for multiple years?
2. Do we need to support incremental updates?
3. Should we add data quality checks during extraction?
4. What's the retention policy for raw downloaded files?

---

*Note: This project is proprietary to Blue Sky Mind LLC. Ensure all code includes proper copyright headers.*
