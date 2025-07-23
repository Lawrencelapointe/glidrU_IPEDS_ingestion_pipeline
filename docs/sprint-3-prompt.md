# GlidrU IPEDS Pipeline - Sprint 3: BigQuery Loader & DBT Implementation

## Project Context

You are continuing work on the GlidrU IPEDS Pipeline, a production-grade data ingestion system that downloads IPEDS (Integrated Postsecondary Education Data System) Access Database files from NCES and loads them into Google BigQuery for analytics.

**Project Owner**: Blue Sky Mind LLC (Commercial, All Rights Reserved)  
**Status**: Private repository, not for publication  
**Technology Stack**: Python 3.12, Poetry, Docker, Google Cloud Platform (GCS, BigQuery), DBT

## Critical Design Decisions (From Sprint 1)

These principles must be maintained throughout all implementations:

1. **Configuration Pattern**: Dual-file approach (config.ini for non-secrets, .env for secrets)
2. **No Hardcoded Values**: Everything configurable
3. **Single Responsibility**: Each class has one clear purpose
4. **Object-Oriented**: Use classes and inheritance appropriately
5. **Testable**: Dependency injection, mocking-friendly design
6. **Container-First**: Designed to run in Docker
7. **Type Hints**: Full type annotations throughout

## Sprint 1 & 2 Completion Summary

### Infrastructure (Sprint 1)
- **GCP Project**: `glidru-ipeds-pipeline` 
- **Service Account**: `ipeds-pipeline-sa@glidru-ipeds-pipeline.iam.gserviceaccount.com`
- **GCS Bucket**: `gs://glidru-ipeds-pipeline-ipeds-raw/`
- **BigQuery Datasets**: 
  - `ipeds_staging` (90-day table expiration)
  - `ipeds_mart` (no expiration)
- **Region**: `us-east4`
- **Authentication**: Service account JSON key at `/home/lcl/.gcp/ipeds-pipeline-key.json`

### Implemented Components (Sprint 1 & 2)
```
glidrU_IPEDS_ingestion_pipeline/
├── config/
│   ├── config.ini          # Non-secret configuration
│   └── .env               # Secret configuration (gitignored)
├── src/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   └── config_manager.py  # Configuration management (tested)
│   ├── downloaders/
│   │   ├── __init__.py
│   │   ├── base_downloader.py
│   │   └── ipeds_downloader.py  # IPEDS file downloader
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── base_extractor.py
│   │   └── mdb_extractor.py    # MDB to Parquet converter
│   └── cli.py                  # CLI with download/extract commands
├── tests/
│   ├── test_config_manager.py
│   ├── test_ipeds_downloader.py
│   └── test_mdb_extractor.py
├── terraform/              # GCP infrastructure as code
├── Dockerfile             # Python 3.12-slim + mdbtools
├── pyproject.toml         # Poetry configuration
└── README.md
```

## Sprint 3 Objectives

Implement the loading and transformation layer:

### 1. BigQuery Loader Component
Load Parquet files into BigQuery staging tables with year-agnostic design (latest year only).

### 2. DBT Starter Project  
Create initial dimensional models with simple transformations.

## Detailed Requirements

### BigQuery Loader Component (`src/loaders/bq_loader.py`)

**Purpose**: Load extracted Parquet files into BigQuery staging tables

**Key Features**:
1. Use BigQuery Storage Write API for performance
2. Add `year` column during load (for latest year only)
3. Support schema inference from Parquet files
4. Handle table creation or replacement with proper data types
5. Generate comprehensive load metadata
6. Atomic operations with rollback capability
7. Batch loading for multiple tables from single year's data

**Implementation Guidelines**:
```python
from abc import ABC, abstractmethod
from typing import Optional, List, Dict
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.bigquery_storage import BigQueryWriteClient
from pydantic import BaseModel
from datetime import datetime
import pandas as pd

class LoadMetadata(BaseModel):
    """Metadata for a loaded table."""
    table_name: str
    year: int
    source_file: str
    rows_loaded: int
    load_timestamp: datetime
    load_duration_seconds: float
    
class LoadManifest(BaseModel):
    """Manifest for a complete load operation."""
    year: int
    tables_loaded: List[LoadMetadata]
    total_rows: int
    total_duration_seconds: float
    manifest_path: Optional[str] = None

class BaseLoader(ABC):
    """Abstract base class for data loaders."""
    
    @abstractmethod
    def load_single_table(self, source_path: str, table_name: str, **kwargs) -> LoadMetadata:
        """Load a single table."""
        pass
    
    @abstractmethod
    def load_from_manifest(self, manifest_path: str, **kwargs) -> LoadManifest:
        """Load multiple tables from a manifest."""
        pass

class BQLoader(BaseLoader):
    """Loads Parquet files into BigQuery using Storage Write API."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.project_id = config.get("gcp.project_id")
        self.dataset_id = config.get("bigquery.staging_dataset")
        self.location = config.get("bigquery.location", "us-east4")
        self.use_storage_api = config.get("bigquery.use_storage_api", "true").lower() == "true"
        
    def load_single_table(self, 
                         parquet_path: str,
                         year: int,
                         table_name: str,
                         write_disposition: str = "WRITE_TRUNCATE") -> LoadMetadata:
        """
        Load a single Parquet file to BigQuery.
        
        Steps:
        1. Read Parquet schema
        2. Add year column to DataFrame
        3. Create/replace table (WRITE_TRUNCATE for latest data only)
        4. Use Storage Write API for loading
        5. Generate metadata
        """
        pass
    
    def load_from_manifest(self,
                          manifest_path: str,
                          year: int,
                          parallel: bool = False) -> LoadManifest:
        """Load all tables listed in extraction manifest."""
        pass
```

**Table Naming Convention**:
- Staging tables: `ipeds_staging.{table_name}` (e.g., `ipeds_staging.hd`)
- No year suffix - year is a column in the table (contains only latest year)
- All lowercase table names
- Tables are fully replaced on each load (no append/historical data)

### DBT Starter Project

**Directory Structure**:
```
dbt/
├── dbt_project.yml
├── profiles.yml.template
├── models/
│   ├── staging/
│   │   ├── _staging.yml          # Documentation and tests
│   │   ├── stg_ipeds__hd.sql    # Institution headers
│   │   ├── stg_ipeds__ic.sql    # Institutional characteristics
│   │   └── stg_ipeds__sfa.sql   # Student financial aid
│   └── marts/
│       ├── _marts.yml            # Documentation and tests
│       ├── dim_institution.sql   # Institution dimension
│       └── fact_cost_aid.sql     # Cost and aid facts
├── macros/
│   └── get_latest_year.sql       # Helper macro
└── tests/
    └── generic/
        └── test_not_null_where.sql
```

**Initial Models (Keep Simple)**:

`models/staging/stg_ipeds__hd.sql`:
```sql
{{
    config(
        materialized='view'
    )
}}

SELECT
    unitid,
    instnm,
    addr,
    city,
    stabbr,
    zip,
    year,
    _loaded_at
FROM {{ source('ipeds_staging', 'hd') }}
```

`models/marts/dim_institution.sql`:
```sql
{{
    config(
        materialized='table'
    )
}}

-- Since we only load latest year, no need for year filtering
WITH latest_data AS (
    SELECT *
    FROM {{ ref('stg_ipeds__hd') }}
)
SELECT
    unitid AS institution_id,
    instnm AS institution_name,
    addr AS address,
    city,
    stabbr AS state,
    zip AS zip_code,
    year AS data_year,
    CURRENT_TIMESTAMP() AS created_at
FROM latest_data
```

`models/marts/fact_cost_aid.sql`:
```sql
{{
    config(
        materialized='table'
    )
}}

-- Simple fact table - will expand after data analysis
WITH latest_sfa AS (
    SELECT *
    FROM {{ ref('stg_ipeds__sfa') }}
)
SELECT
    unitid AS institution_id,
    year AS academic_year,
    -- Additional metrics to be added after data profiling
    CURRENT_TIMESTAMP() AS created_at
FROM latest_sfa
```

### DBT Runner Component (`src/core/dbt_runner.py`)

```python
import subprocess
from pathlib import Path
from typing import Optional, List, Dict
from pydantic import BaseModel

class DBTRunResult(BaseModel):
    """Result of a dbt run command."""
    success: bool
    models_run: int
    models_failed: int
    execution_time: float
    error_message: Optional[str] = None

class DBTRunner:
    """Executes dbt commands."""
    
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.project_dir = Path(config.get("dbt.project_dir"))
        self.profiles_dir = Path.home() / ".dbt"
        self.target = config.get("dbt.target", "prod")
        
    def run(self, 
            select: Optional[str] = None,
            full_refresh: bool = False,
            threads: Optional[int] = None) -> DBTRunResult:
        """Execute dbt run command."""
        pass
        
    def test(self, select: Optional[str] = None) -> DBTRunResult:
        """Execute dbt test command."""
        pass
        
    def compile(self) -> bool:
        """Compile dbt project to validate syntax."""
        pass
```

## CLI Commands to Implement

Add these commands to existing `src/cli.py`:

```python
@app.command()
def load(
    year: int = typer.Argument(..., help="Academic year to load"),
    table: Optional[str] = typer.Option(None, help="Specific table to load"),
    manifest: Optional[str] = typer.Option(None, help="Path to extraction manifest")
):
    """Load extracted Parquet files into BigQuery."""
    pass

@app.command()
def transform(
    select: Optional[str] = typer.Option(None, help="Select specific models"),
    full_refresh: bool = typer.Option(False, help="Full refresh mode"),
    test: bool = typer.Option(True, help="Run tests after transformation")
):
    """Run dbt transformations."""
    pass

@app.command()
def pipeline(
    year: int = typer.Argument(..., help="Academic year to process"),
    skip_download: bool = typer.Option(False, help="Skip download step"),
    skip_extract: bool = typer.Option(False, help="Skip extraction step"),
    skip_load: bool = typer.Option(False, help="Skip load step"),
    skip_transform: bool = typer.Option(False, help="Skip transform step")
):
    """Run complete pipeline for a year."""
    pass
```

## Configuration Updates

Add to `config/config.ini`:
```ini
[bigquery]
staging_dataset = ipeds_staging
mart_dataset = ipeds_mart
location = us-east4
write_disposition = WRITE_TRUNCATE
create_disposition = CREATE_IF_NEEDED
use_storage_api = true
batch_size = 10000

[dbt]
project_dir = dbt
target = prod
threads = 4
```

## Testing Requirements

### Unit Tests
- `tests/test_bq_loader.py` - Mock BigQuery client and Storage API
- `tests/test_dbt_runner.py` - Mock subprocess calls

### Test Coverage Goals
- Minimum 80% code coverage
- Mock all external dependencies
- Test error handling and edge cases
- Validate year column addition logic

## Dependencies to Add

Update `pyproject.toml`:
```toml
[tool.poetry.dependencies]
google-cloud-bigquery = "^3.13.0"
google-cloud-bigquery-storage = "^2.22.0"
dbt-bigquery = "^1.7.0"
pandas-gbq = "^0.19.0"
```

## Success Criteria for Sprint 3

1. ✅ Parquet files load successfully to BigQuery with year column
2. ✅ Storage Write API provides measurable performance improvement
3. ✅ Year-agnostic table design implemented correctly
4. ✅ DBT models compile and run without errors
5. ✅ Simple dimensional models created (dim_institution, fact_cost_aid)
6. ✅ CLI commands working for load and transform operations
7. ✅ Load and transform are separate, manual steps (not auto-triggered)
8. ✅ Comprehensive unit tests with >80% coverage
9. ✅ No hardcoded values - all configuration-driven
10. ✅ Code passes type checking and linting

## Development Workflow

1. Create feature branch: `git checkout -b feature/sprint-3-loader-dbt`
2. Implement BQLoader with Storage Write API
3. Create DBT project structure and initial models
4. Implement DBTRunner
5. Add CLI commands
6. Write comprehensive tests
7. Update documentation

## Important Notes

- **Manual Steps**: Load and transform remain separate manual operations
- **Simple Models**: DBT models should be minimal - complexity added after data analysis
- **Year Column**: All staging tables must include year column (no year-suffixed tables)
- **Storage API**: Use BigQuery Storage Write API for performance
- **No Validation**: Data validation rules deferred to future sprint

---

*Note: This project is proprietary to Blue Sky Mind LLC. Ensure all code includes proper copyright headers.*
