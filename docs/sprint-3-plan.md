# GlidrU IPEDS Pipeline - Sprint 3: Implementation Plan

## Design Decisions (Confirmed)

1. **Table Structure**: Year-agnostic staging tables with an added `year` column
2. **DBT Complexity**: Start with simple transformations, iterate later
3. **Orchestration**: Keep load and transform as separate manual steps
4. **Validation**: Defer validation rules until after first data analysis
5. **API Choice**: Use BigQuery Storage Write API for better performance

## Implementation Plan

### Phase 1: BigQuery Loader (Week 1)

#### 1.1 Create Load Models (`src/loaders/load_models.py`)
```python
class LoadMetadata(BaseModel):
    table_name: str
    year: int
    source_file: str
    rows_loaded: int
    load_timestamp: datetime
    load_duration_seconds: float
    
class LoadManifest(BaseModel):
    year: int
    tables_loaded: List[LoadMetadata]
    total_rows: int
    total_duration_seconds: float
    manifest_path: Optional[str]
```

#### 1.2 Implement BQLoader (`src/loaders/bq_loader.py`)
- Use BigQuery Storage Write API for Parquet loading
- Add `year` column during load process
- Support table creation with schema inference
- Generate comprehensive load metadata

**Key Methods:**
```python
def load_single_table(
    self,
    parquet_path: str,
    year: int,
    table_name: str
) -> LoadMetadata

def load_from_manifest(
    self,
    manifest_path: str,
    year: int
) -> LoadManifest
```

#### 1.3 CLI Command: `load`
```bash
# Load all extracted tables for a year
glidru-ipeds load --year 2023

# Load specific table
glidru-ipeds load --year 2023 --table hd

# Load from specific manifest
glidru-ipeds load --manifest gs://bucket/manifest.json
```

### Phase 2: DBT Starter Project (Week 2)

#### 2.1 DBT Project Structure
```
dbt/
├── dbt_project.yml
├── profiles.yml.template
├── models/
│   ├── staging/
│   │   ├── _staging.yml
│   │   ├── stg_ipeds__hd.sql      # Institution headers
│   │   ├── stg_ipeds__ic.sql      # Institutional characteristics
│   │   └── stg_ipeds__sfa.sql     # Student financial aid
│   └── marts/
│       ├── _marts.yml
│       ├── dim_institution.sql     # Simple institution dimension
│       └── fact_cost_aid.sql       # Basic cost/aid facts
└── macros/
    └── get_latest_year.sql
```

#### 2.2 Simple Initial Models

**dim_institution.sql** (simple version):
```sql
WITH latest_hd AS (
    SELECT *
    FROM {{ ref('stg_ipeds__hd') }}
    WHERE year = {{ get_latest_year() }}
)
SELECT
    unitid,
    instnm AS institution_name,
    addr AS address,
    city,
    stabbr AS state,
    zip,
    year
FROM latest_hd
```

**fact_cost_aid.sql** (simple version):
```sql
WITH latest_sfa AS (
    SELECT *
    FROM {{ ref('stg_ipeds__sfa') }}
    WHERE year = {{ get_latest_year() }}
)
SELECT
    unitid,
    year,
    -- Add basic financial metrics
    -- Will expand after data analysis
FROM latest_sfa
```

#### 2.3 DBTRunner (`src/core/dbt_runner.py`)
```python
class DBTRunner:
    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.project_dir = Path(config.get("dbt.project_dir"))
        
    def run(self, select: Optional[str] = None) -> DBTRunResult
    def test(self, select: Optional[str] = None) -> DBTTestResult
    def compile(self) -> bool
```

#### 2.4 CLI Command: `transform`
```bash
# Run all models
glidru-ipeds transform

# Run specific model
glidru-ipeds transform --select dim_institution

# Full refresh
glidru-ipeds transform --full-refresh
```

### Phase 3: Testing & Documentation (Week 3)

#### 3.1 Unit Tests
- Mock BigQuery client and Storage Write API
- Test schema inference and year column addition
- Mock dbt subprocess calls
- Test error handling and retries

#### 3.2 Configuration Updates
```ini
[bigquery]
location = us-east4
write_disposition = WRITE_TRUNCATE
create_disposition = CREATE_IF_NEEDED
use_storage_api = true
batch_size = 10000

[dbt]
project_dir = dbt
profiles_dir = ~/.dbt
target = prod
threads = 4
```

#### 3.3 Dependencies
```toml
[tool.poetry.dependencies]
google-cloud-bigquery = "^3.13.0"
google-cloud-bigquery-storage = "^2.22.0"
dbt-bigquery = "^1.7.0"
pandas-gbq = "^0.19.0"  # For schema helpers
```

## Implementation Order

1. **Day 1-2**: Load models and BQLoader class foundation
2. **Day 3-4**: Storage Write API integration and year column logic
3. **Day 5**: Load CLI command and testing
4. **Day 6-7**: DBT project setup with simple models
5. **Day 8-9**: DBTRunner implementation
6. **Day 10**: Transform CLI command
7. **Day 11-12**: Unit tests and documentation
8. **Day 13-14**: Integration testing and polish

## Success Criteria

- [ ] Parquet files load successfully to BigQuery with year column
- [ ] Storage Write API provides performance improvement
- [ ] DBT models compile and run without errors
- [ ] CLI commands work intuitively
- [ ] Unit test coverage > 80%
- [ ] Documentation updated with new features
- [ ] No hardcoded values (all config-driven)

## Notes

- Validation command deferred to Sprint 4 (after data analysis)
- Complex DBT transformations deferred to future sprints
- Focus on reliability and clean interfaces over features
