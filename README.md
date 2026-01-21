# Recipe Data Engineering Pipeline (Python, Prefect, BigQuery, dbt)


## Overview

This project implements a full data engineering pipeline that ingests recipe data from public APIs, normalizes and transforms it, stores it in a relational database and a cloud data warehouse, and models it using dbt.

The pipeline is orchestrated with Prefect, supports caching and incremental processing, and produces analytics-ready tables in BigQuery.


## Tech Stack

- Python (ETL)
- Prefect (orchestration)
- Pandas / PyArrow (parquet)
- MySQL (OLTP store)
- Google Cloud Storage
- BigQuery (warehouse)
- dbt (modeling & tests)


## Set up (local)

### Create Docker Container for MySQL

#### Create .env file in project root and enter database connection details

```
DATABASE_URL=mysql+pymysql://root:<PASSWORD>@127.0.0.1:<PORT>/<DATABASE_NAME>?charset=utf8mb4
SPOONACULAR_API_KEY=<KEY>
```

### Start the Python environment

cd to the project root

```python
python3 -m venv .venv
.venv\Scripts\Activate.ps1
```


## Data Quality Checks

### 1. Data Quality Module (`src/quality/data_quality.py`)
A comprehensive data quality checking system with:

- **QualityCheckResult**: Dataclass to store check results
- **DataQualityChecker**: Main class with multiple check types:
  - `check_not_null()`: Validates non-null value proportions
  - `check_unique()`: Checks for duplicate values
  - `check_value_range()`: Validates numeric ranges
  - `check_record_count()`: Ensures acceptable record counts
  - `check_recipe_data_quality()`: Recipe-specific comprehensive checks

**Key Features:**
- Configurable thresholds for each check
- Detailed logging of all check results
- Ability to raise errors on quality failures
- Summary reporting

### 2. ETL Metadata Tracking (`src/utils/etl_metadata.py`)
Tracks ETL run metadata including:

- Run timestamps (extract, transform, load)
- Record counts at each stage
- File paths (raw and processed)
- Quality check results
- Error messages for failed runs
- Run status (running, completed, failed)

**Database Table:** `etl_runs` - Stores all ETL run metadata for historical tracking


## How to Use

### Basic Usage (with quality checks)
```python
from src.prefect_flows.recipe_flow import recipe_etl_flow

# Run ETL with quality validation (default)
recipe_etl_flow()
```

### Disable Quality Checks (if needed)
```python
recipe_etl_flow(validate_quality=False)
```

### Check ETL Run Status
```python
from src.utils.etl_metadata import get_latest_run_status

status = get_latest_run_status()
print(status)
```

### Manual Quality Check
```python
import pandas as pd
from src.quality.data_quality import DataQualityChecker

df = pd.read_parquet("data/processed/recipes.parquet")
checker = DataQualityChecker()
checker.check_recipe_data_quality(df)
checker.log_summary()

# Get summary as dict
summary = checker.get_summary()
```

## Set up for Cloud / Warehouse 
(optional - program run separately to local run, but local run must be done first to create parquet file)

### Register for GCP (Google Storage and BigQuery)
### Create BUCKET "recipe-etl-bucket" on GCS, and DATASET "recipe_dw", TABLE "recipes" on BigQuery
### Run load_to_bigquery.py (src/load)