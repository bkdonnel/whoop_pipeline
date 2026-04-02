# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Python-based data extraction pipeline for the Whoop API. The project extracts user data from various Whoop v2 endpoints (Cycle, Sleep, Recovery, Workout, User) and loads them to Snowflake. Runs as a daily scheduled GitHub Actions workflow.

## Project Structure

```
whoop_pipeline/
├── .github/
│   └── workflows/
│       └── whoop_pipeline.yml    # GitHub Actions daily pipeline workflow
├── dags/                         # Legacy Airflow DAGs (no longer used)
├── include/
│   └── extract/                  # Python extraction modules
│       ├── whoop_extractor.py    # Main extraction script (Whoop API v2)
│       └── utils/
│           ├── auth.py           # OAuth2 authentication handler
│           ├── snowflake_client.py # Snowflake database integration (RSA key auth)
│           └── state_manager.py  # Pipeline state tracking
├── include/dbt/                  # dbt transformations
│   ├── models/
│   │   ├── staging/              # Staging models for raw data
│   │   │   ├── stg_sleep.sql
│   │   │   ├── stg_cycle.sql
│   │   │   ├── stg_workout.sql
│   │   │   ├── stg_recovery.sql
│   │   │   ├── stg_user.sql
│   │   │   └── schema.yml        # Source definitions pointing to raw_* tables
│   │   ├── intermediate/         # Intermediate analytics models
│   │   │   └── int_recovery_percentiles.sql
│   │   └── marts/                # Dimensional models for analytics
│   │       ├── dimensions/
│   │       │   ├── dim_date.sql  # Date dimension using Snowflake GENERATOR
│   │       │   └── dim_user.sql  # User dimension
│   │       ├── facts/
│   │       │   ├── fact_daily_recovery.sql
│   │       │   ├── fact_daily_strain.sql
│   │       │   └── seasonal_adaptation.sql
│   │       └── schema.yml
│   ├── macros/
│   │   ├── generate_staging_model.sql  # Automated staging model creation
│   │   └── get_custom_schema.sql       # Schema naming logic
│   ├── packages.yml              # dbt package dependencies (dbt-utils only)
│   ├── dbt_project.yml           # dbt configuration (no +schema overrides)
│   └── profiles.yml              # Snowflake connection profile
├── run_pipeline.py               # Main pipeline script for GitHub Actions
├── get_whoop_tokens.py           # Interactive OAuth flow to get fresh tokens
├── requirements-pipeline.txt    # Lightweight dependencies (no Airflow)
├── requirements.txt              # Full dependencies (legacy)
├── .env                          # Local environment variables (gitignored)
└── README.md                     # Project documentation
```

## Current Production Setup

### GitHub Actions Workflow
- **File**: `.github/workflows/whoop_pipeline.yml`
- **Schedule**: Daily at 8 AM UTC
- **Trigger**: Also supports manual `workflow_dispatch`
- **Steps**: checkout → setup Python 3.11 → install requirements-pipeline.txt → run_pipeline.py → dbt deps → dbt staging → dbt marts → dbt test

### Pipeline Entry Point
`run_pipeline.py` is the main script:
1. Refreshes Whoop OAuth token using stored refresh token
2. Connects to Snowflake with RSA private key
3. Extracts all 5 endpoints via `safe_extract_with_retry`
4. Token refresh uses direct HTTP call (not `auth.py`) to handle rotating refresh tokens

## Configuration

### GitHub Repository Secrets
Set these in GitHub → Settings → Secrets and variables → Actions:
```
WHOOP_CLIENT_ID
WHOOP_CLIENT_SECRET
WHOOP_ACCESS_TOKEN
WHOOP_REFRESH_TOKEN
WHOOP_REDIRECT_URI=http://localhost:8080/callback
SNOWFLAKE_ACCOUNT=AAB46027
SNOWFLAKE_USER=BRYAN
SNOWFLAKE_DATABASE=DATAEXPERT_STUDENT
SNOWFLAKE_SCHEMA=BRYAN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ALL_USERS_ROLE
NEW_SNOWFLAKE_PRIVATE_KEY=<PEM private key contents>
```

### Local Development (.env)
```
WHOOP_CLIENT_ID=...
WHOOP_CLIENT_SECRET=...
WHOOP_ACCESS_TOKEN=...
WHOOP_REFRESH_TOKEN=...
WHOOP_REDIRECT_URI=http://localhost:8080/callback
SNOWFLAKE_ACCOUNT=AAB46027
SNOWFLAKE_USER=BRYAN
SNOWFLAKE_DATABASE=DATAEXPERT_STUDENT
SNOWFLAKE_SCHEMA=BRYAN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=ALL_USERS_ROLE
NEW_SNOWFLAKE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----
...
-----END PRIVATE KEY-----
```

### Snowflake Authentication
Uses RSA private key (PEM format) stored in `NEW_SNOWFLAKE_PRIVATE_KEY`. The same key pair is shared with the `stock-market-streaming` project. `snowflake_client.py` uses `load_pem_private_key` (not DER conversion).

### Getting Fresh Whoop Tokens
Run this locally when tokens expire:
```bash
python get_whoop_tokens.py
```
This starts a local HTTP server on port 8080 to capture the OAuth callback automatically and prints the new access and refresh tokens. Update them in `.env` and GitHub Secrets.

## Data Pipeline

### Whoop API
- **Base URL**: `https://api.prod.whoop.com/developer/v2` (v1 deprecated October 1, 2025)
- **Authentication**: OAuth2 with rotating refresh tokens
- **Rate Limiting**: 60-second backoff for 429 errors, 1-second delay between pages
- **Pagination**: 25 records per page

### Raw Tables (Snowflake)
All tables land in `DATAEXPERT_STUDENT.BRYAN` schema with `raw_` prefix:
- `DATAEXPERT_STUDENT.BRYAN.raw_cycle`
- `DATAEXPERT_STUDENT.BRYAN.raw_sleep`
- `DATAEXPERT_STUDENT.BRYAN.raw_recovery`
- `DATAEXPERT_STUDENT.BRYAN.raw_workout`
- `DATAEXPERT_STUDENT.BRYAN.raw_user`
- `DATAEXPERT_STUDENT.BRYAN.pipeline_state` (incremental state tracking)

### Rotating Refresh Tokens
Whoop issues a new refresh token on every token refresh. Each GitHub Actions run consumes the current refresh token. The workflow updates `WHOOP_REFRESH_TOKEN` in GitHub Secrets after each successful run so the next run has a valid token. If a run fails mid-flight before saving the new token, you must run `get_whoop_tokens.py` locally to get fresh tokens.

## dbt Transformations

### Schema Architecture
All dbt models write to a single flat schema: `DATAEXPERT_STUDENT.BRYAN`. No sub-schemas. `dbt_project.yml` has no `+schema` overrides.

### Data Flow
```
DATAEXPERT_STUDENT.BRYAN.raw_*  →  stg_*  →  int_*  →  dim_* / fact_*
```

### Staging Models (read from raw_* tables)
- `stg_sleep` - Sleep data with millisecond-to-hour/minute conversions
- `stg_cycle` - Physiological cycle data with type casting
- `stg_workout` - Exercise data with heart rate zone time conversions
- `stg_recovery` - Recovery metrics with type casting
- `stg_user` - User profile data

Source definitions in `staging/schema.yml` point to:
- `database: DATAEXPERT_STUDENT`, `schema: BRYAN`, tables named `raw_sleep`, `raw_cycle`, etc.

### Intermediate Models
- `int_recovery_percentiles` - Rolling 365-day percentile calculations for recovery metrics
  - Uses `PERCENT_RANK()` window functions (tag: `marts`, runs after dim_user/dim_date)

### Marts Layer
- `dim_date` - Date dimension using Snowflake native GENERATOR (not dbt_date macro)
- `dim_user` - User dimension with surrogate key via dbt_utils
- `fact_daily_recovery` - Daily recovery metrics fact table
- `fact_daily_strain` - Daily strain/workout metrics
- `seasonal_adaptation` - Seasonal performance analysis

### dim_date Implementation
Uses Snowflake's native `GENERATOR` function (NOT `dbt_date.get_date_dimension()`). The dbt_date macro caused a recursive CTE error in Snowflake:
```sql
WITH date_spine AS (
    SELECT DATEADD(day, SEQ4(), '2020-01-01'::date) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4020))
)
SELECT
    REPLACE(CAST(date_day AS VARCHAR), '-', '')::INTEGER AS date_sk,
    ...
FROM date_spine WHERE date_day <= '2030-12-31'
```

### Package Dependencies
- **dbt-utils**: Surrogate key generation and common transformations
- (dbt-date removed — caused recursive CTE errors in Snowflake)

### Running dbt Locally
```bash
cd include/dbt
dbt deps
dbt run --select staging
dbt run --select marts
dbt test
```

## Snowflake Schema Overview
All objects in `DATAEXPERT_STUDENT.BRYAN`:
- `raw_cycle`, `raw_sleep`, `raw_recovery`, `raw_workout`, `raw_user` - Raw API data
- `pipeline_state` - Incremental extraction state
- `stg_*` - Staging models
- `int_recovery_percentiles` - Intermediate calculations
- `dim_date`, `dim_user` - Dimension tables
- `fact_daily_recovery`, `fact_daily_strain`, `seasonal_adaptation` - Fact tables

## Troubleshooting

### Token Issues
- **"invalid_request" on token refresh**: Rotating refresh tokens — old token already consumed. Run `python get_whoop_tokens.py` locally for fresh tokens.
- **404 on API endpoints**: Check you're using v2 base URL. Whoop v1 was deprecated October 1, 2025.

### Snowflake Connection Issues
- **JWT token invalid**: Verify `NEW_SNOWFLAKE_PRIVATE_KEY` contains the correct PEM key for account `AAB46027` (BRYAN user). Check with `SELECT CURRENT_ACCOUNT()` in Snowflake.
- **Account format**: Use just `AAB46027`, not `LYWBBPJ-AAB46027` (the locator prefix is not needed).
- **Role**: `ALL_USERS_ROLE` — verify with `SELECT CURRENT_ROLE()`.

### dbt Issues
- **Recursive CTE error on dim_date**: Do not use `dbt_date.get_date_dimension()` macro — it generates internal CTEs that conflict with Snowflake. Use the `GENERATOR` approach in `dim_date.sql`.
- **`is_current` column not found**: `dim_user` is a simple (non-SCD2) table. Do not reference `u.is_current` in joins.
- **int_recovery_percentiles running during staging step**: This model has tag `['marts']` (not staging). It depends on `dim_user` and `dim_date` so it must run after them.

### State Management
```sql
-- Check current pipeline state
SELECT * FROM DATAEXPERT_STUDENT.BRYAN.pipeline_state ORDER BY table_name;

-- Reset to avoid long backfills
UPDATE DATAEXPERT_STUDENT.BRYAN.pipeline_state
SET last_processed_at = '2025-01-01T00:00:00.000Z'
WHERE table_name = 'cycle';
```

## Migration History

### March 2026: Full Infrastructure Migration
1. **Snowflake**: Migrated from old account (TIBERSEPTIM/agcllyu-pl32467) to free account (BRYAN/AAB46027/DATAEXPERT_STUDENT)
2. **Authentication**: Switched from password auth to RSA private key (PEM format), shared key with stock-market-streaming project
3. **Orchestration**: Migrated from Astronomer Cloud Airflow → GitHub Actions (daily schedule)
4. **Whoop API**: Migrated from v1 (deprecated) to v2
5. **Schema**: Flattened from multi-schema (`whoop.raw.*`, `whoop.staging.*`, `whoop.marts.*`) to single schema (`DATAEXPERT_STUDENT.BRYAN.*`) with `raw_` prefix for raw tables

## Additional Files
- **MODELING.md**: Guide for implementing dimensional modeling with fact and dimension tables
