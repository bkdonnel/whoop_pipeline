# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a Python-based data extraction pipeline for the Whoop API. The project extracts user data from various Whoop endpoints (Cycle, Sleep, Recovery, Workout, User) and loads them directly to Snowflake tables. Runs as scheduled DAGs in Apache Airflow on Astronomer Cloud.

## Project Structure

```
whoop_pipeline/
├── dags/                     # Airflow DAGs
│   ├── whoop_pipeline_dag.py # Main incremental extraction DAG
│   ├── test_whoop_variables.py # Debug DAGs for troubleshooting
│   └── debug_api_detailed.py
├── include/
│   └── extract/              # Python extraction modules
│       ├── whoop_extractor.py # Main extraction script
│       └── utils/
│           ├── auth.py        # OAuth2 authentication handler
│           ├── snowflake_client.py # Snowflake database integration
│           └── state_manager.py    # Pipeline state tracking
├── include/dbt/              # dbt transformations
│   ├── models/
│   │   ├── staging/          # Staging models for raw data
│   │   │   ├── stg_sleep.sql
│   │   │   ├── stg_cycle.sql
│   │   │   ├── stg_workout.sql
│   │   │   ├── stg_recovery.sql
│   │   │   └── schema.yml    # Model and source definitions
│   │   └── marts/            # Dimensional models for analytics
│   │       ├── dimensions/   # Dimension tables
│   │       │   └── dim_date.sql # Date dimension with dbt-date package
│   │       └── schema.yml    # Marts documentation and tests
│   ├── macros/
│   │   ├── generate_staging_model.sql  # Automated staging model creation
│   │   └── get_custom_schema.sql       # Schema naming logic
│   ├── packages.yml          # dbt package dependencies (includes dbt-date)
│   ├── dbt_project.yml       # dbt configuration
│   └── profiles.yml          # Snowflake connection profile
├── .env                     # Environment variables (credentials)
├── .env.astronomer          # Astronomer-specific environment variables
├── requirements.txt         # Python dependencies
├── Dockerfile              # Container configuration
├── docker-compose.yml     # Docker services (legacy)
├── reset_state_dates.sql   # SQL script for resetting pipeline state
└── README.md               # Project documentation
```

## Deployment Commands

### Astronomer Deployment
```bash
# Deploy to Astronomer Cloud
astro deploy

# Check deployment status
astro deploy list

# View logs
astro deploy logs

# Access Airflow UI: https://cmeujt9am0hm801p5xw58ivqw.astronomer.run/
```

## Cosmos Integration (Current Production Setup)

### Live DAGs
The project uses Astronomer Cosmos for dbt integration, providing superior debugging and observability:

1. **`whoop_pipeline_cosmos_simple`** (PRODUCTION)
   - Scheduled every 3 hours
   - Full pipeline: extract → staging models → marts models → quality checks
   - Each dbt model runs as separate Airflow task with dedicated logs
   - Connection: `snowflake_cosmos` (configured in Airflow UI)
   - Includes dimensional modeling with `dim_date` table

2. **`whoop_pipeline_cosmos_test`** (TESTING/DEBUGGING)
   - Manual trigger only
   - Tests single model (`stg_user`)
   - Useful for testing connection and debugging individual models

### Cosmos Benefits Achieved
- ✅ **Individual Task Debugging**: Each dbt model has dedicated Airflow task logs
- ✅ **Isolated Failures**: One model failure doesn't stop the entire pipeline
- ✅ **Clear Error Messages**: Specific dbt compilation/runtime errors (no more bash debugging)
- ✅ **Task-Level Retries**: Retry individual models without rebuilding entire pipeline
- ✅ **Native Airflow Integration**: Data lineage tracking and OpenLineage events
- ✅ **Schema Change Handling**: Clear messages for incremental model conflicts

### Connection Setup
Cosmos uses Airflow connection `snowflake_cosmos` with:
- **Type**: Snowflake
- **Host**: Snowflake account 
- **Login**: Snowflake username
- **Extra**: JSON with account, warehouse, database, role, private_key_content

### Local Development (Legacy)
```bash
# Install dependencies
pip install -r requirements.txt

# Set up authentication (interactive)
python include/extract/utils/auth.py

# Run data extraction locally
python include/extract/whoop_extractor.py
```

### dbt Transformations (Docker - Legacy)
**Prerequisites:**
1. Ensure Snowflake credentials are properly set in `.env` file
2. Raw data must be loaded into these Snowflake tables:
   - `whoop.raw.sleep`
   - `whoop.raw.cycle` 
   - `whoop.raw.workout`
   - `whoop.raw.recovery`

```bash
# Run all dbt steps (quick start)
./run_dbt.sh

# Or run individual steps:
# Install dbt dependencies
docker-compose --profile dbt run dbt-deps

# Run all staging models (includes dependency installation)
docker-compose --profile dbt run dbt-run

# Run tests for all staging models  
docker-compose --profile dbt run dbt-test

# Debug connection issues
docker run --rm --env-file .env -v $(pwd)/include/dbt:/app/dbt -w /app/dbt whoop_pipeline-dbt-run dbt debug
```

## Configuration

1. **Whoop API Setup**: Register at https://developer.whoop.com/ to get:
   - Client ID
   - Client Secret
   - Set redirect URI (default: http://localhost:8080/callback)

2. **Airflow Variables**: Set these in Astronomer UI (Admin → Variables):
   ```
   WHOOP_CLIENT_ID=your_client_id_here
   WHOOP_CLIENT_SECRET=your_client_secret_here
   WHOOP_ACCESS_TOKEN=your_access_token_here
   WHOOP_REFRESH_TOKEN=your_refresh_token_here
   WHOOP_REDIRECT_URI=http://localhost:8080/callback
   ```

3. **Environment Variables**: Update `.env.astronomer` file with Snowflake credentials:
   ```
   # Snowflake
   SNOWFLAKE_ACCOUNT=your_snowflake_account
   SNOWFLAKE_USER=your_snowflake_user
   SNOWFLAKE_PASSWORD=your_snowflake_password
   SNOWFLAKE_ROLE=your_snowflake_role
   SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
   SNOWFLAKE_DATABASE=whoop
   SNOWFLAKE_SCHEMA=raw
   SNOWFLAKE_PRIVATE_KEY=your_rsa_private_key (base64 encoded)
   ```

4. **Authentication Flow**: Run auth setup once locally to get access/refresh tokens, then add them to Airflow Variables

## Data Pipeline

The pipeline extracts data directly to Snowflake tables in the `whoop.raw` schema:
- `whoop.raw.cycle` - Physiological cycle data
- `whoop.raw.sleep` - Sleep activity data  
- `whoop.raw.recovery` - Recovery scores and metrics
- `whoop.raw.workout` - Exercise and activity data
- `whoop.raw.user` - User profile information

### Pipeline Features
- **Incremental Extraction**: Only pulls new data since last run (every 3 hours)
- **State Management**: Tracks last processed timestamp per endpoint in `whoop.metadata.pipeline_state`
- **Rate Limit Handling**: Automatically handles API rate limits with backoff and retry
- **Data Validation**: Adds `_extracted_at` timestamp metadata to all records
- **Error Recovery**: Comprehensive retry logic and state tracking

## dbt Transformations

The pipeline includes dbt models to transform raw Whoop data through staging and marts layers using automated model generation:

### Staging Models
- `stg_sleep` - Sleep data with millisecond to hour/minute conversions
- `stg_cycle` - Physiological cycle data with basic type casting
- `stg_workout` - Exercise data with heart rate zone time conversions
- `stg_recovery` - Recovery metrics with basic type casting
- `stg_user` - User profile data

### Marts Layer (Dimensional Models)
- `dim_date` - Comprehensive date dimension using dbt-date package
  - Date range: 2020-01-01 to 2027-12-31
  - Includes season, training periods, and time-based analytics features
  - Surrogate key generation with dbt_utils
  - Ready for fact table joins and time-series analysis

### Package Dependencies
- **dbt-date**: Provides robust date dimension functionality
- **dbt-utils**: Utility macros for surrogate keys and common transformations

### Automated Model Generation
Uses custom macros for consistent staging model creation:
- `generate_staging_model()` - Creates standardized staging views
- Static column definitions for reliable Docker/CI execution
- Configurable transformations (e.g., milliseconds to hours/minutes)
- Consistent type casting across all models

### Data Flow
```
whoop.raw.*        → whoop.staging.stg_*      → whoop.marts.*
whoop.raw.sleep    → whoop.staging.stg_sleep
whoop.raw.cycle    → whoop.staging.stg_cycle
whoop.raw.workout  → whoop.staging.stg_workout
whoop.raw.recovery → whoop.staging.stg_recovery
whoop.raw.user     → whoop.staging.stg_user
                                               → whoop.marts.dim_date
```

### Testing
Comprehensive data quality tests include:
- Uniqueness and not-null constraints for primary keys
- Value range validations for metrics
- Accepted values for categorical fields
- Data type consistency checks
- Date dimension validation (uniqueness, accepted values)

## API Integration

- **Base URL**: https://api.prod.whoop.com/developer/v1
- **Authentication**: OAuth2 with automatic token refresh via Airflow Variables
- **Rate Limiting**: Built-in handling with 60-second backoff for 429 errors
- **Pagination**: Automatic handling with 25 records per page (API limit)
- **Scopes**: read:cycles read:recovery read:sleep read:workout read:profile offline

## Troubleshooting

### Common Issues Fixed
1. **400 Errors**: Fixed by changing from API v2 to v1 and using limit=25 (not 50)
2. **429 Rate Limiting**: Handled with automatic 60-second backoff and 1-second delays between pages
3. **Token Refresh Failures**: Fixed by using Airflow Variables instead of environment variables
4. **Massive Backfills**: Avoided by resetting state table dates to recent dates instead of 2020
5. **dbt Staging Tables Not Updating in Astronomer**: Missing `dbt deps` command in Astronomer DAG - local docker-compose worked because it included `dbt deps && dbt run --select tag:staging`

### dbt Staging Model Issues
If staging tables (`whoop.staging.stg_*`) are not updating in Astronomer but work locally:

**Root Cause 1**: The Astronomer dbt task was missing `dbt deps` while local docker-compose included it.

**Solution 1**: Update the DAG's `run_dbt_staging` task to include:
```bash
/usr/local/bin/dbt deps --profiles-dir /usr/local/airflow/include/dbt --project-dir /usr/local/airflow/include/dbt
```

**Root Cause 2**: Missing `+schema: staging` in dbt_project.yml causing models to not be found by `--select tag:staging`.

**Solution 2**: Update dbt_project.yml staging configuration to include schema:
```yaml
staging:
  +schema: staging
  +materialized: incremental
  +tags: ['staging']
```

**Root Cause 3**: dbt version compatibility issues between local (1.7.18) and Astronomer (1.10.8) environments - selectors not working.

**Solution 3**: Use explicit model names instead of selectors for dbt 1.10.8 compatibility:
```bash
# Instead of: --select tag:staging or --select models/staging
# Use: --models stg_cycle stg_recovery stg_sleep stg_user stg_workout
```

**Debugging Steps**:
1. Check DAG logs for `dbt list --select tag:staging` output - should show 5 models
2. Verify `dbt run` exit code is 0, not 2 (error)
3. Add comprehensive debugging to diagnose parsing issues:
   ```bash
   echo "=== Check file structure ==="
   ls -la models/
   ls -la models/staging/
   echo "=== dbt debug ==="
   /usr/local/bin/dbt debug --profiles-dir /path --project-dir /path
   echo "=== List ALL models ==="
   /usr/local/bin/dbt list --profiles-dir /path --project-dir /path
   echo "=== List staging models by path ==="
   /usr/local/bin/dbt list --select models/staging --profiles-dir /path --project-dir /path
   echo "=== List models with staging tag ==="
   /usr/local/bin/dbt list --select tag:staging --profiles-dir /path --project-dir /path
   echo "=== Running dbt staging models by path ==="
   /usr/local/bin/dbt run --select models/staging --profiles-dir /path --project-dir /path
   ```

**Final Solution Summary**:
The issue was dbt 1.10.8 complete failure - even explicit model names fail with exit code 2 and produce zero output despite 6-second execution time. This indicates a fundamental dbt 1.10.8 bug or environment incompatibility.

**SOLUTION 1 - Fixed V1 DAG (WORKING)**:
Updated `whoop_pipeline_dag.py` to run all staging models explicitly instead of using selectors:
```bash
# Instead of: --select tag:staging (fails in dbt 1.10.8)
# Use individual model runs:
/usr/local/bin/dbt run --models stg_user --profiles-dir ... 
/usr/local/bin/dbt run --models stg_sleep --profiles-dir ...
/usr/local/bin/dbt run --models stg_cycle --profiles-dir ...
/usr/local/bin/dbt run --models stg_recovery --profiles-dir ...
/usr/local/bin/dbt run --models stg_workout --profiles-dir ...
```

**SOLUTION 2 - Direct SQL Approach**:
Created `whoop_pipeline_dag_v2.py` that bypasses dbt entirely using direct SQL transformations:
```python
# Create staging tables with direct SQL instead of dbt
def create_staging_tables(**context):
    # Connect to Snowflake directly
    # Execute CREATE OR REPLACE TABLE staging.stg_* AS SELECT ... FROM raw.*
    # Include same transformations (ms to hours, type casting, etc.)
```

**dbt 1.10.8 Critical Issues Found**:
1. **Selector failure**: `tag:staging` and `models/staging` return empty results
2. **Silent execution failure**: `dbt run` produces zero output despite 6-second runtime
3. **Exit code 2 with no error messages**: Indicates fundamental dbt environment issue
4. **Local vs Astronomer incompatibility**: Same models work in dbt 1.7.18 but fail in 1.10.8

**Available Solutions**:
1. **Fixed V1 DAG** (RECOMMENDED): Use updated `whoop_pipeline_dag.py` with explicit model names
2. **Direct SQL**: Use `whoop_pipeline_dag_v2.py` with Python + Snowflake connector
3. **dbt version downgrade**: Request Astronomer to use dbt 1.7.x
4. **Local dbt execution**: Run dbt transformations outside Astronomer environment

**Comparison**:
- **Local (working)**: `dbt deps && dbt run --select tag:staging` (dbt 1.7.18)
- **Astronomer V1 (FIXED)**: Individual model runs `--models stg_*` (dbt 1.10.8)
- **Astronomer V2 (working)**: Direct SQL transformations bypasses dbt

**Current Production Setup**:
- **DAG**: `whoop_pipeline_incremental` (V1 - Fixed)
- **All 5 staging models run explicitly**: `stg_user`, `stg_sleep`, `stg_cycle`, `stg_recovery`, `stg_workout`
- **Same transformations**: milliseconds to hours/minutes, proper type casting via dbt macros
- **Schema**: Creates tables in correct `whoop.staging.*` schema

**Verification**: 
- **V1 DAG (Fixed)**: All staging tables should update after each run
- **V2 DAG**: Alternative option using direct SQL
- **Schema check**: Tables appear in `whoop.staging.*` schema
- **Record counts**: All staging tables should match raw table counts

### Debug DAGs
- `test_whoop_variables`: Tests Airflow variable access and basic API calls
- `debug_api_detailed`: Detailed API endpoint testing with different parameters

### State Management
Reset pipeline state to avoid long backfills:
```sql
-- Check current state
SELECT table_name, last_processed_at, last_run_status 
FROM whoop.metadata.pipeline_state 
ORDER BY table_name;

-- Reset to recent dates if needed
UPDATE whoop.metadata.pipeline_state 
SET last_processed_at = '2024-08-25T00:00:00.000Z'
WHERE table_name = 'user';
```

## Production Schedule
- **DAG**: `whoop_pipeline_cosmos_simple` (CURRENT PRODUCTION)
- **Schedule**: Every 3 hours
- **Pipeline Flow**: extract → staging models → marts models → quality checks
- **Expected Records**: 0-25 records per endpoint per run (incremental)
- **Deployment**: Astronomer Cloud (https://cmeujt9am0hm801p5xw58ivqw.astronomer.run/)

## Schema Overview
- **Raw Layer**: `whoop.raw.*` - Direct API extracts with minimal processing
- **Staging Layer**: `whoop.staging.*` - Cleaned, typed, and transformed data
- **Marts Layer**: `whoop.marts.*` - Dimensional models ready for analytics
  - Currently includes `dim_date` with comprehensive date attributes
  - Future fact tables will reference staging models and date dimension

## Additional Files
- **MODELING.md**: Comprehensive guide for implementing dimensional modeling with fact and dimension tables
- **packages.yml**: Contains dbt package dependencies including dbt-date and dbt-utils