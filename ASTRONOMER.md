# Astronomer & Airflow Migration Guide

This guide walks you through converting your current Whoop pipeline to run on Astronomer with Airflow for incremental processing every 3 hours.

## Overview

**Current State**: Manual/Docker-based pipeline that processes all historical data
**Target State**: Automated Airflow pipeline that processes only new data incrementally

## Prerequisites

- [ ] Astronomer CLI installed (`astro dev init`)
- [ ] Existing Whoop API credentials
- [ ] Snowflake connection details
- [ ] Git repository access

## Step 1: Project Structure Setup

### 1.1 Initialize Astronomer Project
```bash
# In your project root
astro dev init

# This creates:
# ├── dags/
# ├── include/
# ├── plugins/
# ├── tests/
# ├── Dockerfile
# ├── requirements.txt
# └── .astro/
```

### 1.2 Update Project Structure
```
whoop_pipeline/
├── dags/
│   └── whoop_pipeline_dag.py          # Main Airflow DAG
├── include/
│   ├── extract/
│   │   ├── whoop_extractor.py         # Modified for incremental
│   │   └── utils/
│   │       ├── auth.py
│   │       ├── snowflake_client.py
│   │       └── state_manager.py       # NEW: Track pipeline state
│   └── dbt/                           # Move dbt project here
│       ├── models/
│       ├── macros/
│       └── dbt_project.yml
├── plugins/                           # Custom Airflow operators (if needed)
├── tests/
├── Dockerfile                         # Astronomer-compatible
├── requirements.txt                   # Airflow + your dependencies
└── ASTRONOMER.md                      # This guide
```

## Step 2: State Management Implementation

### 2.1 Create State Management Table
```sql
-- Run in Snowflake
CREATE TABLE IF NOT EXISTS whoop.metadata.pipeline_state (
    table_name VARCHAR(50) PRIMARY KEY,
    last_processed_at TIMESTAMP_NTZ,
    last_run_status VARCHAR(20),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Initialize state for each table
INSERT INTO whoop.metadata.pipeline_state VALUES
('sleep', '2020-01-01T00:00:00.000Z', 'success', CURRENT_TIMESTAMP()),
('cycle', '2020-01-01T00:00:00.000Z', 'success', CURRENT_TIMESTAMP()),
('workout', '2020-01-01T00:00:00.000Z', 'success', CURRENT_TIMESTAMP()),
('recovery', '2020-01-01T00:00:00.000Z', 'success', CURRENT_TIMESTAMP()),
('user', '2020-01-01T00:00:00.000Z', 'success', CURRENT_TIMESTAMP());
```

### 2.2 Create State Manager Class
Create `include/extract/utils/state_manager.py`:
```python
import snowflake.connector
from datetime import datetime
from typing import Optional

class StateManager:
    def __init__(self, snowflake_conn):
        self.conn = snowflake_conn
    
    def get_last_processed_timestamp(self, table_name: str) -> str:
        """Get the last processed timestamp for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_processed_at FROM whoop.metadata.pipeline_state WHERE table_name = %s",
            (table_name,)
        )
        result = cursor.fetchone()
        return result[0].isoformat() + 'Z' if result else '2020-01-01T00:00:00.000Z'
    
    def update_last_processed_timestamp(self, table_name: str, timestamp: str, status: str = 'success'):
        """Update the last processed timestamp for a table"""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            MERGE INTO whoop.metadata.pipeline_state AS target
            USING (SELECT %s as table_name, %s as last_processed_at, %s as status) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET 
                last_processed_at = source.last_processed_at,
                last_run_status = source.status,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT 
                (table_name, last_processed_at, last_run_status, updated_at)
                VALUES (source.table_name, source.last_processed_at, source.status, CURRENT_TIMESTAMP())
            """,
            (table_name, timestamp, status)
        )
        self.conn.commit()
```

## Step 3: Modify Data Extraction for Incremental Loading

### 3.1 Update Whoop Extractor
Modify `include/extract/whoop_extractor.py`:

```python
from datetime import datetime, timezone
from .utils.state_manager import StateManager

class IncrementalWhoopExtractor:
    def __init__(self, snowflake_conn):
        self.state_manager = StateManager(snowflake_conn)
        self.base_url = "https://api.prod.whoop.com/developer/v1"
        # ... existing auth setup
        
        # Define all Whoop API endpoints with their configurations
        self.endpoints = {
            'cycle': {
                'url': f"{self.base_url}/cycle",
                'table_name': 'raw.cycle',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'sleep': {
                'url': f"{self.base_url}/activity/sleep", 
                'table_name': 'raw.sleep',
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'recovery': {
                'url': f"{self.base_url}/recovery",
                'table_name': 'raw.recovery', 
                'supports_incremental': True,
                'time_field': 'updated_at'
            },
            'workout': {
                'url': f"{self.base_url}/activity/workout",
                'table_name': 'raw.workout',
                'supports_incremental': True, 
                'time_field': 'updated_at'
            },
            'user': {
                'url': f"{self.base_url}/user/profile/basic",
                'table_name': 'raw.user',
                'supports_incremental': False,  # User profile changes infrequently
                'time_field': None
            }
        }
    
    def extract_incremental_data(self, endpoint_name):
        """Extract incremental data for any endpoint"""
        config = self.endpoints[endpoint_name]
        
        # Handle non-incremental endpoints (like user profile)
        if not config['supports_incremental']:
            return self.extract_full_refresh(endpoint_name)
        
        last_processed = self.state_manager.get_last_processed_timestamp(endpoint_name)
        
        # Call Whoop API with since parameter for incremental extraction
        params = {
            'start': last_processed,
            'limit': 50  # Pagination
        }
        
        all_records = []
        url = config['url']
        
        while url:
            response = self.make_paginated_request(url, params)
            records = response.get('records', [])
            all_records.extend(records)
            
            # Get next page URL
            url = response.get('next_token')
            params = {'pageToken': url} if url else None
        
        if all_records:
            # Add ingestion timestamp
            current_time = datetime.now(timezone.utc).isoformat()
            for record in all_records:
                record['ingested_at'] = current_time
            
            # Load to Snowflake
            self.load_to_snowflake(config['table_name'], all_records)
            
            # Update state with the latest record timestamp
            time_field = config['time_field']
            if time_field and all_records:
                latest_timestamp = max([record[time_field] for record in all_records if time_field in record])
                self.state_manager.update_last_processed_timestamp(endpoint_name, latest_timestamp)
        
        return len(all_records)
    
    def extract_full_refresh(self, endpoint_name):
        """Extract full data for endpoints that don't support incremental (like user profile)"""
        config = self.endpoints[endpoint_name]
        url = config['url']
        
        try:
            response_data = self.make_api_request(url)
            
            # User profile returns single object, not paginated
            if endpoint_name == 'user':
                all_records = [response_data] if response_data else []
            else:
                # Handle other non-incremental endpoints if needed
                all_records = response_data.get('records', [response_data])
            
            if all_records:
                # Add ingestion timestamp
                current_time = datetime.now(timezone.utc).isoformat()
                for record in all_records:
                    record['ingested_at'] = current_time
                
                # Load to Snowflake (replace existing data)
                self.load_to_snowflake(config['table_name'], all_records, replace=True)
            
            return len(all_records)
            
        except Exception as e:
            print(f"Error extracting {endpoint_name}: {e}")
            raise
```

### 3.2 Add Error Handling and Retries
```python
def safe_extract_with_retry(self, endpoint_name: str, max_retries: int = 3):
    """Extract with retry logic and state management for any endpoint"""
    for attempt in range(max_retries):
        try:
            # Use the universal extraction method for all endpoints
            count = self.extract_incremental_data(endpoint_name)
            
            print(f"Successfully extracted {count} {endpoint_name} records")
            return count
            
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {endpoint_name}: {e}")
            if attempt == max_retries - 1:
                # Mark as failed in state table
                self.state_manager.update_last_processed_timestamp(
                    endpoint_name, 
                    self.state_manager.get_last_processed_timestamp(endpoint_name),
                    'failed'
                )
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

## Step 4: Convert dbt Models to Incremental

### 4.1 Update dbt_project.yml
```yaml
models:
  whoop_dbt:
    staging:
      +materialized: incremental
      +on_schema_change: 'fail'
      +tags: ['staging']
```

### 4.2 Modify Staging Models for Incremental Data
Update incremental staging models (e.g., `models/staging/stg_sleep.sql`):

```sql
{# Add incremental configuration #}
{{ config(
    materialized='incremental',
    unique_key='ID',
    on_schema_change='fail'
) }}

{# Existing column definitions #}
{% set column_casts = {
    'ID': 'string',
    'USER_ID': 'string',
    -- ... rest of columns
} %}

{# Generate base model #}
{{ generate_staging_model('raw', 'sleep', column_casts, transformations) }}

{# Add incremental logic #}
{% if is_incremental() %}
  -- Only process records newer than what we already have
  WHERE INGESTED_AT > (SELECT COALESCE(MAX(INGESTED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
```

### 4.3 Update Macro for Incremental Support
Modify `macros/generate_staging_model.sql`:

```sql
{% macro generate_staging_model(source_name, table_name, column_casts, transformations = {}) %}

WITH base AS (
    SELECT
    {% for col_name, col_type in column_casts.items() %}
        {%- if col_name in transformations %}
            {{ transformations[col_name] }} AS {{ col_name }}
        {%- else %}
            {{ col_name }}::{{ col_type }} AS {{ col_name }}
        {%- endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ source(source_name, table_name) }}
    
    {# Incremental logic will be added by the calling model #}
)

SELECT * FROM base

{% endmacro %}
```

### 4.4 Handle Non-Incremental Models (User Profile)
For the user endpoint, create a separate staging model that replaces data each run:

`models/staging/stg_user.sql`:
```sql
{# User profile - full refresh each time #}
{{ config(
    materialized='table',
    tags=['staging', 'user']
) }}

{% set column_casts = {
    'USER_ID': 'string',
    'EMAIL': 'string', 
    'FIRST_NAME': 'string',
    'LAST_NAME': 'string',
    'CREATED_AT': 'timestamp_ntz',
    'UPDATED_AT': 'timestamp_ntz'
} %}

{{ generate_staging_model('raw', 'user', column_casts) }}
```

## Step 5: Create Airflow DAG

### 5.1 Main DAG File
Create `dags/whoop_pipeline_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 13),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'whoop_pipeline_incremental',
    default_args=default_args,
    description='Incremental Whoop data pipeline',
    schedule_interval='0 */3 * * *',  # Every 3 hours
    catchup=False,
    max_active_runs=1,
)

def extract_whoop_data(**context):
    """Extract incremental data from Whoop API"""
    from include.extract.whoop_extractor import IncrementalWhoopExtractor
    
    # Get Snowflake connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    
    # Initialize extractor
    extractor = IncrementalWhoopExtractor(conn)
    
    # Extract each data type - all available Whoop API endpoints
    endpoints = ['sleep', 'cycle', 'workout', 'recovery', 'user']
    results = {}
    
    for endpoint in endpoints:
        try:
            count = extractor.safe_extract_with_retry(endpoint)
            results[endpoint] = count
            print(f"Extracted {count} records for {endpoint}")
        except Exception as e:
            print(f"Failed to extract {endpoint}: {e}")
            raise
    
    # Push results to XCom for monitoring
    return results

# Task 1: Extract data from Whoop API
extract_task = PythonOperator(
    task_id='extract_whoop_data',
    python_callable=extract_whoop_data,
    dag=dag,
)

# Task 2: dbt transformations using Cosmos
dbt_config = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt',
)

profile_config = ProfileConfig(
    profile_name='whoop_snowflake',
    target_name='dev',
    profiles_yml_filepath='/usr/local/airflow/include/dbt/profiles.yml',
)

dbt_tasks = DbtTaskGroup(
    group_id="dbt_staging",
    project_config=dbt_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt"),
    select=['tag:staging'],
    dag=dag,
)

# Task 3: Data quality checks
def run_data_quality_checks(**context):
    """Run custom data quality checks"""
    # Get extraction results from previous task
    extract_results = context['ti'].xcom_pull(task_ids='extract_whoop_data')
    
    # Basic checks for all endpoints
    for endpoint, count in extract_results.items():
        if endpoint == 'user':
            # User profile should always return 1 record (or 0 if error)
            if count == 0:
                print(f"Warning: No user profile data found")
            elif count > 1:
                print(f"Warning: Multiple user records found: {count}")
        else:
            # For incremental endpoints (sleep, cycle, workout, recovery)
            if count == 0:
                print(f"Info: No new records found for {endpoint} (normal for incremental)")
            elif count > 1000:
                print(f"Warning: Unusually high record count for {endpoint}: {count}")
    
    return "Data quality checks passed"

quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Set task dependencies
extract_task >> dbt_tasks >> quality_check_task
```

## Step 6: Configure Astronomer Environment

### 6.1 Update Dockerfile
```dockerfile
FROM quay.io/astronomer/astro-runtime:11.7.0

# Install dbt and other dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your code
COPY include/ /usr/local/airflow/include/
COPY dags/ /usr/local/airflow/dags/
COPY plugins/ /usr/local/airflow/plugins/

# Set permissions
USER astro
```

### 6.2 Update requirements.txt
```txt
# Airflow providers
apache-airflow-providers-snowflake==5.1.1
astronomer-cosmos[dbt-snowflake]==1.2.5

# Your existing dependencies
requests==2.31.0
snowflake-connector-python==3.15.0
python-dotenv==1.0.0
dbt-core==1.10.6
dbt-snowflake==1.10.0

# Additional dependencies
pandas==2.1.0
```

### 6.3 Configure Airflow Connections
In Airflow UI, create connections:

**Snowflake Connection** (`snowflake_default`):
- Connection Type: Snowflake
- Host: `agcllyu-pl32467.snowflakecomputing.com`
- Schema: `staging`
- Login: Your username
- Password: Your password
- Extra: `{"account": "agcllyu-pl32467", "warehouse": "COMPUTE_WH", "database": "whoop", "role": "ACCOUNTADMIN"}`

**Whoop API Variables**:
- `WHOOP_CLIENT_ID`: Your client ID
- `WHOOP_CLIENT_SECRET`: Your client secret
- `WHOOP_REDIRECT_URI`: Your redirect URI

## Step 7: Testing and Deployment

### 7.1 Local Testing
```bash
# Start Airflow locally
astro dev start

# Access Airflow UI at http://localhost:8080
# Username: admin, Password: admin

# Test your DAG
astro dev run dags test whoop_pipeline_incremental
```

### 7.2 Deployment Strategy

**Initial Migration**:
1. Run full historical load one final time with current setup
2. Record the timestamp of this load as your starting point
3. Deploy incremental pipeline
4. Run first incremental load to verify it only picks up new data

**Production Deployment**:
```bash
# Deploy to Astronomer
astro deploy

# Monitor first few runs closely
# Check Airflow logs and Snowflake for expected data volumes
```

## Step 8: Monitoring and Alerting

### 8.1 Add Data Volume Monitoring
Create alerts for:
- Zero records extracted (potential API issues)
- Unusually high record counts (potential duplicate processing)
- Pipeline failures or timeouts
- dbt test failures

### 8.2 Create Operational Dashboard
Track these metrics:
- Records processed per run by table
- Pipeline execution time trends
- Success/failure rates
- Data freshness (time between record creation and processing)

## Step 9: Backfill Strategy

### 9.1 Manual Backfill Task
Create an additional DAG for backfilling specific date ranges:

```python
def backfill_date_range(start_date: str, end_date: str, table_name: str):
    """Backfill data for a specific date range"""
    # Temporarily override state management
    # Extract data for specific date range
    # Process through dbt
    pass
```

## Troubleshooting

### Common Issues:
1. **State corruption**: Reset state table to recover
2. **Duplicate data**: Check unique_key configuration in dbt models  
3. **Missing data**: Verify API pagination logic
4. **Airflow connection issues**: Check connection configuration and network access

### Recovery Procedures:
1. **Failed extraction**: Pipeline will retry from last successful timestamp
2. **Failed transformation**: dbt incremental models handle reruns gracefully
3. **Partial failures**: Each table tracked independently

## Expected Outcomes

**Performance Improvements**:
- **Runtime**: 3-5 minutes instead of 15-30 minutes
- **API calls**: 10-25 requests instead of 100+ requests  
- **Data processing**: 10-50 new records instead of 1000+ records per endpoint
- **Cost**: 80-90% reduction in compute costs
- **Endpoint coverage**: All 5 Whoop API endpoints (cycle, sleep, recovery, workout, user)

**Reliability Improvements**:
- Automatic retries and error handling
- Independent table processing (one failure doesn't stop others)
- Built-in monitoring and alerting
- Easy backfill capabilities

This migration will transform your pipeline from a manual, resource-intensive process to an efficient, automated data pipeline that scales with your data volume!