# Whoop Data Modeling Guide

This guide explains how to set up dimensional and fact tables for your Whoop data pipeline using dbt.

## Current Setup

Your existing dbt project has:
- **Staging models**: `stg_user`, `stg_sleep`, `stg_cycle`, `stg_recovery`, `stg_workout`
- **Schema**: `whoop.staging.*`
- **Configuration**: Already includes `marts` config in `dbt_project.yml`

## Dimensional Model Design

### Star Schema Structure

**Dimension Tables:**
- `dim_user` - User profiles (SCD Type 1)
- `dim_date` - Date dimension for time-based analysis
- `dim_sleep_score_state` - Sleep score states lookup
- `dim_cycle_score_state` - Cycle score states lookup

**Fact Tables:**
- `fact_sleep` - Daily sleep metrics and scores
- `fact_cycles` - Daily physiological cycles
- `fact_workouts` - Individual workout sessions
- `fact_recovery` - Daily recovery metrics

## File Structure

Create this directory structure under `include/dbt/models/`:

```
marts/
├── dimensions/
│   ├── dim_user.sql
│   ├── dim_date.sql
│   ├── dim_sleep_score_state.sql
│   └── dim_cycle_score_state.sql
├── facts/
│   ├── fact_sleep.sql
│   ├── fact_cycles.sql
│   ├── fact_workouts.sql
│   └── fact_recovery.sql
└── schema.yml
```

## Step-by-Step Implementation

### 1. Create Directory Structure

```bash
mkdir -p include/dbt/models/marts/dimensions
mkdir -p include/dbt/models/marts/facts
```

### 2. Create Dimension Tables

#### `include/dbt/models/marts/dimensions/dim_user.sql`

```sql
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

SELECT
    user_id,
    email,
    first_name,
    last_name,
    created_at,
    updated_at,
    ingested_at
FROM {{ ref('stg_user') }}
```

#### `include/dbt/models/marts/dimensions/dim_date.sql`

```sql
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(dateadd(year, 2, current_date()) as date)"
    )}}
),

date_dimension AS (
    SELECT
        date_day,
        extract(year from date_day) AS year,
        extract(month from date_day) AS month,
        extract(day from date_day) AS day,
        extract(dayofweek from date_day) AS day_of_week,
        extract(dayofyear from date_day) AS day_of_year,
        extract(week from date_day) AS week_of_year,
        extract(quarter from date_day) AS quarter,
        dayname(date_day) AS day_name,
        monthname(date_day) AS month_name,
        CASE
            WHEN extract(dayofweek from date_day) IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend
    FROM date_spine
)

SELECT * FROM date_dimension
```

#### `include/dbt/models/marts/dimensions/dim_sleep_score_state.sql`

```sql
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

SELECT DISTINCT
    score_state,
    CASE
        WHEN score_state = 'SCORED' THEN 'Sleep data fully processed'
        WHEN score_state = 'PENDING' THEN 'Sleep data being processed'
        WHEN score_state = 'UNSCORABLE' THEN 'Unable to score sleep data'
        ELSE 'Unknown state'
    END AS score_state_description
FROM {{ ref('stg_sleep') }}
WHERE score_state IS NOT NULL
```

#### `include/dbt/models/marts/dimensions/dim_cycle_score_state.sql`

```sql
{{ config(
    materialized='table',
    tags=['marts', 'dimensions']
) }}

SELECT DISTINCT
    score_state,
    CASE
        WHEN score_state = 'SCORED' THEN 'Cycle data fully processed'
        WHEN score_state = 'PENDING' THEN 'Cycle data being processed'
        WHEN score_state = 'UNSCORABLE' THEN 'Unable to score cycle data'
        ELSE 'Unknown state'
    END AS score_state_description
FROM {{ ref('stg_cycle') }}
WHERE score_state IS NOT NULL
```

### 3. Create Fact Tables

#### `include/dbt/models/marts/facts/fact_sleep.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    tags=['marts', 'facts']
) }}

SELECT
    s.id,
    s.user_id,
    date(s.start_time) AS sleep_date,
    s.created_at,
    s.updated_at,
    s.start_time,
    s.end_time,
    s.timezone_offset,
    s.nap,
    s.score_state,

    -- Sleep duration metrics (converted to hours/minutes)
    s.total_in_bed_time_milli AS total_in_bed_hours,
    s.total_awake_time_milli AS total_awake_minutes,
    s.total_no_data_time_milli AS total_no_data_minutes,
    s.total_light_sleep_time_milli AS total_light_sleep_hours,
    s.total_slow_wave_sleep_time_milli AS total_slow_wave_sleep_hours,
    s.total_rem_sleep_time_milli AS total_rem_sleep_hours,

    -- Sleep quality metrics
    s.sleep_cycle_count,
    s.disturbance_count,
    s.respiratory_rate,
    s.sleep_performance_percentage,
    s.sleep_consistency_percentage,
    s.sleep_efficiency_percentage,

    -- Sleep need metrics (converted to hours)
    s.baseline_milli AS baseline_hours,
    s.need_from_sleep_debt_milli AS need_from_sleep_debt_hours,
    s.need_from_recent_strain_milli AS need_from_recent_strain_hours,
    s.need_from_recent_nap_milli AS need_from_recent_nap_hours,

    s.ingested_at

FROM {{ ref('stg_sleep') }} s

{% if is_incremental() %}
WHERE s.ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
{% endif %}
```

#### `include/dbt/models/marts/facts/fact_cycles.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    tags=['marts', 'facts']
) }}

SELECT
    c.id,
    c.user_id,
    date(c.start_time) AS cycle_date,
    c.created_at,
    c.updated_at,
    c.start_time,
    c.end_time,
    c.timezone_offset,
    c.score_state,

    -- Strain and energy metrics
    c.strain,
    c.kilojoule,

    -- Heart rate metrics
    c.average_heart_rate,
    c.max_heart_rate,

    c.ingested_at

FROM {{ ref('stg_cycle') }} c

{% if is_incremental() %}
WHERE c.ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
{% endif %}
```

#### `include/dbt/models/marts/facts/fact_workouts.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    tags=['marts', 'facts']
) }}

SELECT
    w.id,
    w.user_id,
    date(w.created_at) AS workout_date,
    w.created_at,
    w.updated_at,
    w.start_time,
    w.end_time,
    w.timezone_offset,

    -- Workout metrics
    w.sport_id,
    w.score_state,
    w.strain,
    w.average_heart_rate,
    w.max_heart_rate,
    w.kilojoule,

    -- Heart rate zones (converted to minutes)
    w.zone_zero_milli AS zone_zero_minutes,
    w.zone_one_milli AS zone_one_minutes,
    w.zone_two_milli AS zone_two_minutes,
    w.zone_three_milli AS zone_three_minutes,
    w.zone_four_milli AS zone_four_minutes,
    w.zone_five_milli AS zone_five_minutes,

    w.ingested_at

FROM {{ ref('stg_workout') }} w

{% if is_incremental() %}
WHERE w.ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
{% endif %}
```

#### `include/dbt/models/marts/facts/fact_recovery.sql`

```sql
{{ config(
    materialized='incremental',
    unique_key='cycle_id',
    on_schema_change='fail',
    tags=['marts', 'facts']
) }}

SELECT
    r.cycle_id,
    r.user_id,
    date(r.created_at) AS recovery_date,
    r.created_at,
    r.updated_at,

    -- Recovery metrics
    r.score_state,
    r.user_calibrating,
    r.recovery_score,
    r.resting_heart_rate,
    r.hrv_rmssd_milli,
    r.spo2_percentage,
    r.skin_temp_celsius,

    r.ingested_at

FROM {{ ref('stg_recovery') }} r

{% if is_incremental() %}
WHERE r.ingested_at > (SELECT COALESCE(MAX(ingested_at), '1900-01-01') FROM {{ this }})
{% endif %}
```

### 4. Create Schema Documentation

#### `include/dbt/models/marts/schema.yml`

```yaml
version: 2

models:
  # Dimension Tables
  - name: dim_user
    description: "User dimension table with profile information"
    columns:
      - name: user_id
        description: "Unique user identifier"
        tests:
          - unique
          - not_null
      - name: email
        description: "User email address"
      - name: first_name
        description: "User first name"
      - name: last_name
        description: "User last name"

  - name: dim_date
    description: "Date dimension for time-based analysis"
    columns:
      - name: date_day
        description: "Calendar date"
        tests:
          - unique
          - not_null
      - name: year
        description: "Year"
      - name: month
        description: "Month (1-12)"
      - name: day
        description: "Day of month"
      - name: day_of_week
        description: "Day of week (1=Sunday, 7=Saturday)"
      - name: is_weekend
        description: "Boolean indicating weekend"

  # Fact Tables
  - name: fact_sleep
    description: "Daily sleep metrics and performance"
    columns:
      - name: id
        description: "Unique sleep record identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: sleep_date
        description: "Date of sleep session"
      - name: total_in_bed_hours
        description: "Total time in bed (hours)"
      - name: sleep_performance_percentage
        description: "Sleep performance score (0-100)"

  - name: fact_cycles
    description: "Daily physiological cycle data"
    columns:
      - name: id
        description: "Unique cycle identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: strain
        description: "Daily strain score (0-21)"
      - name: average_heart_rate
        description: "Average heart rate during cycle"

  - name: fact_workouts
    description: "Individual workout sessions"
    columns:
      - name: id
        description: "Unique workout identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: sport_id
        description: "Sport/activity type identifier"
      - name: strain
        description: "Workout strain score"

  - name: fact_recovery
    description: "Daily recovery metrics"
    columns:
      - name: cycle_id
        description: "Associated cycle identifier"
        tests:
          - unique
          - not_null
      - name: user_id
        description: "User identifier"
        tests:
          - not_null
      - name: recovery_score
        description: "Recovery score (0-100)"
      - name: resting_heart_rate
        description: "Resting heart rate (bpm)"
```

### 5. Update dbt_project.yml

Your `dbt_project.yml` already has marts configuration:

```yaml
models:
  whoop_dbt:
    marts:
      +schema: marts
      +materialized: table
      +tags: ['marts']
```

This will create tables in the `whoop.marts` schema.

### 6. Update Main DAG

Update your Cosmos DAG in `dags/whoop_pipeline_cosmos_simple.py` to include marts:

```python
# Add marts task group
marts_models = DbtTaskGroup(
    group_id="marts",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
    ),
    operator_args={
        "vars": {
            "is_test": False,
        },
    },
    select=["tag:marts"],  # Run all marts models
    dag=dag,
)

# Update dependencies
extract_data >> staging_models >> marts_models
```

Or for the fixed V1 DAG, add individual mart model tasks:

```python
# Add mart model tasks
run_dim_user = BashOperator(
    task_id='run_dim_user',
    bash_command='/usr/local/bin/dbt run --models dim_user --profiles-dir /usr/local/airflow/include/dbt --project-dir /usr/local/airflow/include/dbt',
    dag=dag,
)

run_fact_sleep = BashOperator(
    task_id='run_fact_sleep',
    bash_command='/usr/local/bin/dbt run --models fact_sleep --profiles-dir /usr/local/airflow/include/dbt --project-dir /usr/local/airflow/include/dbt',
    dag=dag,
)

# Add more mart tasks...

# Update dependencies
run_stg_workout >> [run_dim_user, run_fact_sleep, run_fact_cycles, run_fact_workouts, run_fact_recovery]
```

## Deployment Steps

1. **Create directory structure and files above**
2. **Test locally** (if using Docker):
   ```bash
   docker-compose --profile dbt run dbt-run --models tag:marts
   ```
3. **Deploy to Astronomer**:
   ```bash
   astro deploy
   ```
4. **Verify in Snowflake**: Tables should appear in `whoop.marts` schema

## Benefits of This Approach

- **Star schema design** for optimal analytics performance
- **Consistent naming** and documentation
- **Incremental loading** for large fact tables
- **Data quality tests** ensure reliability
- **Time-based partitioning** via date dimension
- **Cosmos integration** provides task-level monitoring and debugging

## Usage Examples

Once deployed, you can query the dimensional model:

```sql
-- Daily sleep performance by user
SELECT
    u.first_name,
    u.last_name,
    d.date_day,
    s.sleep_performance_percentage,
    s.total_in_bed_hours
FROM whoop.marts.fact_sleep s
JOIN whoop.marts.dim_user u ON s.user_id = u.user_id
JOIN whoop.marts.dim_date d ON s.sleep_date = d.date_day
WHERE d.date_day >= current_date - 30
ORDER BY s.sleep_performance_percentage DESC;

-- Weekly strain and recovery correlation
SELECT
    d.week_of_year,
    AVG(c.strain) as avg_strain,
    AVG(r.recovery_score) as avg_recovery
FROM whoop.marts.fact_cycles c
JOIN whoop.marts.fact_recovery r ON c.id = r.cycle_id
JOIN whoop.marts.dim_date d ON c.cycle_date = d.date_day
GROUP BY d.week_of_year
ORDER BY d.week_of_year;
```