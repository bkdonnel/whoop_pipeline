from datetime import datetime, timedelta
from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

# DAG configuration - ONE TIME RUN ONLY
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 14),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create one-time DAG for dim_date creation
dag = DAG(
    'create_dim_date_one_time',
    default_args=default_args,
    description='One-time creation of dim_date table - DISABLE AFTER FIRST RUN',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['one-time', 'setup', 'dimension'],
)

# Cosmos dbt configuration
dbt_project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/dbt",
)

# Select only dim_date model
dim_date_render_config = RenderConfig(
    select=["models/marts/dimensions/dim_date"],  # Specific model path
)

profile_config = ProfileConfig(
    profile_name="whoop_snowflake",
    target_name="prod",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
        conn_id="snowflake_cosmos",
        profile_args={
            "schema": "marts",  # Create in marts schema
        }
    )
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

# Create dbt task group for dim_date only
create_dim_date_task = DbtTaskGroup(
    group_id="create_dim_date",
    project_config=dbt_project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=dim_date_render_config,
    operator_args={
        "full_refresh": True,  # Force full refresh for initial creation
    },
    dag=dag,
)

# Single task - no dependencies needed
create_dim_date_task