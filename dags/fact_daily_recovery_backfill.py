"""
One-time backfill DAG for fact_daily_recovery table
This DAG will run the fact_daily_recovery model with --full-refresh to backfill historical data with new columns.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

# Define paths
DBT_PROJECT_PATH = "/usr/local/airflow/include/dbt"
DBT_EXECUTABLE_PATH = "/usr/local/bin/dbt"

# Profile configuration for Snowflake
profile_config = ProfileConfig(
    profile_name="whoop_snowflake",
    target_name="prod",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(
        conn_id="snowflake_cosmos",
        profile_args={
            "schema": "staging",
        }
    )
)

# Project configuration
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# Execution configuration
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    dag_id="fact_daily_recovery_backfill",
    description="One-time backfill of fact_daily_recovery table with full refresh",
    start_date=datetime(2025, 9, 23),
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "one-time", "fact-table"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "owner": "data-team",
    },
)
def fact_daily_recovery_backfill_dag():
    """
    One-time DAG to backfill fact_daily_recovery with full refresh.

    This will:
    1. Run all upstream dependencies (staging and intermediate models)
    2. Run fact_daily_recovery with --full-refresh to rebuild entire table
    3. Run data quality tests

    Usage:
    1. Deploy this DAG
    2. Trigger it manually from Airflow UI
    3. Monitor completion
    4. Verify historical records have new columns populated
    5. Delete/disable this DAG after successful backfill
    """

    # Step 1: Run upstream dependencies first (without select to avoid conflicts)
    upstream_models = DbtTaskGroup(
        group_id="run_upstream_dependencies",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["stg_recovery", "stg_sleep", "stg_user", "int_recovery_percentiles"]
        ),
        operator_args={
            "full_refresh": False,  # Don't full refresh staging - just ensure they're current
        },
    )

    # Step 2: Full refresh fact_daily_recovery
    backfill_fact_table = DbtTaskGroup(
        group_id="backfill_fact_daily_recovery",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["fact_daily_recovery"]
        ),
        operator_args={
            "full_refresh": True,  # This is the key - forces complete rebuild
        },
    )

    # Step 3: Run tests to validate the backfill
    test_fact_table = DbtTaskGroup(
        group_id="test_fact_daily_recovery",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            select=["fact_daily_recovery"]
        ),
        operator_args={
            "cmd": "test",
        },
    )

    # Define dependencies
    upstream_models >> backfill_fact_table >> test_fact_table

# Instantiate the DAG
fact_daily_recovery_backfill = fact_daily_recovery_backfill_dag()