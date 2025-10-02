from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

# DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create main DAG
dag = DAG(
    'whoop_pipeline_cosmos_simple',
    default_args=default_args,
    description='Whoop Data Pipeline with Cosmos dbt Integration (Simple)',
    schedule_interval='0 */3 * * *',  # Every 3 hours
    catchup=False,
    max_active_runs=1,
)

# Task 1: Extract incremental data from Whoop API
def extract_whoop_data(**context):
    import os, base64
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    import snowflake.connector
    from include.extract.whoop_extractor import IncrementalWhoopExtractor

    # Snowflake credentials from Airflow Variables
    snowflake_account = Variable.get('SNOWFLAKE_ACCOUNT')
    snowflake_user = Variable.get('SNOWFLAKE_USER')  
    snowflake_private_key = Variable.get('SNOWFLAKE_PRIVATE_KEY')
    snowflake_warehouse = Variable.get('SNOWFLAKE_WAREHOUSE')
    snowflake_database = Variable.get('SNOWFLAKE_DATABASE', default_var='whoop')
    snowflake_schema = 'raw'
    snowflake_role = Variable.get('SNOWFLAKE_ROLE')

    # Convert private key
    private_key_bytes = base64.b64decode(snowflake_private_key)
    private_key_obj = serialization.load_der_private_key(
        private_key_bytes, password=None, backend=default_backend()
    )

    conn = snowflake.connector.connect(
        user=snowflake_user,
        private_key=private_key_obj,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
        role=snowflake_role
    )

    extractor = IncrementalWhoopExtractor(conn)
    endpoints = ['sleep','cycle','workout','recovery','user']
    results = {}
    for endpoint in endpoints:
        try:
            results[endpoint] = extractor.safe_extract_with_retry(endpoint)
        except Exception as e:
            print(f"Failed to extract {endpoint}: {e}")
            results[endpoint] = 0

    conn.close()
    return results

# Task 3: Data quality checks
def run_data_quality_checks(**context):
    extract_results = context['ti'].xcom_pull(task_ids='extract_whoop_data')
    for endpoint, count in extract_results.items():
        if endpoint == 'user' and count != 1:
            print(f"Warning: Unexpected user record count: {count}")
        elif endpoint != 'user' and count == 0:
            print(f"Info: No new records for {endpoint}")
        elif endpoint != 'user' and count > 1000:
            print(f"Warning: High record count for {endpoint}: {count}")
    return "Data quality checks passed"

# Create extract task
extract_task = PythonOperator(
    task_id='extract_whoop_data',
    python_callable=extract_whoop_data,
    dag=dag,
)

# Cosmos dbt configuration
dbt_project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/dbt",
    dbt_vars={"DBT_PACKAGES_HUB_URL": "https://hub.getdbt.com/"},
)

staging_render_config = RenderConfig(
    select=["tag:staging"],  # Only run staging models
)

marts_render_config = RenderConfig(
    select=["tag:marts", "tag:metrics"],  # Include all marts models and metrics models
)

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

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

# Create dbt task group for staging models
dbt_staging_task_group = DbtTaskGroup(
    group_id="dbt_staging_models",
    project_config=dbt_project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=staging_render_config,
    operator_args={"full_refresh": True, "install_deps": True},  # Install dependencies and full refresh
    dag=dag,
)

# Create dbt task group for marts models (including dim_date)
dbt_marts_task_group = DbtTaskGroup(
    group_id="dbt_marts_models",
    project_config=dbt_project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=marts_render_config,
    operator_args={"install_deps": True},  # Install dependencies
    dag=dag,
)

# Create quality check task
quality_check_task = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Set up task dependencies
extract_task >> dbt_staging_task_group >> dbt_marts_task_group >> quality_check_task