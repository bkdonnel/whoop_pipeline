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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Create test DAG
dag = DAG(
    'whoop_pipeline_cosmos_test',
    default_args=default_args,
    description='Test Cosmos dbt Integration with Single Model',
    schedule_interval=None,  # Manual trigger for testing
    catchup=False,
    max_active_runs=1,
)

# Simplified test task to verify Variables work
def test_variables(**context):
    print("Testing Airflow Variables access...")
    try:
        account = Variable.get('SNOWFLAKE_ACCOUNT')
        user = Variable.get('SNOWFLAKE_USER')
        warehouse = Variable.get('SNOWFLAKE_WAREHOUSE')
        database = Variable.get('SNOWFLAKE_DATABASE', default_var='whoop')
        role = Variable.get('SNOWFLAKE_ROLE')
        
        print(f"✅ SNOWFLAKE_ACCOUNT: {account}")
        print(f"✅ SNOWFLAKE_USER: {user}")
        print(f"✅ SNOWFLAKE_WAREHOUSE: {warehouse}")
        print(f"✅ SNOWFLAKE_DATABASE: {database}")
        print(f"✅ SNOWFLAKE_ROLE: {role}")
        print("✅ All variables accessible")
        return "Variables test passed"
    except Exception as e:
        print(f"❌ Variables test failed: {e}")
        raise

# Create test task
test_vars_task = PythonOperator(
    task_id='test_variables',
    python_callable=test_variables,
    dag=dag,
)

# Cosmos dbt configuration
dbt_project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/include/dbt",
)

render_config = RenderConfig(
    select=["stg_user"],  # Only test one model
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

# Create dbt task group for just ONE staging model (stg_user is simplest)
dbt_test_task_group = DbtTaskGroup(
    group_id="dbt_test_single_model",
    project_config=dbt_project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    dag=dag,
)

# Final test task
def test_completion(**context):
    print("✅ Cosmos dbt test completed successfully!")
    return "Test completed"

completion_task = PythonOperator(
    task_id='test_completion',
    python_callable=test_completion,
    dag=dag,
)

# Set up test dependencies
test_vars_task >> dbt_test_task_group >> completion_task