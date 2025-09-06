from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]

def create_datasets():
    client = bigquery.Client(project=PROJECT_ID)
    for ds in ["drilldown_raw","drilldown_core","drilldown_mart"]:
        ds_id = f"{PROJECT_ID}.{ds}"
        try:
            client.get_dataset(ds_id)
        except Exception:
            client.create_dataset(bigquery.Dataset(ds_id), exists_ok=True)

def dq_gate():
    client = bigquery.Client(project=PROJECT_ID)
    fresh = list(client.query(f"""
        SELECT DATE_DIFF(CURRENT_DATE(), MAX(invoice_date), DAY) AS days_since_latest
        FROM `{PROJECT_ID}.drilldown_core.fact_sales`
    """).result())[0]["days_since_latest"]
    negs = list(client.query(f"""
        SELECT COUNTIF(line_amount < 0) AS negative_lines
        FROM `{PROJECT_ID}.drilldown_core.fact_sales`
    """).result())[0]["negative_lines"]

    problems = []
    # For demo/test data, allow data up to 2 years old (730 days)
    if fresh is None or (isinstance(fresh, int) and fresh > 730):
        problems.append(f"stale data: days_since_latest={fresh}")
    if negs and int(negs) > 0:
        problems.append(f"negative line amounts: {negs}")
    if problems:
        raise ValueError("DQ FAILED: " + " | ".join(problems))

default_args = {"start_date": datetime(2025,1,1)}

with DAG(
    dag_id="dbt_end_to_end",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Seeds raw -> dbt core/marts -> tests -> DQ gate",
) as dag:

    t0 = PythonOperator(task_id="create_datasets", python_callable=create_datasets)

    t1 = BashOperator(task_id="dbt_deps", bash_command="cd /opt/airflow/dbt && dbt deps")

    # keep your seed target as 'raw' to land in drilldown_raw (matches your profiles.yml)
    t2 = BashOperator(task_id="dbt_seed", bash_command="cd /opt/airflow/dbt && dbt seed --target raw")

    # build core models first
    t3 = BashOperator(task_id="dbt_run_core",  bash_command="cd /opt/airflow/dbt && dbt run --target core --select core")
    
    # build mart models (depends on core models)
    t4 = BashOperator(task_id="dbt_run_marts",  bash_command="cd /opt/airflow/dbt && dbt run --target mart --select marts")

    # test core models
    t5 = BashOperator(task_id="dbt_test_core", bash_command="cd /opt/airflow/dbt && dbt test --target core --select core")
    
    # test mart models
    t6 = BashOperator(task_id="dbt_test_marts", bash_command="cd /opt/airflow/dbt && dbt test --target mart --select marts")

    t7 = PythonOperator(task_id="dq_gate", python_callable=dq_gate)

    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
