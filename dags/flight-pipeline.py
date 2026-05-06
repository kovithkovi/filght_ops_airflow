from scripts.bronze_ingest import bronze_ingest
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
import sys
from pathlib import Path
AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))


default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="flight_ops_medallion_pipeline",
    default_args=default_args,
    description="A simple flight data pipeline",
    start_date=datetime(2026, 5, 6),
    schedule="30 * * * *",
    catchup=False
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=bronze_ingest
    )
