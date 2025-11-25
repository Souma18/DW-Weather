from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


# Thiết lập PYTHONPATH để import được các module trong thư mục scripts
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # Thư mục Weather_airflow
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from transform.transform import run_transform  # noqa: E402


default_args = {
    "owner": "weather_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# 3.1. Khởi chạy tự động transform lúc 7h30
with DAG(
    dag_id="weather_transform_dag",
    description="Transform clean staging data into Dim/Fact tables",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 2, 0),
    schedule_interval="30 7 * * *",
    catchup=False,
    tags=["weather", "transform"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_staging_to_dim_fact",
        python_callable=run_transform,
    )


