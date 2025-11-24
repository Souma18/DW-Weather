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

from clean.clean_process import run_clean_and_insert_all  # noqa: E402


default_args = {
    "owner": "weather_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="weather_clean_dag",
    description="Clean raw CSV weather data into staging tables",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 1, 30),
    schedule_interval="35 13 * * *",  # Hằng ngày lúc 01:30 (sau extract)
    catchup=False,
    tags=["weather", "clean"],
) as dag:

    clean_task = PythonOperator(
        task_id="clean_raw_csv_to_staging",
        python_callable=run_clean_and_insert_all,
    )


