from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


# Thiết lập PYTHONPATH để import được các module trong thư mục scripts
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # Weather_airflow
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from extract.extractor import run as extract_run  # noqa: E402


default_args = {
    "owner": "weather_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="weather_extract_dag",
    description="Extract JSON weather data to CSV (with logging)",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 1, 0),
    schedule_interval="0 1 * * *",  # Hằng ngày lúc 01:00
    catchup=False,
    tags=["weather", "extract"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_json_to_csv",
        python_callable=extract_run,
    )


