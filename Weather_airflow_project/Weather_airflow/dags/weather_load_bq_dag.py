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

from load.load_to_bigquery import WeatherLoadToBigQuery  # noqa: E402


def run_weather_load_to_bq() -> None:
    """
    Wrapper để gọi class WeatherLoadToBigQuery trong PythonOperator.
    """
    loader = WeatherLoadToBigQuery()
    loader.run()


default_args = {
    "owner": "weather_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="weather_load_bq_dag",
    description="Load transformed weather data from MySQL staging to BigQuery",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 2, 30),
    schedule_interval="30 2 * * *",  # Hằng ngày lúc 02:30 (sau transform)
    catchup=False,
    tags=["weather", "load", "bigquery"],
) as dag:

    load_bq_task = PythonOperator(
        task_id="load_weather_to_bigquery",
        python_callable=run_weather_load_to_bq,
    )


