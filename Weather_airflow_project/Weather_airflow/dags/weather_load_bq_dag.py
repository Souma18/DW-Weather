"""
DAG load dữ liệu từ MySQL (db_stage_transform) sang BigQuery.

Sau khi đã chạy xong 3 bước extract -> clean -> transform, DAG này
sẽ đọc các mapping từ bảng etl_metadata.mapping_info và thực hiện
load Dim/Fact lên BigQuery theo cấu hình (full / incremental).
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


# Thiết lập PYTHONPATH để import được các module trong thư mục scripts
# /opt/airflow/Weather_airflow/scripts sẽ chứa các package: database, extract, clean, transform, load, ...
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # Thư mục Weather_airflow
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from load.load_to_bigquery import WeatherLoadToBigQuery  # noqa: E402


def run_weather_load_to_bq() -> None:
    """
    Entrypoint cho DAG load: gọi lớp WeatherLoadToBigQuery và chạy toàn bộ flow.
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
    description="Load Dim/Fact tables from MySQL (stage transform) to BigQuery",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 3, 0),
    # Chạy sau bước transform một chút (sau 13:40)
    schedule_interval="0 8 * * *",
    catchup=False,
    tags=["weather", "load", "bigquery"],
) as dag:

    load_to_bq_task = PythonOperator(
        task_id="load_mysql_to_bigquery",
        python_callable=run_weather_load_to_bq,
    )
