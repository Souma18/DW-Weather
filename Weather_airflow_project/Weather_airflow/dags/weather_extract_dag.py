from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


 # 1.0.0: Khởi chạy dag extract để lấy dữ liệu thời tiết từ JSON và lưu thành CSV vào lúc 6:30 hàng ngày
# Thiết lập PYTHONPATH để import được các module trong thư mục scripts
# /opt/airflow/Weather_airflow/scripts sẽ chứa các package: database, extract, clean, transform, ...
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # Thư mục Weather_airflow
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

from extract.extractor import run as extract_run  # noqa: E402


# 1.0: DAG extract chạy hằng ngày lúc 13:30, là bước đầu tiên của pipeline (EXTRACT)
default_args = {
    "owner": "weather_etl",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="weather_extract_dag",  # 1.0.1: Định nghĩa DAG extract
    description="Extract JSON weather data to CSV (with logging)",
    default_args=default_args,
    start_date=datetime(2025, 11, 24, 1, 0),
    schedule_interval="30 6 * * *",  # 1.0.2: Trigger mỗi ngày lúc 13:30
    catchup=False,
    tags=["weather", "extract"],
) as dag:

    # 1.1: Task duy nhất của DAG – gọi hàm extractor.run() để thực hiện toàn bộ flow extract
    extract_task = PythonOperator(
        task_id="extract_json_to_csv",
        python_callable=extract_run,
    )


