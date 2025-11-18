# file: /opt/airflow/dags/extract_dag.py

# Import datetime để đặt ngày bắt đầu cho DAG
from datetime import datetime

# Import lớp DAG của Airflow – dùng để định nghĩa 1 DAG
from airflow import DAG

# Import operator cho phép chạy hàm Python trong DAG
from airflow.operators.python import PythonOperator

# -------------------------------------------------------------------
# Import script nghiệp vụ
# Ở đây ta import hàm run() từ file: /opt/airflow/jobs/extract.py
# DAG sẽ gọi hàm này mỗi lần đến giờ chạy
# -------------------------------------------------------------------
from jobs.extract import run



extract()
# -------------------------------------------------------------------
# Định nghĩa DAG
# with DAG(...) as dag: là cú pháp chuẩn của Airflow để khai báo DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="extract_dag",          # Tên DAG hiển thị trong Airflow UI
    schedule_interval="0 6 * * *", # Cron: chạy lúc 06:00 sáng hàng ngày
    start_date=datetime(2024, 1, 1), # Ngày bắt đầu kích hoạt DAG
    catchup=False                  # Không chạy bù các ngày quá khứ
) as dag:

    # ---------------------------------------------------------------
    # Định nghĩa 1 task trong DAG
    # Task này dùng PythonOperator để gọi hàm run() bạn import phía trên
    # ---------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="run_extract_script",  # Tên task hiển thị trong Airflow
        python_callable=run            # Hàm run() của script sẽ được gọi
    )

    # Nếu có nhiều task, ta có thể thêm dependency kiểu:
    # task1 >> task2
