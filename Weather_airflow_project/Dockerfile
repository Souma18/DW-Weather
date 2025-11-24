FROM apache/airflow:2.9.2

USER airflow

# Cài thêm các thư viện Python dùng trong project
COPY dev-requirements.txt /opt/airflow/dev-requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/dev-requirements.txt

# Copy toàn bộ code Airflow vào image
COPY Weather_airflow/dags /opt/airflow/dags
COPY Weather_airflow/scripts /opt/airflow/scripts