"""
DAG load dữ liệu từ MySQL sang BigQuery tạm thời được vô hiệu hóa
để tập trung test 3 bước đầu tiên: extract -> clean -> transform.

File này vẫn được Airflow scan nhưng KHÔNG tạo DAG nào,
nên sẽ không có job load nào được trigger.

Khi cần bật lại, bạn có thể restore nội dung DAG cũ
hoặc viết DAG load mới cho chuẩn hơn.
"""

# Intentionally left blank: no DAG is defined here.