from etl_metadata.setup_db import *

# 1.0.1 (EXTRACT) - Import hàm kết nối & tạo bảng metadata dùng chung cho flow Extract
engine_elt, SessionELT = connection_elt()  # 1.0.2 (EXTRACT) - Khởi tạo engine + SessionELT kết nối DB log ETL

# 1.0.3 (EXTRACT) - Tạo/cập nhật schema bảng metadata (log_extract_run, log_extract_event, ...)
create_table(engine_elt)
