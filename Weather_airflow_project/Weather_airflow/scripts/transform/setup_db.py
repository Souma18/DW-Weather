from datetime import datetime
from database.base import create_tables
from database import BaseTransform
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata.setup_db import *
from clean.setup_db import connection_clean
from etl_metadata.models import TransformLog
# 3.3 Tạo kết nối với database "db_etl_metadata"
engine_elt, SessionELT = connection_elt()
# 3.4.1 Tạo table "transform_log" nếu chưa có
create_table(engine_elt)
# 3.5 Tạo kết nối với database "db_stage_clean"
engine_clean, SessionClean = connection_clean(is_clean=False)

def create_table_transform(engine_transform):
    import transform.models
    create_tables(engine_transform, BaseTransform)
    
def connection_transform():
    transform_log_error = TransformLog(
        status="FAILED",
        record_count=0,
        message="Kết nối không thành công đến db_stage_transform",
        start_at=datetime.now(),
        end_at=datetime.now()
    )
    # 3.8.2 Lưu Log thất bại vào table "transform_log" của database "db_etl_metadata" 
    engine_transform, SessionTransform = setup_database("transform_db_url", lambda: log_dual_status(transform_log_error, SessionELT, "Lỗi kết nối Transform DB", "Không thể kết nối với db_stage_transform"), echo=False)
    return engine_transform, SessionTransform
