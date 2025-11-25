from datetime import datetime 
from database.base import create_tables
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata.models import CleanLog, TransformLog
from database import BaseClean
from etl_metadata.setup_db import *
engine_elt, SessionELT = connection_elt()
create_table(engine_elt)

def create_table_clean(engine_clean):
    import clean.models
    create_tables(engine_clean, BaseClean)
def connection_clean(is_clean=True, is_transform=True):
    clean_log_error = None
    if is_clean:
        clean_log_error = CleanLog(
        status="FAILED",
        total_rows=0,
        inserted_rows=0,
        error_msg="Kết nối không thành công đến db_stage_clean",
        process_time=datetime.now()
        )
    elif is_transform:
        clean_log_error = TransformLog(
            status="FAILED",
            record_count=0,
            message="Kết nối không thành công đến db_stage_clean",
            start_at=datetime.now(),
            end_at=datetime.now()
    )       
    # 3.6.2 Gửi mail thông báo. Lưu Log thất bại vào table "transform_log" của database "db_etl_metadata"
    engine_clean, SessionClean = setup_database("clean_db_url", lambda: log_dual_status(clean_log_error, SessionELT, "Lỗi kết nối Clean DB", "Không thể kết nối với db_stage_clean"), echo=False)
    return engine_clean, SessionClean

