import datetime
from database.base import create_tables
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata.models import CleanLog
from database import BaseClean
from etl_metadata.setup_db import *
engine_elt, SessionELT = connection_elt()
create_table(engine_elt)

def create_table(engine_clean):
    import clean.models
    create_tables(engine_clean, BaseClean)
def connection_clean():
    clean_log_error = CleanLog(
        status="Failure",
        total_rows=0,
        inserted_rows=0,
        error_msg="Kết nối không thành công đến db_stage_clean",
        process_time=datetime.now()
    )
    engine_clean, SessionClean = setup_database("clean_db_url", lambda: log_dual_status(clean_log_error, SessionELT, "Lỗi kết nối Clean DB", "Không thể kết nối với db_stage_clean"), echo=False)
    return engine_clean, SessionClean

