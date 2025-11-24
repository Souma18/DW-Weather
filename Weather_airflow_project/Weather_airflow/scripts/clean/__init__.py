import datetime
from database.base import create_tables
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata import *
from etl_metadata.models import CleanLog
from sqlalchemy.orm import declarative_base
BaseClean = declarative_base()

clean_log_error = CleanLog(
    status="Failure",
    total_rows=0,
    inserted_rows=0,
    error_msg="Kết nối không thành công đến db_stage_clean",
    process_time=datetime.now()
    )
engine_clean, SessionClean = setup_database("clean_db_url", lambda: log_dual_status(clean_log_error, SessionELT, "Lỗi kết nối Clean DB", "Không thể kết nối với db_stage_clean"), echo=False)
from clean.models  import *
def create_table():
    create_tables(engine_clean, BaseClean)


