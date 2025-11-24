import datetime
from database.base import create_tables
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata import *
from etl_metadata.models import TransformLog
from sqlalchemy.orm import declarative_base
BaseTransform = declarative_base()
transform_log_error = TransformLog(
    status="Failure",
    record_count=0,
    message="Kết nối không thành công đến db_stage_transform",
    start_at=datetime.now(),
    end_at=datetime.now()
    )
engine_transform, SessionTransform = setup_database("transform_db_url", lambda: log_dual_status(transform_log_error, SessionELT, "Lỗi kết nối Transform DB", "Không thể kết nối với db_stage_transform"), echo=False)
from transform.models import *
def create_table():
    create_tables(engine_transform, BaseTransform)
