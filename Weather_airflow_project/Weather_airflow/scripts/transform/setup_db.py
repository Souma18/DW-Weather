import datetime
from database.base import create_tables
from database import BaseTransform
from database.logger import log_dual_status
from database.setup_db import setup_database
from etl_metadata.setup_db import *
from clean.setup_db import connection_clean
from etl_metadata.models import TransformLog
from sqlalchemy.orm import declarative_base
engine_elt, SessionELT = connection_elt()
create_table(engine_elt)
engine_clean, SessionClean = connection_clean()

def create_table(engine_transform):
    import transform.models
    create_tables(engine_transform, BaseTransform)
    
def connection_transform():
    transform_log_error = TransformLog(
        status="Failure",
        record_count=0,
        message="Kết nối không thành công đến db_stage_transform",
        start_at=datetime.now(),
        end_at=datetime.now()
    )
    engine_transform, SessionTransform = setup_database("transform_db_url", lambda: log_dual_status(transform_log_error, SessionELT, "Lỗi kết nối Transform DB", "Không thể kết nối với db_stage_transform"), echo=False)
    return engine_transform, SessionTransform
