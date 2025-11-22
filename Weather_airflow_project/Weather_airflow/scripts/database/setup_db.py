from datetime import datetime
from base import create_engine_and_session, create_tables
from database import BaseELT, BaseClean, BaseTransform
import os
from logger import log_email_status, log_dual_status
from elt_metadata.models import TransformLog, CleanLog
def setup_database(url, logger, echo=False):
    engine, SessionLocal = create_engine_and_session(url, logger, echo=echo)
    return engine, SessionLocal

DEFAULT_ELT_DB_URL = os.getenv(
    "ELT_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_etl_metadata"
)
DEFAULT_CLEAN_DB_URL = os.getenv(
    "CLEAN_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_stage_clean"
)
DEFAULT_TRANSFORM_DB_URL = os.getenv(
    "TRANSFORM_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_stage_transform"
)
DEFAULT_RECIEVER_EMAIL = os.getenv(
    "RECIEVER_EMAIL", "22130080@st.hcmuaf.edu.vn"
)

engine_elt, SessionELT = setup_database(DEFAULT_ELT_DB_URL, 
                                                  lambda: log_email_status(to_email=DEFAULT_RECIEVER_EMAIL,
                                                                           subject= "LỖi hệ thống warehouse",
                                                                            content="Không thể kết nối với db db_elt_metadata"),
                                                                            echo=True)
import elt_metadata.models
create_tables(engine_elt, BaseELT)

clean_log_error = CleanLog(
    status="Failure",
    total_rows=0,
    inserted_rows=0,
    error_msg="Kết nối không thành công đến db_stage_clean",
    process_time=datetime.now()
    )
engine_clean, SessionClean = setup_database(DEFAULT_CLEAN_DB_URL, lambda: log_dual_status(clean_log_error, SessionELT, DEFAULT_RECIEVER_EMAIL, "Lỗi kết nối Clean DB", "Không thể kết nối với db_stage_clean"), echo=False)
import clean.models
create_tables(engine_clean, BaseClean)

transform_log_error = TransformLog(
    status="Failure",
    record_count=0,
    message="Kết nối không thành công đến db_stage_transform",
    start_at=datetime.now(),
    end_at=datetime.now()
    )
engine_transform, SessionTransform = setup_database(DEFAULT_TRANSFORM_DB_URL, lambda: log_dual_status(transform_log_error, SessionELT, DEFAULT_RECIEVER_EMAIL, "Lỗi kết nối Transform DB", "Không thể kết nối với db_stage_transform"), echo=False)
import transform.models
create_tables(engine_transform, BaseTransform)

