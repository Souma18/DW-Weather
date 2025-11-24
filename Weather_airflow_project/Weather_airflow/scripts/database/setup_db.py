from datetime import datetime
from database.base import create_engine_and_session, create_tables
from . import BaseELT, BaseClean, BaseTransform
from utils.file_utils import extract_values_from_json
import os
from database.logger import log_email_status, log_dual_status
from etl_metadata.models import TransformLog, CleanLog
def setup_database(config_name, logger, echo=False):
    db_url = extract_values_from_json(r'data\config\config.json', 'db_url')
    url = db_url[config_name]
    engine, SessionLocal = create_engine_and_session(url, logger, echo=echo)
    return engine, SessionLocal

db_url = extract_values_from_json(r'data\config\config.json', 'db_url')
DEFAULT_ELT_DB_URL =  db_url['elt_db_url']
DEFAULT_CLEAN_DB_URL = db_url['clean_db_url']
DEFAULT_TRANSFORM_DB_URL = db_url['transform_db_url']

DEFAULT_RECIEVER_EMAIL = os.getenv(
    "RECIEVER_EMAIL", "minhhien7840@gmail.com"
)

engine_elt, SessionELT = create_engine_and_session(DEFAULT_ELT_DB_URL, 
                                                  lambda: log_email_status(to_email=DEFAULT_RECIEVER_EMAIL,
                                                                           subject= "LỖi hệ thống warehouse",
                                                                            content="Không thể kết nối với db db_elt_metadata"),
                                                                            echo=False)
import etl_metadata.models
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

