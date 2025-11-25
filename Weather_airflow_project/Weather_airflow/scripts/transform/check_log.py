from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from transform.setup_db import SessionELT
from database.base import session_scope
from database.logger import log_dual_status
from etl_metadata.models import CleanLog, TransformLog
from utils.date_utils import date_now

today_start = date_now()

@dataclass
class CleanLogObj:
    id: int
    file_name: str
    process_time: datetime
    status: str
    total_rows: Optional[int] = None
    inserted_rows: Optional[int] = None
    error_msg: Optional[str] = None
    start_index: Optional[int] = None
    end_index: Optional[int] = None
    fail_range: Optional[str] = None
    table_type: Optional[str] = None

def get_success_logs():
    """
    Lấy danh sách logs có status 'CLEANED' dưới dạng object detached (không link ORM)
    """
    with session_scope(SessionELT) as session:
        logs = session.query(CleanLog).filter(CleanLog.status == "CLEANED").all()

        if not logs:
            transform_log = TransformLog(
                status="FAILED",
                record_count=0,
                message="Hôm nay job clean chưa có dữ liệu mới.",
                start_at=today_start,
                end_at=date_now()
            )
            # 3.9.2 Lưu Log thất bại vào table "transform_log" của database "db_etl_metadata" 
            log_dual_status(transform_log, SessionELT,
                            subject="Lỗi hệ thống DW-Weather",
                            content="Hôm nay job clean chưa có dữ liệu mới.")
            return []

        # Chuyển sang object detached
        detached_logs = [
            CleanLogObj(
                id=log.id,
                file_name=log.file_name,
                process_time=log.process_time,
                status=log.status,
                total_rows=log.total_rows,
                inserted_rows=log.inserted_rows,
                error_msg=log.error_msg,
                start_index=log.start_index,
                end_index=log.end_index,
                fail_range=log.fail_range,
                table_type=log.table_type,
            )
            for log in logs
        ]
        # 3.9.1 Trả lại danh sách success_logs
        return detached_logs
