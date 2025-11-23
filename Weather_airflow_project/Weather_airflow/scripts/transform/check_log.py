from sqlalchemy import inspect
from database.setup_db import SessionELT
from database.base import session_scope
from database.logger import log_dual_status
from elt_metadata.models import CleanLog, TransformLog
from datetime import datetime

def row_to_dict(row):
    return {c.key: getattr(row, c.key) for c in inspect(row).mapper.column_attrs}
today_start = datetime.combine(datetime.today(), datetime.min.time())
success_logs = []
with session_scope(SessionELT) as session:
    success_logs = session.query(CleanLog).filter(
        CleanLog.status.in_(["SUCCESS"])
    ).all()
    success_logs = [row_to_dict(r) for r in success_logs]  # convert ORM → dict

    if not success_logs:
        transform_log = TransformLog(
            status="Failure",
            record_count=0,
            message="Hôm nay job clean chưa có dữ liệu mới.",
            start_at=today_start,
            end_at=datetime.now()
        )
        log_dual_status(transform_log, SessionELT,
                        to_email="minhhien7840@gmail.com", subject="Lỗi hệ thống DW-Weather",
                        content="Hôm nay job clean chưa có dữ liệu mới.")