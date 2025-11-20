from database import SessionELT, session_scope
from log import CleanLog, TransformLog
from datetime import datetime

today_start = datetime.combine(datetime.today(), datetime.min.time())
today_end = datetime.combine(datetime.today(), datetime.max.time())
success_logs = []
with session_scope(SessionELT) as session:
    success_logs = session.query(CleanLog).filter(
        CleanLog.status == "Success",
        CleanLog.process_time >= today_start,
        CleanLog.process_time <= today_end
    ).all()
    if not success_logs:
        transform_log = TransformLog(
            status="Failure",
            record_count=0,
            message="Hôm nay job clean chưa có dữ liệu mới.",
            start_at=datetime.combine(datetime.today(), datetime.min.time()),
            end_at=datetime.now()
        )
        session.add(transform_log)
