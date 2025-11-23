# log_service.py
from datetime import datetime
from elt_metadata.models import LogExtractRun, LogExtractEvent

class LogService:
    """
    Service để quản lý log extract: LogExtractRun và LogExtractEvent
    """
    def __init__(self, session):
        self.session = session

    # -------------------- RUN --------------------
    def create_run(self, job_name: str, run_number: int,
                   started_at: datetime, status: str,
                   created_at: datetime) -> LogExtractRun:
        """
        Tạo mới một log run
        """
        run = LogExtractRun(
            job_name=job_name,
            run_date=started_at.date(),   # lấy ngày từ started_at
            run_number=run_number,
            started_at=started_at,
            status=status,
            created_at=created_at
        )
        self.session.add(run)
        return run

    def update_run(self, run: LogExtractRun, **kwargs) -> None:
        """
        Cập nhật thông tin log run
        """
        for key, value in kwargs.items():
            if hasattr(run, key):
                setattr(run, key, value)
        run.updated_at = datetime.now()

    # -------------------- EVENT --------------------
    def create_event(self, run: LogExtractRun, step: str, status: str,
                     url: str = None, file_name: str = None,
                     data_type: str = None, sys_id: str = None,
                     record_count: int = None, error_code: str = None,
                     error_message: str = None, started_at: datetime = None,
                     finished_at: datetime = None, created_at: datetime = None) -> LogExtractEvent:
        """
        Tạo mới một log event gắn với một run
        """
        if created_at is None:
            created_at = datetime.now()
        if started_at is None:
            started_at = datetime.now()

        event = LogExtractEvent(
            run_id=run.id,
            step=step,
            status=status,
            url=url,
            file_name=file_name,
            data_type=data_type,
            sys_id=sys_id,
            record_count=record_count,
            error_code=error_code,
            error_message=error_message,
            started_at=started_at,
            finished_at=finished_at,
            created_at=created_at
        )
        self.session.add(event)
        return event
