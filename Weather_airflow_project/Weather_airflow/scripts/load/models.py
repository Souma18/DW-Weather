from datetime import datetime
from sqlalchemy import Enum
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, Date, VARCHAR, ForeignKey
)
from sqlalchemy.orm import relationship
from database import BaseELT
import uuid


# 1. Bảng log quá trình Clean
class CleanLog(BaseELT):
    __tablename__ = "clean_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    process_time = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), nullable=False)
    total_rows = Column(Integer)
    inserted_rows = Column(Integer)
    error_msg = Column(Text, nullable=True)
    start_index = Column(Integer, nullable=True)
    end_index = Column(Integer, nullable=True)
    fail_range = Column(String(50), nullable=True)
    table_type = Column(String(50), nullable=True)


# 2. Bảng cấu hình Load (tên đúng chuẩn: LoadLog)

class MappingInfo(BaseELT):
    __tablename__ = "mapping_info"

    id               = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính tự tăng")
    source_table     = Column(String(100), nullable=False, comment="Tên bảng nguồn trong db_stage_transform")
    target_table     = Column(String(100), nullable=False, comment="Tên bảng đích trong BigQuery")
    timestamp_column = Column(String(100), nullable=True, comment="Cột timestamp để incremental (NULL = full load)")
    load_type        = Column(Enum('full', 'incremental', name='load_type_enum'), nullable=False, default='incremental')
    load_order       = Column(Integer, default=99, comment="Thứ tự chạy (số nhỏ chạy trước)")
    is_active        = Column(Boolean, default=True, comment="Bật/tắt bảng này")
    note             = Column(String(255), nullable=True, comment="Ghi chú thêm")


class LoadLog(BaseELT):
    __tablename__ = "load_log"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    status       = Column(String(20), nullable=False)
    record_count = Column(Integer, nullable=True)
    source_name  = Column(String(255), nullable=True)
    table_name   = Column(Text, nullable=True)
    message      = Column(Text, nullable=True)
    start_at     = Column(DateTime, nullable=True)
    end_at       = Column(DateTime, nullable=True)


# 3. Bảng log quá trình Transform (dùng trong load_to_bigquery.py)
class TransformLog(BaseELT):
    __tablename__ = "transform_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính log")
    status = Column(String(20), nullable=False, comment="Trạng thái: SUCCESS / FAILED / SKIP")
    record_count = Column(Integer, nullable=True, comment="Số bản ghi đã xử lý")
    source_name = Column(String(255), nullable=True, comment="Tên bảng/file nguồn")
    table_name = Column(Text, nullable=True, comment="Tên bảng đích trong BigQuery")
    message = Column(Text, nullable=True, comment="Chi tiết log, lỗi, cảnh báo")
    start_at = Column(DateTime, nullable=True, comment="Thời gian bắt đầu")
    end_at = Column(DateTime, nullable=True, comment="Thời gian kết thúc")


# 4. Bảng log chạy Extract (job tổng)
class LogExtractRun(BaseELT):
    __tablename__ = "log_extract_run"

    id = Column(VARCHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_name = Column(String(100), nullable=False)
    run_date = Column(Date, nullable=False)
    run_number = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime)
    status = Column(String(20), nullable=False)
    total_links = Column(Integer)
    success_count = Column(Integer)
    fail_count = Column(Integer)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, onupdate=datetime.utcnow)

    events = relationship("LogExtractEvent", back_populates="run", cascade="all, delete-orphan")


# 5. Bảng log chi tiết từng link Extract
class LogExtractEvent(BaseELT):
    __tablename__ = "log_extract_event"

    id = Column(VARCHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(VARCHAR(36), ForeignKey("log_extract_run.id", ondelete="CASCADE"), nullable=False)
    step = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)
    url = Column(Text)
    file_name = Column(String(255))
    data_type = Column(String(100))
    sys_id = Column(String(50))
    record_count = Column(Integer)
    error_code = Column(String(50))
    error_message = Column(Text)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    run = relationship("LogExtractRun", back_populates="events")