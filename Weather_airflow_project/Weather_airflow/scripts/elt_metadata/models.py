from datetime import datetime
from sqlalchemy import CHAR, VARCHAR, Column, BigInteger, Integer, String, Date, DateTime, Text, ForeignKey
from database import BaseELT
import uuid
from sqlalchemy.orm import relationship

class TransformLog(BaseELT):
    __tablename__ = "transform_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính log")
    status = Column(String(20), nullable=False, comment="Trạng thái bước transform")
    record_count = Column(Integer, nullable=True, comment="Số bản ghi đã xử lý")
    source_name = Column(String(255), nullable=True, comment="File hoặc bảng nguồn, JSON")
    table_name = Column(Text, nullable=True, comment="Thông tin bảng đích đã ghi dữ liệu")
    message = Column(Text, nullable=True, comment="Chi tiết log, cảnh báo/lỗi")
    start_at = Column(DateTime, nullable=True, comment="Thời gian bắt đầu")
    end_at = Column(DateTime, nullable=True, comment="Thời gian kết thúc")
    
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
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime)

    events = relationship("LogExtractEvent", back_populates="run")

class LogExtractEvent(BaseELT):
    __tablename__ = "log_extract_event"

    id = Column(VARCHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    run_id = Column(VARCHAR, ForeignKey("log_extract_run.id"), nullable=False)
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
    created_at = Column(DateTime, nullable=False)

    run = relationship("LogExtractRun", back_populates="events")
