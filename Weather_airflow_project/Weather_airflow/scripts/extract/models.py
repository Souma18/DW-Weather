# models.py
import uuid
from sqlalchemy import CHAR, VARCHAR, Column, BigInteger, Integer, String, Date, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from db import Base
from datetime import datetime

class LogExtractRun(Base):
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

class LogExtractEvent(Base):
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
