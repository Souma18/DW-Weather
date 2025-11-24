from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy import Enum
from database import BaseELT


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


class TransformLog(BaseELT):
    __tablename__ = "transform_log"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    status       = Column(String(20), nullable=False)
    record_count = Column(Integer, nullable=True)
    source_name  = Column(String(255), nullable=True)
    table_name   = Column(Text, nullable=True)
    message      = Column(Text, nullable=True)
    start_at     = Column(DateTime, nullable=True)
    end_at       = Column(DateTime, nullable=True)  # Sửa typo: "Thời gianlr" → "Thời gian"


class CleanLog(BaseELT):
    __tablename__ = "clean_log"
    id            = Column(Integer, primary_key=True, autoincrement=True)
    file_name     = Column(String(255), nullable=False)
    process_time  = Column(DateTime, default=datetime.utcnow)
    status        = Column(String(20), nullable=False)
    total_rows    = Column(Integer)
    inserted_rows = Column(Integer)
    error_msg     = Column(Text, nullable=True)
    start_index   = Column(Integer, nullable=True)
    end_index     = Column(Integer, nullable=True)
    fail_range    = Column(String(50), nullable=True)
    table_type    = Column(String(50), nullable=True)