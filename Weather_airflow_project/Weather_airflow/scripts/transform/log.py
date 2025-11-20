from datetime import datetime
from sqlalchemy import Column, Integer, DateTime, String, Text
from database import BaseELT

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
    success_range = Column(String(50), nullable=True)  
    fail_range = Column(String(50), nullable=True) 
    table_type = Column(String(50), nullable=True) 