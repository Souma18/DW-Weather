import datetime
from sqlalchemy import Column, Integer, DateTime, String, Text
from database import BaseELT

class TransformLog(BaseELT):
    __tablename__ = "transform_log"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính log")
    status = Column(String(20), nullable=False, comment="Trạng thái bước transform")
    record_count = Column(Integer, nullable=True, comment="Số bản ghi đã xử lý")
    source_name = Column(String(255), nullable=False, comment="File hoặc bảng nguồn")
    source_type = Column(String(50), nullable=False, comment="Loại nguồn: file/csv/db/table")
    mapping_info = Column(Text, nullable=True, comment="Thông tin ánh xạ cột nguồn -> cột đích, dạng JSON")
    first_processed_id = Column(Integer, nullable=True, comment="ID bản ghi đầu đã xử lý")
    last_processed_id = Column(Integer, nullable=True, comment="ID bản ghi cuối cùng đã xử lý")
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
