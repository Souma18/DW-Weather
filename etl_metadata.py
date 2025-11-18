from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

LOG_DB_URL = "mysql+pymysql://root:1@localhost/db_etl_metadata"

log_engine = create_engine(LOG_DB_URL, echo=False)
LogBase = declarative_base()
LogSessionLocal = sessionmaker(bind=log_engine, autocommit=False, autoflush=False)


class CleanLog(LogBase):
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

    def __repr__(self):
        return (
            f"<CleanLog(id={self.id}, file_name={self.file_name}, "
            f"status={self.status}, process_time={self.process_time})>"
        )


def init_log_db() -> None:
    """Create log database tables if not exist."""
    LogBase.metadata.create_all(bind=log_engine)