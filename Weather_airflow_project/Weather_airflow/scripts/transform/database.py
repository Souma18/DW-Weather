"""
database.py
Cấu hình kết nối database, tạo `engine`, `SessionLocal` và `Base` chung.
"""
# mail cho toàn bộ fail
from contextlib import contextmanager
import os
import time
from MySQLdb import OperationalError
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
# file
DEFAULT_ELT_DB_URL = os.getenv(
    "ELT_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_etl_metadata"
)
DEFAULT_CLEAN_DB_URL = os.getenv(
    "CLEAN_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_stage_clean"
)
DEFAULT_TRANSFORM_DB_URL = os.getenv(
    "TRANSFORM_DB_URL", "mysql+mysqldb://root:1@localhost:3306/db_stage_transform"
)

def create_engine_and_session(url: str, logger, echo: bool = True):
    engine = create_engine_with_retry(url, logger=logger, echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine, SessionLocal

def create_engine_with_retry(url: str, logger, max_retry: int = 3, wait_seconds: int = 300, echo: bool = False):
    times = 1
    while times <= max_retry:
        try:
            engine = create_engine(url, echo=echo)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except OperationalError as e:
            if times == max_retry:
                # email hoặc log
                logger()
                break
            time.sleep(wait_seconds)
            times += 1
            
@contextmanager
def session_scope(SessionLocal):
    """Context manager cho session: commit khi ok, rollback khi lỗi, luôn close."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
def logger():
    print("Cannot connect to database after several retries.")
engine_elt, SessionELT = create_engine_and_session(DEFAULT_ELT_DB_URL, logger=logger)
engine_clean, SessionClean = create_engine_and_session(DEFAULT_CLEAN_DB_URL, logger=logger)
engine_transform, SessionTransform = create_engine_and_session(DEFAULT_TRANSFORM_DB_URL, logger=logger)
BaseELT  = declarative_base()
BaseClean  = declarative_base()
BaseTransform = declarative_base()