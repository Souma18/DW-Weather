"""
database.py
Cấu hình kết nối database, tạo `engine`, `SessionLocal` và `Base` chung.
"""
from contextlib import contextmanager
import os
import time
from MySQLdb import OperationalError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DEFAULT_ELT_DB_URL = os.getenv(
    "ELT_DB_URL", "mysql+mysqldb://root:1@host:3306/db_elt_metadata"
)
DEFAULT_CLEAN_DB_URL = os.getenv(
    "CLEAN_DB_URL", "mysql+mysqldb://root:1@host:3306/db_stage_clean"
)
DEFAULT_TRANSFORM_DB_URL = os.getenv(
    "TRANSFORM_DB_URL", "mysql+mysqldb://root:1@host:3306/db_stage_transform"
)

def create_engine_and_session(url: str, echo: bool = False):
    engine = create_engine_with_retry(url, echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine, SessionLocal

def create_engine_with_retry(url: str, max_retry: int = 3, wait_seconds: int = 300, echo: bool = False):
    times = 1
    while times <= max_retry:
        try:
            engine = create_engine(url, echo=echo)
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            return engine
        except OperationalError as e:
            if times == max_retry:
                raise
            time.sleep(wait_seconds)
            times += 1

def create_base(engine):
    Base= declarative_base()
    Base.metadata.bind = engine
    Base.metadata.create_all()
    return Base

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
        
engine_elt, SessionELT = create_engine_and_session(DEFAULT_ELT_DB_URL)
engine_clean, SessionClean = create_engine_and_session(DEFAULT_CLEAN_DB_URL)
engine_transform, SessionTransform = create_engine_and_session(DEFAULT_TRANSFORM_DB_URL)
BaseELT = create_base(engine_elt)
BaseClean = create_base(engine_clean)
BaseTransform = create_base(engine_transform)