"""
database.py
Cấu hình kết nối database, tạo `engine`, `SessionLocal` và `Base` chung.
"""
from contextlib import contextmanager
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Declarative base chia sẻ giữa modules
Base = declarative_base()

# Mặc định (có thể override bằng biến môi trường)
DEFAULT_CLEAN_DB_URL = os.getenv(
    "CLEAN_DB_URL", "mysql+mysqldb://root:1@host:3306/db_clean"
)
DEFAULT_TRANSFORM_DB_URL = os.getenv(
    "TRANSFORM_DB_URL", "mysql+mysqldb://root:1@host:3306/db_stage_transform"
)


def create_engine_and_session(url: str, echo: bool = False):
    engine = create_engine(url, echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine, SessionLocal


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
