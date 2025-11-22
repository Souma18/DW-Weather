from contextlib import contextmanager
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

def create_engine_with_retry(url, logger, max_retry=3, wait_seconds=10, echo=False):
    times = 1
    while times <= max_retry:
        try:
            engine = create_engine(url, echo=echo)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except OperationalError:
            if times == max_retry:
                logger()
                raise
            time.sleep(wait_seconds)
            times += 1

def create_engine_and_session(url, logger, echo=False):
    engine = create_engine_with_retry(url, logger, echo=echo)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine, SessionLocal

def create_tables(engine, Base):
    Base.metadata.create_all(bind=engine)
    
@contextmanager
def session_scope(SessionLocal):
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
        

