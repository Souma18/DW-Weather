# db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DB_URL = "mysql+mysqlconnector://root:01252331055@localhost/db_etl_metadata"

engine = create_engine(DB_URL, echo=False, pool_recycle=3600)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

Base = declarative_base()

def get_session():
    return SessionLocal()
