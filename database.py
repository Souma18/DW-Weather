from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Connection configuration (update user/pass/db as needed)
DATABASE_URL = "mysql+pymysql://root:1@localhost/db_stage_clean"

engine = create_engine(
    DATABASE_URL,
    echo=True  # Set False to disable SQL log output
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_session():
    """Create a new SQLAlchemy session."""
    return SessionLocal()


def init_db() -> None:
    """Import models module to register models and create all tables."""
    try:
        import models  # noqa: F401
    except ImportError:
        # If models can't be imported, attempt to create tables for registered models
        pass

    Base.metadata.create_all(bind=engine)