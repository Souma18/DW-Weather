from database.base import create_tables
from database.logger import  log_email_status
from database.setup_db import setup_database
from sqlalchemy.orm import declarative_base

BaseELT = declarative_base()
engine_elt, SessionELT = setup_database("elt_db_url", 
                                                  lambda: log_email_status(subject= "LỖi hệ thống warehouse",
                                                                            content="Không thể kết nối với db db_elt_metadata"),
                                                                            echo=False)
from etl_metadata.models import *
def create_table():
    create_tables(engine_elt, BaseELT)