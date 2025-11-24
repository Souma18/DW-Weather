from database.base import create_tables
from database.logger import  log_email_status
from database.setup_db import setup_database
from database import BaseELT

def create_table(engine_elt):
    import etl_metadata.models
    create_tables(engine_elt, BaseELT)
    
def connection_elt():
     engine_elt, SessionELT = setup_database("elt_db_url", 
                                                  lambda: log_email_status(subject= "LỖi hệ thống warehouse",
                                                                            content="Không thể kết nối với db db_elt_metadata"),
                                                                            echo=False)
     return engine_elt, SessionELT