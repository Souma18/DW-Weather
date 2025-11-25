from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
load_dotenv()
BaseELT = declarative_base()
BaseClean = declarative_base()
BaseTransform = declarative_base()
