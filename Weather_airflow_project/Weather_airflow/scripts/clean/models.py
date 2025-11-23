from database import BaseClean
from sqlalchemy import Column, Integer, Float, String, DateTime

class FogRecord(BaseClean):
    __tablename__ = "fog_records"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String(200), nullable=False)  
    lat = Column(Integer, nullable=False)
    lon = Column(Integer, nullable=False)
    hp = Column(Float, nullable=True)
    country = Column(String(100), nullable=False)
    fog = Column(String(10), nullable=False)
    visibility = Column(Float, nullable=True)
    datetime = Column(DateTime, nullable=False)