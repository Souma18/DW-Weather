from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Text
from sqlalchemy.orm import relationship

from database import BaseClean
class User(BaseClean):
    __tablename__ = "User"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính")
    email = Column(String(255), nullable=True)
    name = Column(String(100), nullable=True)