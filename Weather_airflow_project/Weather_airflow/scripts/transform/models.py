"""
ORM models cho schema transform (Dim + Fact tables).

Các bảng định nghĩa theo sơ đồ được cung cấp trong attachments:
- DimLocation
- DimCyclone
- FactHeavyRain
- FactThunderstorm
- FactFog
- FactGale
- FactCycloneTrack

Mỗi model có comment tiếng Việt nhỏ để dễ hiểu và có các relationship cơ bản.
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Text
from sqlalchemy.orm import relationship

from database import BaseTransform


# ------------------------
# Dimension: DimLocation
# ------------------------
class DimLocation(BaseTransform):
    __tablename__ = "dim_location"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính")
    station = Column(String(200), nullable=True, comment="Tên trạm hoặc địa điểm đo")
    lat = Column(Numeric(10, 6), nullable=True, comment="Vĩ độ")
    lon = Column(Numeric(10, 6), nullable=True, comment="Kinh độ")
    hp = Column(Numeric(10, 2), nullable=True, comment="Độ cao hoặc áp suất (tuỳ dữ liệu)")
    country = Column(String(100), nullable=True, comment="Quốc gia")
    createdAt = Column(DateTime, nullable=True)

    # Relationship tới các bảng fact tham chiếu location
    heavy_rains = relationship("FactHeavyRain", back_populates="location", cascade="all, delete-orphan")
    thunderstorms = relationship("FactThunderstorm", back_populates="location", cascade="all, delete-orphan")
    fogs = relationship("FactFog", back_populates="location", cascade="all, delete-orphan")
    gales = relationship("FactGale", back_populates="location", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<DimLocation(id={self.id}, station={self.station})>"


# ------------------------
# Dimension: DimCyclone
# ------------------------
class DimCyclone(BaseTransform):
    __tablename__ = "dim_cyclone"

    id = Column(Integer, primary_key=True, autoincrement=True, comment="Khóa chính định danh cơn bão")
    name = Column(String(100), nullable=True, comment="Tên cơn bão")
    intensity = Column(String(50), nullable=True, comment="Cường độ bão")
    start_time = Column(DateTime, nullable=True, comment="Thời gian bắt đầu")
    latest_time = Column(DateTime, nullable=True, comment="Thời gian cập nhật gần nhất")
    updatedAt = Column(DateTime, nullable=True, comment="Ngày giờ cập nhật bản ghi")

    # Relationship tới bảng track (FactCycloneTrack)
    tracks = relationship("FactCycloneTrack", back_populates="cyclone", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<DimCyclone(id={self.id}, name={self.name})>"


# ------------------------
# Fact tables
# ------------------------
class FactHeavyRain(BaseTransform):
    __tablename__ = "fact_heavy_rain"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True)
    event_datetime = Column(DateTime, nullable=False)
    rainfall_mm = Column(Numeric(10, 2), nullable=True)
    createdAt = Column(DateTime, nullable=True)

    location = relationship("DimLocation", back_populates="heavy_rains")

    def __repr__(self):
        return f"<FactHeavyRain(id={self.id}, location_id={self.location_id}, event={self.event_datetime})>"


class FactThunderstorm(BaseTransform):
    __tablename__ = "fact_thunderstorm"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True)
    event_datetime = Column(DateTime, nullable=False)
    thunderstorms_index = Column(Numeric(10, 2), nullable=True)
    createdAt = Column(DateTime, nullable=True)

    location = relationship("DimLocation", back_populates="thunderstorms")

    def __repr__(self):
        return f"<FactThunderstorm(id={self.id}, location_id={self.location_id})>"


class FactFog(BaseTransform):
    __tablename__ = "fact_fog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True)
    event_datetime = Column(DateTime, nullable=False)
    fog_index = Column(String(50), nullable=True)
    visibility = Column(String(50), nullable=True)
    createdAt = Column(DateTime, nullable=True)

    location = relationship("DimLocation", back_populates="fogs")

    def __repr__(self):
        return f"<FactFog(id={self.id}, location_id={self.location_id})>"


class FactGale(BaseTransform):
    __tablename__ = "fact_gale"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True)
    event_datetime = Column(DateTime, nullable=False)
    knots = Column(Numeric(10, 2), nullable=True)
    ms = Column(Numeric(10, 2), nullable=True)
    degrees = Column(Integer, nullable=True)
    direction = Column(String(50), nullable=True)
    createdAt = Column(DateTime, nullable=True)

    location = relationship("DimLocation", back_populates="gales")

    def __repr__(self):
        return f"<FactGale(id={self.id}, location_id={self.location_id})>"


class FactCycloneTrack(BaseTransform):
    __tablename__ = "fact_cyclone_track"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cyclone_id = Column(Integer, ForeignKey("dim_cyclone.id"), nullable=False, index=True)
    event_datetime = Column(DateTime, nullable=False)
    lat = Column(Numeric(10, 6), nullable=True)
    lon = Column(Numeric(10, 6), nullable=True)
    intensity = Column(String(50), nullable=True)
    pressure = Column(Numeric(10, 2), nullable=True)
    max_wind_speed = Column(Numeric(10, 2), nullable=True)
    gust = Column(Numeric(10, 2), nullable=True)
    speed_of_movement = Column(Numeric(10, 2), nullable=True)
    movement_direction = Column(String(50), nullable=True)
    wind_radis = Column(Text, nullable=True)
    center_id = Column(String(50), nullable=True)
    createdAt = Column(DateTime, nullable=True)

    cyclone = relationship("DimCyclone", back_populates="tracks")

    def __repr__(self):
        return f"<FactCycloneTrack(id={self.id}, cyclone_id={self.cyclone_id})>"

