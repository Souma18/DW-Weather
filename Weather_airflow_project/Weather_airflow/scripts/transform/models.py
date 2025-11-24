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
    sys_id = Column(Integer, nullable=True, comment="ID hệ thống")
    name = Column(String(100), nullable=True, comment="Tên cơn bão")
    storm_id = Column(String(50), nullable=True, comment="Mã bão")
    intensity = Column(String(50), nullable=True, comment="Cường độ bão")
    start_time = Column(DateTime, nullable=True, comment="Thời gian bắt đầu")
    latest_time = Column(DateTime, nullable=True, comment="Thời gian cập nhật gần nhất")
    updatedAt = Column(DateTime, nullable=True, comment="Ngày cập nhật bảng ghi")

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
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True, comment="ID tham chiếu tới bảng DimLocation")
    hp = Column(Numeric(10, 2), nullable=True, comment="Độ cao hoặc áp suất (tuỳ dữ liệu)")
    event_datetime = Column(DateTime, nullable=False, comment="Thời gian xảy ra sự kiện")
    rainfall_mm = Column(Numeric(10, 2), nullable=True, comment="Lượng mưa (mm)")
    createdAt = Column(DateTime, nullable=True, comment="Thời gian tạo bản ghi")

    location = relationship("DimLocation", back_populates="heavy_rains")

    def __repr__(self):
        return f"<FactHeavyRain(id={self.id}, location_id={self.location_id}, event={self.event_datetime})>"


class FactThunderstorm(BaseTransform):
    __tablename__ = "fact_thunderstorm"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True, comment="ID tham chiếu tới bảng DimLocation")
    hp = Column(Numeric(10, 2), nullable=True, comment="Độ cao hoặc áp suất (tuỳ dữ liệu)")
    event_datetime = Column(DateTime, nullable=False, comment="Thời gian xảy ra sự kiện")
    thunderstorms_index = Column(Numeric(10, 2), nullable=True, comment="Chỉ số dông bão")
    createdAt = Column(DateTime, nullable=True, comment="Thời gian tạo bản ghi")

    location = relationship("DimLocation", back_populates="thunderstorms")

    def __repr__(self):
        return f"<FactThunderstorm(id={self.id}, location_id={self.location_id})>"


class FactFog(BaseTransform):
    __tablename__ = "fact_fog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True, comment="ID tham chiếu tới bảng DimLocation")
    hp = Column(Numeric(10, 2), nullable=True, comment="Độ cao hoặc áp suất (tuỳ dữ liệu)")
    event_datetime = Column(DateTime, nullable=False, comment="Thời gian xảy ra sự kiện")
    fog_index = Column(String(50), nullable=True, comment="Chỉ số sương mù")
    visibility = Column(Integer, nullable=True, comment="Tầm nhìn xa")
    createdAt = Column(DateTime, nullable=True, comment="Thời gian tạo bản ghi")

    location = relationship("DimLocation", back_populates="fogs")

    def __repr__(self):
        return f"<FactFog(id={self.id}, location_id={self.location_id})>"


class FactGale(BaseTransform):
    __tablename__ = "fact_gale"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"), nullable=False, index=True, comment="ID tham chiếu tới bảng DimLocation")
    hp = Column(Numeric(10, 2), nullable=True, comment="Độ cao hoặc áp suất (tuỳ dữ liệu)")
    event_datetime = Column(DateTime, nullable=False, comment="Thời gian xảy ra sự kiện")
    knots = Column(Numeric(10, 2), nullable=True, comment="Tốc độ gió (knots)")
    ms = Column(Numeric(10, 2), nullable=True, comment="Tốc độ gió (m/s)")
    degrees = Column(Integer, nullable=True, comment="Hướng gió (độ)")
    direction = Column(String(50), nullable=True, comment="Hướng gió (chữ)")
    createdAt = Column(DateTime, nullable=True, comment="Thời gian tạo bản ghi")

    location = relationship("DimLocation", back_populates="gales")

    def __repr__(self):
        return f"<FactGale(id={self.id}, location_id={self.location_id})>"


class FactCycloneTrack(BaseTransform):
    __tablename__ = "fact_cyclone_track"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cyclone_id = Column(Integer, ForeignKey("dim_cyclone.id"), nullable=False, index=True, comment="ID tham chiếu tới bảng DimCyclone")
    center_id = Column(Integer, nullable=True, comment="ID trung tâm dự báo")
    analysis_time = Column(DateTime, nullable=False, comment="Thời gian phân tích/dự báo")
    lat = Column(Numeric(10, 6), nullable=True, comment="Vĩ độ")
    lon = Column(Numeric(10, 6), nullable=True, comment="Kinh độ")
    speed_of_movement = Column(Numeric(10, 2), nullable=True, comment="Tốc độ di chuyển của bão")    
    movement_direction = Column(String(50), nullable=True, comment="Hướng di chuyển của bão")
    pressure = Column(Numeric(10, 2), nullable=True, comment="Áp suất tại tâm bão")
    max_wind_speed = Column(Numeric(10, 2), nullable=True, comment="Tốc độ gió tối đa")
    gust = Column(Numeric(10, 2), nullable=True, comment="Gió giật")
    wind_threshold_kt = Column(Numeric(10, 2), nullable=True, comment="Ngưỡng gió (knots)")
    NEQ_nm = Column(Numeric(10, 2), nullable=True, comment="Bán kính gió hướng Đông Bắc (nm)")
    SEQ_nm = Column(Numeric(10, 2), nullable=True, comment="Bán kính gió hướng Đông Nam (nm)")
    SWQ_nm = Column(Numeric(10, 2), nullable=True, comment="Bán kính gió hướng Tây Nam (nm)")
    NWQ_nm = Column(Numeric(10, 2), nullable=True, comment="Bán kính gió hướng Tây Bắc (nm)")
    createdAt = Column(DateTime, nullable=True, comment="Thời gian tạo bản ghi")

    cyclone = relationship("DimCyclone", back_populates="tracks")

    def __repr__(self):
        return f"<FactCycloneTrack(id={self.id}, cyclone_id={self.cyclone_id})>"
