from database import BaseClean
from sqlalchemy import Column, Integer, Float, String, DateTime

class Fog(BaseClean):
    __tablename__ = "fog"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    hp = Column(Float)
    country = Column(String(255))
    fog = Column(String(255))
    visibility = Column(Integer)
    datetime = Column(DateTime)

class Gale(BaseClean):
    __tablename__ = "gale"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    hp = Column(Float)
    country = Column(String(255))
    knots = Column(Integer)
    ms = Column(Integer)
    degrees = Column(Integer)
    direction = Column(String(50))
    datetime = Column(DateTime)

class HeavyRain(BaseClean):
    __tablename__ = "heavyrain"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    hp = Column(Float)
    country = Column(String(255))
    hvyrain = Column(Float)
    datetime = Column(DateTime)

class Thunderstorms(BaseClean):
    __tablename__ = "thunderstorms"
    id = Column(Integer, primary_key=True, autoincrement=True)
    station = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    hp = Column(Float)
    country = Column(String(255))
    thunderstorms = Column(Integer)
    datetime = Column(DateTime)

class TCForecast(BaseClean):
    __tablename__ = "tc_forecast"
    id = Column(Integer, primary_key=True, autoincrement=True)
    time_interval = Column(Integer)
    lat = Column(Float)
    lng = Column(Float)
    pressure = Column(Float)
    max_wind_speed = Column(Float)
    gust = Column(Float)
    intensity_category = Column(Integer)
    wind_threshold_kt = Column(Float)
    NEQ_nm = Column(Float)
    SEQ_nm = Column(Float)
    SWQ_nm = Column(Float)
    NWQ_nm = Column(Float)
    forecast_time = Column(DateTime)
class TCTrack(BaseClean):
    __tablename__ = "tc_track"
    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_time = Column(DateTime)
    tc_name = Column(String(255))
    tc_id = Column(String(50))
    lat = Column(Float)
    lng = Column(Float)
    speed_of_movement = Column(Float)
    movement_direction = Column(String(50))
    pressure = Column(Float)
    max_wind_speed = Column(Float)
    gust = Column(Float)
    intensity_category = Column(Integer)
    wind_threshold_kt = Column(Float)
    NEQ_nm = Column(Float)
    SEQ_nm = Column(Float)
    SWQ_nm = Column(Float)
    NWQ_nm = Column(Float)
    center_id = Column(Integer)
class TC(BaseClean):
    __tablename__ = "tc"
    id = Column(Integer, primary_key=True, autoincrement=True)
    sysid = Column(Integer)
    name = Column(String(255))
    storm_id = Column(String(50))
    intensity = Column(String(50))
    intensity_category = Column(Integer)
    start = Column(DateTime)
    latest = Column(DateTime)
    same = Column(String(255))
    centerid = Column(Integer)
    gts = Column(String(255))