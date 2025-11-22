from datetime import datetime
from database.setup_db import SessionClean, SessionTransform, SessionELT, DEFAULT_RECIEVER_EMAIL
from database import session_scope
from database.logger import log_dual_status
from check_log import success_logs
from elt_metadata.models import TransformLog
from models import (
    DimLocation, DimCyclone,
    FactHeavyRain, FactThunderstorm, FactFog, FactGale, FactCycloneTrack
)
from clean.models import FogRecord

class TransformLogger:
    def __init__(self):
        self.logs = {
            "dim_location": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "dim_cyclone": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_heavy_rain": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_gale": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_fog": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_thunderstorm": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_cyclone_track": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
        }

    def update(self, table_name, source_name, dt):
        if table_name in self.logs:
            log = self.logs[table_name]
            log["record_count"] += 1
            if source_name and source_name not in log["source_name"]:
                log["source_name"].append(source_name)
            log["end_at"] = dt

    def save_logs(self, session):
        for table_name, log_data in self.logs.items():
            if log_data["record_count"] == 0:
                continue
            
            msg = f"Đã chèn {log_data['record_count']} bản ghi mới cho bảng {table_name} thành công."
            transform_log = TransformLog(
                status="Success",
                record_count=log_data["record_count"],
                source_name=", ".join(log_data["source_name"]),
                table_name=table_name,
                message=msg,
                start_at=log_data["start_at"],
                end_at=log_data["end_at"] if log_data["end_at"] else datetime.now(),
            )
            
            log_dual_status(
                transform_log, 
                session, 
                DEFAULT_RECIEVER_EMAIL, 
                f"ETL Transform: Thành công - {table_name}", 
                f"Thông báo: {msg}\nSố bản ghi: {log_data['record_count']}\nNguồn dữ liệu: {log_data['source_name']}"
            )

def get_or_create_location(session, clean_obj, logger, source_name=None):
    location = (
        session.query(DimLocation)
        .filter(DimLocation.lat == clean_obj.lat, DimLocation.lon == clean_obj.lon)
        .first()
    )
    if location is None:
        location = DimLocation(
            station=clean_obj.station,
            lat=clean_obj.lat,
            lon=clean_obj.lon,
            hp=clean_obj.hp,
            country=clean_obj.country,
            createdAt=datetime.now(),
        )
        session.add(location)
        session.flush()
        logger.update("dim_location", source_name, location.createdAt)
    return location

def get_or_create_cyclone(session, clean_obj, logger, source_name=None):
    cyclone = (
        session.query(DimCyclone).filter(DimCyclone.name == clean_obj.name).first()
    )
    if cyclone is None:
        cyclone = DimCyclone(
            name=clean_obj.name,
            intensity=clean_obj.intensity,
            start_time=clean_obj.start_time,
            latest_time=clean_obj.latest_time,
            updatedAt=datetime.now(),
        )
        session.add(cyclone)
        session.flush()
        logger.update("dim_cyclone", source_name, cyclone.updatedAt)
    return cyclone

def transform_heavy_rain(clean_obj, session, logger, source_name=None):
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    heavy_rain = FactHeavyRain(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        rainfall_mm=clean_obj.rain_mm,
        createdAt=datetime.now(),
    )
    session.add(heavy_rain)
    logger.update("fact_heavy_rain", source_name, heavy_rain.createdAt)

def transform_thunderstorm(clean_obj, session, logger, source_name=None):
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    thunderstorm = FactThunderstorm(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        thunderstorms_index=clean_obj.thunderstorm_index,
        createdAt=datetime.now(),
    )
    session.add(thunderstorm)
    logger.update("fact_thunderstorm", source_name, thunderstorm.createdAt)

def transform_fog(clean_obj, session, logger, source_name=None):
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    fog = FactFog(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        fog_index=clean_obj.fog,
        visibility=clean_obj.visibility,
        createdAt=datetime.now(),
    )
    session.add(fog)
    logger.update("fact_fog", source_name, fog.createdAt)

def transform_gale(clean_obj, session, logger, source_name=None):
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    gale = FactGale(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        knots=clean_obj.knots,
        ms=clean_obj.ms,
        degrees=clean_obj.degrees,
        direction=clean_obj.direction,
        createdAt=datetime.now(),
    )
    session.add(gale)
    logger.update("fact_gale", source_name, gale.createdAt)

def transform_cyclone_track(clean_obj, session, logger, source_name=None):
    cyclone = get_or_create_cyclone(session, clean_obj, logger, source_name=source_name)
    track = FactCycloneTrack(
        cyclone_id=cyclone.id,
        event_datetime=clean_obj.datetime,
        lat=clean_obj.lat,
        lon=clean_obj.lon,
        intensity=clean_obj.intensity,
        pressure=clean_obj.pressure,
        max_wind_speed=clean_obj.max_wind_speed,
        gust=clean_obj.gust,
        speed_of_movement=clean_obj.speed_of_movement,
        movement_direction=clean_obj.movement_direction,
        wind_radis=clean_obj.wind_radis,
        center_id=clean_obj.center_id,
        createdAt=datetime.now(),
    )
    session.add(track)
    logger.update("fact_cyclone_track", source_name, track.createdAt)

TRANSFORM_HANDLERS = {
    # "FactHeavyRain": (CleanHeavyRain, transform_heavy_rain),
    # "FactThunderstorm": (CleanThunderstorm, transform_thunderstorm),
    "fog_records": (FogRecord, transform_fog),
    # "FactGale": (CleanGale, transform_gale),
    # "FactCycloneTrack": (CleanCycloneTrack, transform_cyclone_track),
}

def run_transform():
    logger = TransformLogger()
    
    with session_scope(SessionClean) as session_clean:
        logs = success_logs
        for log in logs:
            table_type = log["table_type"]
            start_index = log["start_index"]
            end_index = log["end_index"]
            
            handler_tuple = TRANSFORM_HANDLERS.get(table_type)
            if not handler_tuple:
                continue
                
            clean_model_cls, handler_fn = handler_tuple
            
            clean_objs = (
                session_clean.query(clean_model_cls)
                .filter(clean_model_cls.id >= start_index, clean_model_cls.id <= end_index)
                .all()
            )
            
            with session_scope(SessionTransform) as session_trans:
                for clean_obj in clean_objs:
                    handler_fn(clean_obj, session_trans, logger, source_name=log["file_name"])

    with session_scope(SessionELT) as session_log:
        logger.save_logs(session_log)

if __name__ == "__main__":
    run_transform()