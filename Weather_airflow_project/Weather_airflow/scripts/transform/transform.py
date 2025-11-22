from datetime import datetime
from database import SessionClean, SessionTransform, SessionELT, session_scope
import create_tables
from check_log import success_logs
from log import TransformLog
from models import (
    DimLocation, DimCyclone,
    FactHeavyRain, FactThunderstorm, FactFog, FactGale, FactCycloneTrack
)
# from clean_models import  CleanThunderstorm, CleanFog, CleanGale, CleanCycloneTrack, CleanHeavyRain
from clean_models import   FogRecord

# ------ LOGGING -------
def add_transform_log(
    session, *, status, record_count, source_name, table_name, message, start_at, end_at):
    transform_log = TransformLog(
        status=status,
        record_count=record_count,
        source_name=source_name,
        table_name=table_name,
        message=message,
        start_at=start_at,
        end_at=end_at,
    )
    session.add(transform_log)
etl_logs = {
    "dim_location_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "dim_cyclone_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "fact_heavy_rain_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "fact_gale_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "fact_fog_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "fact_thunderstorm_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
    "fact_cyclone_track_log": {
        "record_count": 0,
        "source_name": [],
        "start_at": datetime.now(),
        "end_at": None,
    },
}
def log_update(log_dict, source_name, dt):
    log_dict["record_count"] += 1
    if source_name and source_name not in log_dict["source_name"]:
        log_dict["source_name"].append(source_name)
    log_dict["end_at"] = dt

def get_or_create_location(session, clean_obj, source_name=None):
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
        log_update(etl_logs["dim_location_log"], source_name, location.createdAt)
    return location

def get_or_create_cyclone(session, clean_obj, source_name=None):
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
        log_update(etl_logs["dim_cyclone_log"], source_name, cyclone.updatedAt)
    return cyclone

def transform_heavy_rain(clean_obj, session, source_name=None):
    location = get_or_create_location(session, clean_obj, source_name=source_name)
    heavy_rain = FactHeavyRain(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        rainfall_mm=clean_obj.rain_mm,
        createdAt=datetime.now(),
    )
    session.add(heavy_rain)
    log_update(etl_logs["fact_heavy_rain_log"], source_name, heavy_rain.createdAt)

def transform_thunderstorm(clean_obj, session, source_name=None):
    location = get_or_create_location(session, clean_obj, source_name=source_name)
    thunderstorm = FactThunderstorm(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        thunderstorms_index=clean_obj.thunderstorm_index,
        createdAt=datetime.now(),
    )
    session.add(thunderstorm)
    log_update(etl_logs["fact_thunderstorm_log"], source_name, thunderstorm.createdAt)

def transform_fog(clean_obj, session, source_name=None):
    location = get_or_create_location(session, clean_obj, source_name=source_name)
    fog = FactFog(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        fog_index=clean_obj.fog,
        visibility=clean_obj.visibility,
        createdAt=datetime.now(),
    )
    session.add(fog)
    log_update(etl_logs["fact_fog_log"], source_name, fog.createdAt)

def transform_gale(clean_obj, session, source_name=None):
    location = get_or_create_location(session, clean_obj, source_name=source_name)
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
    log_update(etl_logs["fact_gale_log"], source_name, gale.createdAt)

def transform_cyclone_track(clean_obj, session, source_name=None):
    cyclone = get_or_create_cyclone(session, clean_obj, source_name=source_name)
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
    log_update(etl_logs["fact_cyclone_track_log"], source_name, track.createdAt)


# TỪNG MAP TABLE_TYPE SANG HÀM BIẾN ĐỔI
TRANSFORM_HANDLERS = {
    # "FactHeavyRain": (CleanHeavyRain, transform_heavy_rain, "fact_heavy_rain"),
    # "FactThunderstorm": (CleanThunderstorm, transform_thunderstorm, "fact_thunderstorm"),
    "fog_records": (FogRecord, transform_fog, "fog_records"),
    # "FactGale": (CleanGale, transform_gale, "fact_gale"),
    # "FactCycloneTrack": (CleanCycloneTrack, transform_cyclone_track, "fact_cyclone_track"),
}
def run_transform():
    with session_scope(SessionClean) as session_clean:
        logs = success_logs
        for log in logs:
            table_type = log["table_type"]
            start_index = log["start_index"]
            end_index = log["end_index"]
            handler_tuple = TRANSFORM_HANDLERS.get(table_type)
            if not handler_tuple:
                continue
            clean_model_cls, handler_fn, table_name = handler_tuple
            clean_objs = (
                session_clean.query(clean_model_cls)
                .filter(clean_model_cls.id >= start_index, clean_model_cls.id <= end_index)
                .all()
            )
            with session_scope(SessionTransform) as session_trans:
                for clean_obj in clean_objs:
                    handler_fn(clean_obj, session_trans, source_name=log["file_name"])

    with session_scope(SessionELT) as session_log:
        for log_var, log_data in etl_logs.items():
            if log_data["record_count"] == 0:
                continue
            table_name = log_var.replace("_log", "")
            msg = f"Insert {log_data['record_count']} new records for {table_name} successfully."
            add_transform_log(
                session=session_log,
                status="Success",
                record_count=log_data["record_count"],
                source_name=", ".join(log_data["source_name"]),   # join để thành string
                table_name=table_name,
                message=msg,
                start_at=log_data["start_at"],
                end_at=log_data["end_at"] if log_data["end_at"] else datetime.now(),
            )
run_transform()