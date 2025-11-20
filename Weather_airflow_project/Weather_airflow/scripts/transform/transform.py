from datetime import datetime
from database import SessionClean, SessionTransform, SessionELT, session_scope
from check_log import success_logs
from log import TransformLog
import create_tables
from models import (
    DimLocation, DimCyclone,
    FactHeavyRain, FactThunderstorm, FactFog, FactGale, FactCycloneTrack
)
from clean_models import User, CleanThunderstorm, CleanFog, CleanGale, CleanCyclone, CleanCycloneTrack
from models import CleanLog

# ------ LOGGING -------
def add_transform_log(
    session, *, status, record_count, source_name, table_name, message, start_at, end_at
):
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

# --------- DIM/FACT HANDLERS ----------
def get_or_create_location(session, clean_obj):
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
            gc=clean_obj.gc,
            createdAt=datetime.now(),
        )
        session.add(location)
        session.flush()
    return location

def transform_heavy_rain(clean_obj, session):
    location = get_or_create_location(session, clean_obj)
    heavy_rain = FactHeavyRain(
        location_id=location.id,
        event_datetime=clean_obj.event_datetime,
        rainfall_mm=clean_obj.rain_mm,
        createdAt=datetime.now(),
    )
    session.add(heavy_rain)

def transform_thunderstorm(clean_obj, session):
    location = get_or_create_location(session, clean_obj)
    thunderstorm = FactThunderstorm(
        location_id=location.id,
        event_datetime=clean_obj.event_datetime,
        thunderstorms_index=clean_obj.thunderstorm_index,
        createdAt=datetime.now(),
    )
    session.add(thunderstorm)

def transform_fog(clean_obj, session):
    location = get_or_create_location(session, clean_obj)
    fog = FactFog(
        location_id=location.id,
        event_datetime=clean_obj.event_datetime,
        fog_index=clean_obj.fog_index,
        visibility=clean_obj.visibility,
        createdAt=datetime.now(),
    )
    session.add(fog)

def transform_gale(clean_obj, session):
    location = get_or_create_location(session, clean_obj)
    gale = FactGale(
        location_id=location.id,
        event_datetime=clean_obj.event_datetime,
        knots=clean_obj.knots,
        ms=clean_obj.ms,
        degrees=clean_obj.degrees,
        direction=clean_obj.direction,
        createdAt=datetime.now(),
    )
    session.add(gale)

def get_or_create_cyclone(session, clean_obj):
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
    return cyclone

def transform_cyclone_track(clean_obj, session):
    cyclone = get_or_create_cyclone(session, clean_obj)
    track = FactCycloneTrack(
        cyclone_id=cyclone.id,
        event_datetime=clean_obj.event_datetime,
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


# TỪNG MAP TABLE_TYPE SANG HÀM BIẾN ĐỔI
TRANSFORM_HANDLERS = {
    "FactHeavyRain": (User, transform_heavy_rain, "fact_heavy_rain"),
    "FactThunderstorm": (CleanThunderstorm, transform_thunderstorm, "fact_thunderstorm"),
    "FactFog": (CleanFog, transform_fog, "fact_fog"),
    "FactGale": (CleanGale, transform_gale, "fact_gale"),
    "FactCycloneTrack": (CleanCycloneTrack, transform_cyclone_track, "fact_cyclone_track"),
}

def run_transform():
    with session_scope(SessionClean) as session_clean:
        # Lấy các log thành công (hoặc sửa theo luồng của bạn)
        logs = session_clean.query(CleanLog).filter(CleanLog.status == "Success").all()
        for log in logs:
            table_type = log.table_type
            success_range = log.success_range
            if not success_range:
                continue
            try:
                start, end = map(int, success_range.split("~"))
            except Exception:
                continue
            # Handler
            handler_tuple = TRANSFORM_HANDLERS.get(table_type)
            if not handler_tuple:
                continue
            clean_model_cls, handler_fn, table_name = handler_tuple
            clean_objs = (
                session_clean.query(clean_model_cls)
                .filter(clean_model_cls.id >= start, clean_model_cls.id <= end)
                .all()
            )
            start_at = datetime.now()
            count = 0
            with session_scope(SessionTransform) as session_trans:
                for clean_obj in clean_objs:
                    handler_fn(clean_obj, session_trans)
                    count += 1
            # Log transform...
            msg = f"Insert {count} new records for {table_name} successfully."
            with session_scope(SessionELT) as session_log:
                add_transform_log(
                    session=session_log,
                    status="Success",
                    record_count=count,
                    source_name=str([log.file_name]),
                    table_name=table_name,
                    message=msg,
                    start_at=start_at,
                    end_at=datetime.now(),
                )