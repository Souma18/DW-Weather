from datetime import datetime
from transform.setup_db import *
engine_transform, SessionTransform = connection_transform()
from database.base import session_scope
from database.logger import log_dual_status
from transform.check_log import success_logs
from etl_metadata.models import TransformLog
from transform.models import *
from clean.models import *

class TransformLogger:
    def __init__(self):
        self.logs = {
            "dim_location": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "dim_cyclone": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
            "fact_heavy_rain_snow": {"record_count": 0, "source_name": [], "start_at": datetime.now(), "end_at": None},
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

    def save_logs(self):
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
                SessionELT, 
                f"ETL Transform: Thành công - {table_name}", 
                f"Thông báo: {msg}\nSố bản ghi: {log_data['record_count']}\nNguồn dữ liệu: {log_data['source_name']}"
            )

def get_or_create_location(session, clean_obj, logger, source_name=None):
    """
    Kiểm tra và tạo location duy nhất dựa trên (lat, lon).
    Ràng buộc: (lat, lon) không được trùng lặp.
    """
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
            country=clean_obj.country,
            createdAt=datetime.now(),
        )
        session.add(location)
        session.flush()
        logger.update("dim_location", source_name, location.createdAt)
    return location

def get_or_create_cyclone(session, clean_obj, logger,isGetter=True, source_name=None):
    """
    Kiểm tra và tạo cyclone duy nhất dựa trên storm_id.
    Ràng buộc: storm_id không được trùng lặp.
    """
    cyclone = (
        session.query(DimCyclone)
        .filter(
            DimCyclone.storm_id == clean_obj.storm_id
            if isGetter
            else DimCyclone.sys_id == clean_obj.sysid
        )
        .first()
    )
        
    if cyclone is None:
        cyclone = DimCyclone(
            sys_id = clean_obj.sysid,
            updatedAt=datetime.now(),
        )
        session.add(cyclone)
        session.flush()
        logger.update("dim_cyclone", source_name, cyclone.updatedAt)
    return cyclone

def transform_heavy_rain(clean_obj, session, logger, source_name=None):
    """
    Transform heavy rain data với kiểm tra trùng lặp.
    Ràng buộc: (location_id, event_datetime) không được trùng lặp.
    """
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    
    # Kiểm tra trùng lặp: (location_id, event_datetime)
    existing = session.query(FactHeavyRainSnow).filter(
        FactHeavyRainSnow.location_id == location.id
        ).first()
    
    if existing is not None:
        return  # Đã tồn tại, bỏ qua
    
    heavy_rain = FactHeavyRainSnow(
        location_id=location.id,
        rainfall_mm=clean_obj.hvyrain,
        hp=clean_obj.hp,
        createdAt=datetime.now(),
    )
    session.add(heavy_rain)
    logger.update("fact_heavy_rain_snow", source_name, heavy_rain.createdAt)

def transform_thunderstorm(clean_obj, session, logger, source_name=None):
    """
    Transform thunderstorm data với kiểm tra trùng lặp.
    Ràng buộc: (location_id, event_datetime) không được trùng lặp.
    """
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    
    # Kiểm tra trùng lặp: (location_id, event_datetime)
    existing = session.query(FactThunderstorm).filter(
        FactThunderstorm.location_id == location.id,
        FactThunderstorm.event_datetime == clean_obj.datetime
    ).first()
    
    if existing is not None:
        return  # Đã tồn tại, bỏ qua
    
    thunderstorm = FactThunderstorm(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        thunderstorms_index=clean_obj.thunderstorms,
        hp=clean_obj.hp,
        createdAt=datetime.now(),
    )
    session.add(thunderstorm)
    logger.update("fact_thunderstorm", source_name, thunderstorm.createdAt)

def transform_fog(clean_obj, session, logger, source_name=None):
    """
    Transform fog data với kiểm tra trùng lặp.
    Ràng buộc: (location_id, event_datetime) không được trùng lặp.
    """
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    
    # Kiểm tra trùng lặp: (location_id, event_datetime)
    existing = session.query(FactFog).filter(
        FactFog.location_id == location.id,
        FactFog.event_datetime == clean_obj.datetime
    ).first()
    
    if existing is not None:
        return  # Đã tồn tại, bỏ qua
    
    fog = FactFog(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        fog_index=clean_obj.fog,
        visibility=str(clean_obj.visibility) if clean_obj.visibility is not None else None,
        hp=clean_obj.hp,
        createdAt=datetime.now(),
    )
    session.add(fog)
    logger.update("fact_fog", source_name, fog.createdAt)

def transform_gale(clean_obj, session, logger, source_name=None):
    """
    Transform gale data với kiểm tra trùng lặp.
    Ràng buộc: (location_id, event_datetime) không được trùng lặp.
    """
    location = get_or_create_location(session, clean_obj, logger, source_name=source_name)
    
    # Kiểm tra trùng lặp: (location_id, event_datetime)
    existing = session.query(FactGale).filter(
        FactGale.location_id == location.id,
        FactGale.event_datetime == clean_obj.datetime
    ).first()
    
    if existing is not None:
        return  # Đã tồn tại, bỏ qua
    
    gale = FactGale(
        location_id=location.id,
        event_datetime=clean_obj.datetime,
        knots=clean_obj.knots,
        ms=clean_obj.ms,
        degrees=clean_obj.degrees,
        direction=clean_obj.direction,
        hp=clean_obj.hp,
        createdAt=datetime.now(),
    )
    session.add(gale)
    logger.update("fact_gale", source_name, gale.createdAt)

def transform_tc(clean_obj, session, logger, source_name=None):
    """
    Transform tropical cyclone (TC) data.
    Cập nhật thông tin cyclone nếu đã tồn tại.
    """
    cyclone = get_or_create_cyclone(session, clean_obj, logger,False, source_name=source_name)
    if(cyclone.name is None):
        cyclone.name = clean_obj.name
        cyclone.sys_id = clean_obj.sysid
        cyclone.storm_id = clean_obj.storm_id
        cyclone.intensity = clean_obj.intensity
        cyclone.start_time = clean_obj.start
        cyclone.latest_time = clean_obj.latest
        logger.update("dim_cyclone", source_name, cyclone.updatedAt)


def transform_cyclone_track(clean_obj, session, logger, source_name=None):
    """
    Transform cyclone track data với kiểm tra trùng lặp.
    Ràng buộc: (cyclone_id, analysis_time) không được trùng lặp.
    """
    class TempCycloneObj:
        def __init__(self, storm_id, ):
            self.storm_id = storm_id
            
    temp_obj = TempCycloneObj(clean_obj.tc_id)
    cyclone = get_or_create_cyclone(session, temp_obj,logger, source_name=source_name)
    
    # Kiểm tra trùng lặp: (cyclone_id, analysis_time)
    existing = session.query(FactCycloneTrack).filter(
        FactCycloneTrack.cyclone_id == cyclone.id,
        FactCycloneTrack.analysis_time == clean_obj.analysis_time
    ).first()
    
    if existing is not None:
        return  # Đã tồn tại, bỏ qua
    
    track = FactCycloneTrack(
        cyclone_id=cyclone.id,
        analysis_time=clean_obj.analysis_time,
        lat=clean_obj.lat,
        lon=clean_obj.lng,
        pressure=clean_obj.pressure,
        max_wind_speed=clean_obj.max_wind_speed,
        gust=clean_obj.gust,
        speed_of_movement=clean_obj.speed_of_movement,
        movement_direction=clean_obj.movement_direction,
        wind_threshold_kt=clean_obj.wind_threshold_kt,
        NEQ_nm=clean_obj.NEQ_nm,
        SEQ_nm=clean_obj.SEQ_nm,
        SWQ_nm=clean_obj.SWQ_nm,
        NWQ_nm=clean_obj.NWQ_nm,
        center_id=clean_obj.center_id,
        createdAt=datetime.now(),
    )
    session.add(track)
    logger.update("fact_cyclone_track", source_name, track.createdAt)

    
TRANSFORM_HANDLERS = {
    "heavyrain_snow": (HeavyRain, transform_heavy_rain),
    "thunderstorms": (Thunderstorms, transform_thunderstorm),
    "fog": (Fog, transform_fog),
    "gale": (Gale, transform_gale),
    "tc_track": (TCTrack, transform_cyclone_track),
    "tc": (TC, transform_tc),
}

def run_transform():
    logger = TransformLogger()
    
    with session_scope(SessionClean) as session_clean:
        # Phân loại logs: ưu tiên 'tc' trước
        tc_logs = [log for log in success_logs if log.get("table_type") == "tc"]
        other_logs = [log for log in success_logs if log.get("table_type") != "tc"]
        
        # Gộp lại danh sách để xử lý: tc trước, các loại khác sau
        sorted_logs = tc_logs + other_logs

        for log in sorted_logs:
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
    logger.save_logs()