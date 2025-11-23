import re
from typing import Any, Dict, Optional, Tuple
import pandas as pd
from clean.models import *  

# ============================
#  Common helper functions
# ============================

_INVALID_STRINGS = {"", "nan", "none", "null", "n/a", "-", "--", "no data","missing","not available"," "}

def _is_invalid_string(value: Any) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip().lower() in _INVALID_STRINGS

def to_float_nullable(value: Any) -> Optional[float]:
    if _is_invalid_string(value):
        return None
    try:
        return float(value)
    except Exception:
        return None

def to_int_nullable(value: Any) -> Optional[int]:
    if _is_invalid_string(value):
        return None
    try:
        return int(float(value))
    except Exception:
        return None

def parse_lat_lon(value: Any) -> Optional[float]:
    if _is_invalid_string(value):
        return None
    try:
        num = float(value)
        v = num / 100.0 if abs(num) > 180 and abs(num / 100.0) <= 180 else num
        return v if -180.0 <= v <= 180.0 else None
    except Exception:
        return None

def parse_hp(value: Any) -> Optional[float]:
    return to_float_nullable(value)

def parse_country(value: Any) -> str:
    if _is_invalid_string(value):
        return "Unknown"
    return str(value).strip()

def parse_datetime(value: Any, fmt: Optional[str] = "%Y%m%d%H") -> Optional[pd.Timestamp]:
    if _is_invalid_string(value):
        return None
    try:
        if fmt:
            return pd.to_datetime(str(value), format=fmt, errors="coerce")
        return pd.to_datetime(value, errors="coerce")
    except Exception:
        return None

def extract_first_int(s: Any) -> Optional[int]:
    if _is_invalid_string(s):
        return None
    m = re.search(r"(\d+)", str(s))
    return int(m.group(1)) if m else None

def parse_visibility(raw_vis: Any, default: int = 100) -> int:
    if _is_invalid_string(raw_vis):
        return default
    s = str(raw_vis)
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else default

def parse_wind_radii(value: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    if _is_invalid_string(value):
        return (None, None, None, None, None)
    text = str(value)
    parts = text.split(";;")
    try:
        threshold_part = parts[0].replace("kt", "").strip()
        threshold = float(threshold_part) if threshold_part != "" else None
    except Exception:
        threshold = None
    quadrants = {"NEQ": None, "SEQ": None, "SWQ": None, "NWQ": None}
    for part in parts[1:]:
        if "|" in part:
            p1, p2 = [p.strip() for p in part.split("|", 1)]
            if p2.upper() in quadrants:
                dist, quad = p1, p2.upper()
            elif p1.upper() in quadrants:
                quad, dist = p1.upper(), p2
            else:
                m = re.search(r"(\d+)", part)
                dist = m.group(1) + "nm" if m else ""
                quad = next((q for q in quadrants if q in part.upper()), None)
            try:
                quadrants[quad] = float(dist.replace("nm", "").strip()) if quad else None
            except Exception:
                pass
    return (threshold, quadrants["NEQ"], quadrants["SEQ"], quadrants["SWQ"], quadrants["NWQ"])

def validate_coords(lat: float, lng: float) -> bool:
    return -90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0

# ============================
#  Cleaners
# ============================

def clean_fog_row(row: Dict[str, Any]) -> Fog:
    visibility_value = parse_visibility(row.get("visibility", ""))
    
    # Sử dụng parse_lat_lon() thay vì to_int_nullable() cho lat và lon
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))
    
    # Kiểm tra nếu lat, lon không hợp lệ (None) thì sẽ trả về giá trị mặc định
    if lat is None:
        lat = 0.0
    if lon is None:
        lon = 0.0
    
    return Fog(
        station = row.get("station"),
        lat = lat,  # Đảm bảo lat là kiểu float
        lon= lon,  # Đảm bảo lon là kiểu float
        hp= parse_hp(row.get("hp")),
        country= parse_country(row.get("country")),
        fog= row.get("fog"),
        visibility= visibility_value,
        datetime= parse_datetime(row.get("datetime")),
    )
    


def clean_gale_row(row: Dict[str, Any]) -> Optional[Gale]:
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))
    if lat is None or lon is None:
        return None
    return Gale(
        station=row.get("station"),
        lat=lat,
        lon=lon,
        hp=parse_hp(row.get("hp")),
        country=parse_country(row.get("country")),
        knots=to_int_nullable(row.get("knots")) or 0,
        ms=to_int_nullable(row.get("ms")) or 0,
        degrees=to_int_nullable(row.get("degrees")) or 0,
        direction=row.get("direction"),
        datetime=parse_datetime(row.get("datetime")),
    )

def clean_heavyrain_row(row: Dict[str, Any]) -> Optional[HeavyRain]:
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))
    if lat is None or lon is None:
        return None
    rain_val = row.get("hvyrain")
    rain = to_float_nullable(rain_val)
    return HeavyRain(
        station=row.get("station"),
        lat=lat,
        lon=lon,
        hp=parse_hp(row.get("hp")),
        country=parse_country(row.get("country")),
        hvyrain=rain,
        datetime=parse_datetime(row.get("datetime")),
    )

def clean_thunderstorms_row(row: Dict[str, Any]) -> Optional[Thunderstorms]:
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))
    if lat is None or lon is None:
        return None
    thunder = to_int_nullable(row.get("thunderstorms"))
    return Thunderstorms(
        station=row.get("station"),
        lat=lat,
        lon=lon,
        hp=parse_hp(row.get("hp")),
        country=parse_country(row.get("country")),
        thunderstorms=thunder,
        datetime=parse_datetime(row.get("datetime")),
    )

def _parse_intensity_category_from_text(text: Any) -> Optional[int]:
    return extract_first_int(text)

def clean_tc_forecast_row(row: Dict[str, Any]) -> Optional[TCForecast]:
    try:
        lat = float(row.get("lat"))
        lng = float(row.get("lng"))
    except Exception:
        return None
    if not validate_coords(lat, lng):
        return None
    intensity_cat = _parse_intensity_category_from_text(row.get("intensity"))
    wind_threshold_kt, NEQ, SEQ, SWQ, NWQ = parse_wind_radii(row.get("wind_radii"))
    fc_time = parse_datetime(row.get("forecast_time"), fmt=None)
    return TCForecast(
        time_interval=to_int_nullable(row.get("time_interval")) or 0,
        lat=lat,
        lng=lng,
        pressure=parse_hp(row.get("pressure")),
        max_wind_speed=parse_hp(row.get("max_wind_speed")),
        gust=parse_hp(row.get("gust")),
        intensity_category=intensity_cat,
        wind_threshold_kt=wind_threshold_kt,
        NEQ_nm=NEQ,
        SEQ_nm=SEQ,
        SWQ_nm=SWQ,
        NWQ_nm=NWQ,
        forecast_time=fc_time,
    )

def clean_tc_track_row(row: Dict[str, Any]) -> Optional[TCTrack]:
    analysis_time = parse_datetime(row.get("analysis_time"), fmt=None)

    try:
        lat = float(row.get("lat"))
        lng = float(row.get("lng"))
    except Exception:
        return None

    if not validate_coords(lat, lng):
        return None

    intensity_cat = _parse_intensity_category_from_text(row.get("intensity"))
    wind_threshold_kt, NEQ, SEQ, SWQ, NWQ = parse_wind_radii(row.get("wind_radii"))

    # 🔥 FIX HERE — xử lý NaN cho tc_name
    tc_name_raw = row.get("tc_name")
    tc_name = None if _is_invalid_string(tc_name_raw) else str(tc_name_raw).strip()

    return TCTrack(
        analysis_time=analysis_time,
        tc_name=tc_name,  # <— đã xử lý NaN
        tc_id=row.get("tc_id"),
        lat=lat,
        lng=lng,
        speed_of_movement=to_float_nullable(row.get("speed_of_movement")) or 0.0,
        movement_direction=row.get("movement_direction"),
        pressure=parse_hp(row.get("pressure")),
        max_wind_speed=parse_hp(row.get("max_wind_speed")),
        gust=parse_hp(row.get("gust")),
        intensity_category=intensity_cat,
        wind_threshold_kt=wind_threshold_kt,
        NEQ_nm=NEQ,
        SEQ_nm=SEQ,
        SWQ_nm=SWQ,
        NWQ_nm=NWQ,
        center_id=to_int_nullable(row.get("center_id")) or 0,
    )


def clean_tc_row(row: Dict[str, Any]) -> TC:
    start_time = parse_datetime(row.get("start"), fmt=None)
    latest_time = parse_datetime(row.get("latest"), fmt=None)
    intensity_cat = _parse_intensity_category_from_text(row.get("intensity"))
    return TC(
        sysid=to_int_nullable(row.get("sysid")) or 0,
        name=str(row.get("name")).strip() if not _is_invalid_string(row.get("name")) else None,
        storm_id=str(row.get("id")).strip() if not _is_invalid_string(row.get("id")) else None,
        intensity=row.get("intensity"),
        intensity_category=intensity_cat,
        start=start_time,
        latest=latest_time,
        same=row.get("same") if not _is_invalid_string(row.get("same")) else None,
        centerid=to_int_nullable(row.get("centerid")) or 0,
        gts=row.get("gts") if not _is_invalid_string(row.get("gts")) else None,
    )

# ============================
#  Mapping
# ============================

CLEAN_FUNCTIONS = {
    "fog": clean_fog_row,
    "gale": clean_gale_row,
    "heavyrain_snow": clean_heavyrain_row,
    "thunderstorms": clean_thunderstorms_row,
    "tc_forecast": clean_tc_forecast_row,
    "tc_track": clean_tc_track_row,
    "tc": clean_tc_row,
}