import pandas as pd

# ============================
#  Common helper functions
# ============================

def parse_lat_lon(value):
    """parse lat/lon dạng integer x100 → float, hoặc None"""
    if pd.isna(value):
        return None
    try:
        v = float(value) / 100
        return v if -10000 < value < 10000 else None
    except:
        return None


def parse_hp(value):
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except:
        return None


def parse_country(value):
    if pd.isna(value):
        return "Unknown"
    s = str(value).strip()
    return "Unknown" if s.lower() in ["", "nan", "none", "null", "n/a"] else s


def parse_datetime(value):
    return pd.to_datetime(str(value), format="%Y%m%d%H", errors="coerce")


# ============================
#  Clean FOG
# ============================

def clean_fog_row(row):
    raw_vis = str(row.get("visibility", "")).strip()

    if raw_vis.lower() in ["", "nan", "none", "null", "n/a", "-", "--", "no data"]:
        visibility_value = 100
    else:
        extracted = pd.to_numeric(
            pd.Series(raw_vis).str.extract(r"(\d+)")[0],
            errors="coerce"
        )
        visibility_value = int(extracted.iloc[0]) if pd.notna(extracted.iloc[0]) else 100

    return {
        "station": row.get("station"),
        "lat": int(row.get("lat", 0)),
        "lon": int(row.get("lon", 0)),
        "hp": parse_hp(row.get("hp")),
        "country": parse_country(row.get("country")),
        "fog": row.get("fog"),
        "visibility": visibility_value,
        "datetime": parse_datetime(row.get("datetime")),
    }


# ============================
#  Clean GALE
# ============================

def clean_gale_row(row):
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))

    if lat is None or lon is None:
        return None

    return {
        "station": row.get("station"),
        "lat": lat,
        "lon": lon,
        "hp": parse_hp(row.get("hp")),
        "country": parse_country(row.get("country")),
        "knots": int(row.get("knots", 0)),
        "ms": int(row.get("ms", 0)),
        "degrees": int(row.get("degrees", 0)),
        "direction": row.get("direction"),
        "datetime": parse_datetime(row.get("datetime")),
    }


# ============================
#  Clean HEAVY RAIN
# ============================

def clean_heavyrain_row(row):
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))

    if lat is None or lon is None:
        return None

    rain_val = row.get("hvyrain")
    rain = float(rain_val) if pd.notna(rain_val) else None

    return {
        "station": row.get("station"),
        "lat": lat,
        "lon": lon,
        "hp": parse_hp(row.get("hp")),
        "country": parse_country(row.get("country")),
        "hvyrain": rain,
        "datetime": None,
    }


# ============================
#  Clean THUNDERSTORMS
# ============================

def clean_thunderstorms_row(row):
    lat = parse_lat_lon(row.get("lat"))
    lon = parse_lat_lon(row.get("lon"))

    if lat is None or lon is None:
        return None

    try:
        thunder = int(row.get("thunderstorms"))
    except:
        thunder = None

    return {
        "station": row.get("station"),
        "lat": lat,
        "lon": lon,
        "hp": parse_hp(row.get("hp")),
        "country": parse_country(row.get("country")),
        "thunderstorms": thunder,
        "datetime": parse_datetime(row.get("datetime")),
    }


# ============================
#  Mapping
# ============================

CLEAN_FUNCTIONS = {
    "fog": clean_fog_row,
    "gale": clean_gale_row,
    "heavyrain": clean_heavyrain_row,
    "thunderstorms": clean_thunderstorms_row,
}
