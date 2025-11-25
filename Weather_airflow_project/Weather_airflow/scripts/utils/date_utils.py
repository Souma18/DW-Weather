# utils_datetime.py
from datetime import datetime
from zoneinfo import ZoneInfo

HCM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def date_now() -> datetime:
    """
    Trả về datetime hiện tại theo múi giờ Việt Nam (HCM).
    """
    return datetime.now(HCM_TZ)