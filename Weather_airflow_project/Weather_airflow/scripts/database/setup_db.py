from pathlib import Path
import os

from database.base import create_engine_and_session
from utils.file_utils import extract_values_from_json


# DATA_DIR được mount trong docker-compose.airflow.yaml: ./data -> /opt/airflow/data
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
CONFIG_PATH = DATA_DIR / "config" / "config.json"


def setup_database(config_name, logger, echo: bool = False):
    """
    1.0.2.x (EXTRACT) - Hàm dùng chung để khởi tạo engine + SessionLocal từ config.json.

    Được gọi bởi connection_elt() trong etl_metadata.setup_db:
        - Đọc block "db_url" trong config.json, lấy URL tương ứng với `config_name`
        - Tạo engine + SessionLocal, kiểm tra kết nối (SELECT 1) qua create_engine_and_session
        - Nếu không kết nối được thì gọi hàm logger (gửi email cảnh báo, ghi log, ...)

    Args:
        config_name: tên cấu hình trong block "db_url" của config.json
        logger: logger để log thông tin kết nối DB
        echo: bật SQLAlchemy echo nếu cần debug
    """
    # 2.2. Load config từ file config.json 
    # 3.2. Load config từ file config.json 
    db_url = extract_values_from_json(CONFIG_PATH, "db_url")
    url = db_url[config_name]
    engine, SessionLocal = create_engine_and_session(url, logger, echo=echo)
    return engine, SessionLocal