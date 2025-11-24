import json
import os
from pathlib import Path
from typing import Any, Dict


# Xác định thư mục gốc của project trong container: /opt/airflow
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


def get_data_dir() -> Path:
    """
    Lấy thư mục data gốc, ưu tiên biến môi trường DATA_DIR.
    Mặc định: <PROJECT_ROOT>/data
    """
    return Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))


def get_config_path() -> Path:
    """
    Đường dẫn tuyệt đối tới file config.json dùng chung.
    Mặc định: <DATA_DIR>/config/config.json
    """
    return get_data_dir() / "config" / "config.json"


def extract_values_from_json(file_path: str | Path, key: str) -> Dict[str, Any]:
    """
    Đọc file JSON và trả về dict của key chính.

    Args:
        file_path: Đường dẫn đến file JSON
        key: Key chính cần lấy (ở level đầu tiên)

    Returns:
        Dict chứa các key-value con của key chính
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} không tồn tại")
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    if not isinstance(data, dict):
        raise ValueError(f"File JSON phải ở dạng dict ở level đầu tiên")
    
    if key not in data:
        raise KeyError(f"Key '{key}' không tồn tại trong {file_path}")
    
    value = data[key]
    if not isinstance(value, dict):
        raise ValueError(f"Key '{key}' phải trả về dict, nhưng nhận được {type(value)}")
    
    return value
