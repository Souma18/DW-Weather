import json
from pathlib import Path
from typing import Any, Dict

def extract_values_from_json(file_path: str, key: str) -> Dict[str, Any]:
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
