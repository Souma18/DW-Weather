"""
Module chính để extract dữ liệu từ JSON links sang CSV files.
"""

import json
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import requests
from urllib.parse import urlparse
import re


# Mapping các loại dữ liệu sang tên viết tắt (tùy chọn)
TYPE_ABBREVIATIONS = {
    "tropical cyclone": "tc",
    "tropical cyclone detail": "tc_detail",
    "tropical cyclone track": "tc_track",
    "tropical cyclone forecast": "tc_forecast",
    "heavyrain/snow": "heavyrain_snow",
    "heavyrain": "heavyrain",
    "snow": "snow",
    "thunderstorms": "thunderstorms",
    "gale": "gale",
    "fog": "fog",
}


def sanitize_filename(name: str, use_abbreviations: bool = False) -> str:
    """
    Xử lý và chuẩn hóa tên file từ dữ liệu đầu vào.
    
    Quy tắc đặt tên:
    - Khoảng trắng được thay bằng dấu gạch dưới (_)
    - Các ký tự đặc biệt không hợp lệ cho tên file được thay bằng dấu gạch dưới
    - Nhiều dấu gạch dưới liên tiếp được gộp thành một
    - Loại bỏ dấu gạch dưới ở đầu và cuối
    
    Args:
        name: Tên gốc từ dữ liệu (có thể có khoảng trắng, ký tự đặc biệt)
        use_abbreviations: Nếu True, sử dụng viết tắt từ TYPE_ABBREVIATIONS nếu có
    
    Returns:
        Tên file đã được chuẩn hóa, an toàn để sử dụng
    """
    # Áp dụng viết tắt nếu có và được bật
    if use_abbreviations:
        name_lower = name.lower().strip()
        # Tìm kiếm khớp một phần (để xử lý trường hợp có thêm thông tin như sys_id)
        for full_name, abbrev in TYPE_ABBREVIATIONS.items():
            if full_name.lower() in name_lower:
                # Thay thế phần khớp bằng viết tắt
                name = name.replace(full_name, abbrev)
                break
    
    # Thay thế khoảng trắng bằng dấu gạch dưới
    name = name.replace(" ", "_")
    
    # Thay thế các ký tự đặc biệt không hợp lệ cho tên file
    # Windows không cho phép: < > : " / \ | ? *
    # Thêm các ký tự khác có thể gây vấn đề
    invalid_chars = r'[<>:"/\\|?*\x00-\x1f]'
    name = re.sub(invalid_chars, "_", name)
    
    # Gộp nhiều dấu gạch dưới liên tiếp thành một
    name = re.sub(r'_+', '_', name)
    
    # Loại bỏ dấu gạch dưới ở đầu và cuối
    name = name.strip('_')
    
    # Đảm bảo tên file không rỗng
    if not name:
        name = "unknown"
    
    return name


def build_filename(data_type: str, timestamp: str, run_number: int,
                   use_abbreviations: bool = False) -> str:
    """
    Xây dựng tên file CSV từ các thành phần.
    
    Format: {sanitized_type}-{timestamp}-run{run_number}.csv
    
    Ví dụ:
        tc-20251118_143022-run1.csv
        fog-20251118_143045-run2.csv
    
    - Dùng dấu gạch ngang (-) để phân tách các đoạn khác nhau (type và timestamp)
    - Dùng dấu gạch dưới (_) để phân tách từ trong cùng một đoạn
    
    Args:
        data_type: Loại dữ liệu (có thể có khoảng trắng, ký tự đặc biệt)
        timestamp: Timestamp dạng YYYYMMDD_HHMMSS
        run_number: Lần chạy trong ngày (run1, run2, ...)
        use_abbreviations: Có sử dụng viết tắt không
    
    Returns:
        Tên file đã được xây dựng (không có extension .csv)
    """
    # Sanitize data_type
    safe_type = sanitize_filename(data_type, use_abbreviations)
    
    # Xây dựng tên file
    # Dùng dấu gạch ngang để phân tách các đoạn chính
    # Format cuối: {type}-{YYYYMMDD_HHMMSS}-run{N}
    filename = f"{safe_type}-{timestamp}-run{run_number}"
    
    return filename


def get_next_run_number(output_dir: Path) -> int:
    """
    Xác định số lần chạy tiếp theo (runN) trong thư mục output hiện tại.
    
    Dựa trên các file có format: *-runN.csv, lấy N lớn nhất và +1.
    Nếu chưa có file nào phù hợp, trả về 1.
    
    Args:
        output_dir: Thư mục chứa các file CSV của ngày hiện tại
    
    Returns:
        Số lần chạy tiếp theo (int >= 1)
    """
    max_run = 0
    
    if not output_dir.exists():
        return 1
    
    for path in output_dir.glob("*.csv"):
        stem = path.stem  # Tên file không có .csv
        parts = stem.split("-")
        if not parts:
            continue
        
        last_part = parts[-1]
        if last_part.startswith("run"):
            # Lấy phần số phía sau "run"
            number_part = last_part[3:]
            try:
                run_index = int(number_part)
            except ValueError:
                continue
            
            if run_index > max_run:
                max_run = run_index
    
    return max_run + 1


def load_links_from_file(file_path: str = None) -> List[str]:
    """
    Load danh sách links từ file text.
    
    Args:
        file_path: Đường dẫn đến file chứa links. Nếu None, sẽ tìm trong data/load_link/
    
    Returns:
        List các links (URLs)
    """
    if file_path is None:
        # Tìm file đầu tiên trong thư mục load_link
        load_link_dir = Path(__file__).parent / "data" / "load_link"
        load_link_dir.mkdir(parents=True, exist_ok=True)
        
        # Tìm tất cả file .txt trong thư mục
        txt_files = list(load_link_dir.glob("*.txt"))
        if not txt_files:
            raise FileNotFoundError(
                f"Không tìm thấy file links trong {load_link_dir}. "
                f"Vui lòng tạo file .txt chứa danh sách links, mỗi link một dòng."
            )
        file_path = txt_files[0]  # Lấy file đầu tiên
    
    links = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Bỏ qua dòng trống và comment
                links.append(line)
    
    print(f"Đã load {len(links)} links từ {file_path}")
    return links


def extract_json_from_url(url: str) -> Dict[str, Any]:
    """
    Tải và parse JSON từ URL.
    
    Args:
        url: URL của JSON file
    
    Returns:
        Dictionary chứa dữ liệu JSON
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi tải {url}: {e}")
        raise


def extract_jsons_to_csv(links: List[str], output_dir: str = None) -> List[str]:
    """
    Nhận danh sách links, tải JSON từ mỗi link và convert sang CSV.
    
    Cấu trúc thư mục + tên file:
    
    - Thư mục theo ngày: raw/YYYYMMDD/
        Ví dụ: data/raw/20251118/
    
    - Mỗi CSV file sẽ có tên: {sanitized_type}-{YYYYMMDD_HHMMSS}-runN.csv
    Trong đó:
        - sanitized_type: loại dữ liệu đã được chuẩn hóa (khoảng trắng -> _, ký tự đặc biệt -> _)
        - YYYYMMDD_HHMMSS: timestamp thời điểm bắt đầu chạy batch
        - runN: số lần chạy trong ngày (run1, run2, ...)
      Dùng dấu gạch ngang (-) để phân tách các đoạn khác nhau
      Dùng dấu gạch dưới (_) để phân tách từ trong cùng một đoạn
    
    Args:
        links: Danh sách URLs chứa JSON data
        output_dir: Thư mục lưu file CSV. Nếu None, sẽ lưu vào data/raw/
    
    Returns:
        List các đường dẫn file CSV đã tạo
    """
    # Thư mục gốc lưu dữ liệu raw
    if output_dir is None:
        base_output_dir = Path(__file__).parent / "data" / "raw"
    else:
        base_output_dir = Path(output_dir)
    
    # Xác định thời điểm chạy batch hiện tại
    run_datetime = datetime.now()
    run_date_str = run_datetime.strftime("%Y%m%d")         # Dùng cho tên thư mục ngày
    run_timestamp = run_datetime.strftime("%Y%m%d_%H%M%S") # Dùng cho tên file
    
    # Thư mục theo ngày: raw/YYYYMMDD/
    output_dir = base_output_dir / run_date_str
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Xác định số lần chạy (runN) trong ngày hiện tại
    run_number = get_next_run_number(output_dir)
    
    csv_files = []
    
    for idx, url in enumerate(links, 1):
        print(f"\n[{idx}/{len(links)}] Đang xử lý: {url}")
        
        try:
            # Tải JSON
            json_data = extract_json_from_url(url)
            
            # Xử lý trường hợp JSON trả về là list rỗng hoặc không đúng format
            if isinstance(json_data, list):
                if len(json_data) == 0:
                    print(f"   Cảnh báo: JSON trả về mảng rỗng (không có dữ liệu), bỏ qua link này")
                    continue
                else:
                    print(f"    Cảnh báo: JSON format không đúng (là list thay vì object), bỏ qua link này")
                    continue
            
            # Kiểm tra xem có phải là dict không
            if not isinstance(json_data, dict):
                print(f"    Cảnh báo: JSON format không đúng, bỏ qua link này")
                continue
            
            # Lấy thông tin type và fields
            data_type = json_data.get("type", "unknown")
            fields = json_data.get("fields", [])
            
            # Xử lý trường hợp không có fields - có thể là format tropical cyclone detail
            if not fields:
                # Kiểm tra xem có phải là format tropical cyclone detail không
                if "track" in json_data or "forecast" in json_data:
                    sys_id = json_data.get("sys_id", "unknown")
                    data_type = f"tropical cyclone detail_{sys_id}"
                    
                    # Xử lý track và forecast riêng biệt
                    csv_files_for_url = []
                    
                    # Xử lý track
                    if "track" in json_data and isinstance(json_data["track"], list) and len(json_data["track"]) > 0:
                        track_records = json_data["track"]
                        track_fields = list(track_records[0].keys())  # Lấy fields từ object đầu tiên
                        
                        # Gắn sys_id vào data_type để phân biệt hệ thống bão
                        # Ví dụ sau khi chuẩn hóa + viết tắt: tc_track_2025204-20251118_143022-run1.csv
                        data_type = f"tropical cyclone track_{sys_id}"
                        base_filename = build_filename(
                            data_type=data_type,
                            timestamp=run_timestamp,
                            run_number=run_number,
                            use_abbreviations=True,
                        )
                        csv_filename = f"{base_filename}.csv"
                        csv_path = output_dir / csv_filename
                        
                        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(track_fields)
                            
                            for record in track_records:
                                row = [record.get(field, "") for field in track_fields]
                                writer.writerow(row)
                        
                        print(f"   Đã tạo: {csv_path}")
                        print(f"  Số records: {len(track_records)}, Type: tropical cyclone track, Fields: {len(track_fields)}")
                        csv_files_for_url.append(str(csv_path))
                    
                    # Xử lý forecast
                    if "forecast" in json_data and isinstance(json_data["forecast"], list) and len(json_data["forecast"]) > 0:
                        forecast_records = json_data["forecast"]
                        forecast_fields = list(forecast_records[0].keys())
                        
                        # Gắn sys_id vào data_type để phân biệt hệ thống bão
                        # Ví dụ: tc_forecast_2025204-20251118_143022-run1.csv
                        data_type = f"tropical cyclone forecast_{sys_id}"
                        base_filename = build_filename(
                            data_type=data_type,
                            timestamp=run_timestamp,
                            run_number=run_number,
                            use_abbreviations=True,
                        )
                        csv_filename = f"{base_filename}.csv"
                        csv_path = output_dir / csv_filename
                        
                        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(forecast_fields)
                            
                            for record in forecast_records:
                                row = [record.get(field, "") for field in forecast_fields]
                                writer.writerow(row)
                        
                        print(f"   Đã tạo: {csv_path}")
                        print(f"  Số records: {len(forecast_records)}, Type: tropical cyclone forecast, Fields: {len(forecast_fields)}")
                        csv_files_for_url.append(str(csv_path))
                    
                    if csv_files_for_url:
                        csv_files.extend(csv_files_for_url)
                        continue
                    else:
                        print(f"    Cảnh báo: Không tìm thấy dữ liệu track hoặc forecast, bỏ qua link này")
                        continue
                else:
                    print(f"    Cảnh báo: Không tìm thấy 'fields' trong JSON và không phải format đặc biệt, bỏ qua link này")
                    continue
            
            # Tạo tên file sử dụng hàm build_filename
            # Format: {type}-{YYYYMMDD_HHMMSS}-runN.csv
            base_filename = build_filename(
                data_type=data_type,
                timestamp=run_timestamp,
                run_number=run_number,
                use_abbreviations=True,
            )
            csv_filename = f"{base_filename}.csv"
            csv_path = output_dir / csv_filename
            
            # Mở file CSV để ghi
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Ghi header
                writer.writerow(fields)
                
                # Ghi dữ liệu từ các timestamp keys hoặc key đặc biệt
                # Bỏ qua các key không phải dữ liệu (type, fields, update, sys_id, gts)
                total_records = 0
                for key in json_data.keys():
                    if key not in ["type", "fields", "update", "sys_id", "gts"]:
                        records = json_data[key]
                        if isinstance(records, list):
                            for record in records:
                                if isinstance(record, list):
                                    writer.writerow(record)
                                    total_records += 1
            
            print(f"   Đã tạo: {csv_path}")
            print(f"  Số records: {total_records}, Type: {data_type}, Fields: {len(fields)}")
            csv_files.append(str(csv_path))
            
        except Exception as e:
            print(f"  ❌ Lỗi khi xử lý {url}: {e}")
            continue
    
    return csv_files


def run(links_file_path: str = None, output_dir: str = None) -> None:
    """
    Hàm chính để chạy toàn bộ quá trình extract.
    
    Quy trình:
    1. Load danh sách links từ file
    2. Extract JSON từ các links và convert sang CSV
    
    Args:
        links_file_path: Đường dẫn đến file chứa links. Nếu None, tự động tìm trong data/load_link/
        output_dir: Thư mục lưu file CSV. Nếu None, lưu vào data/raw/
    """
    print("=" * 60)
    print("BẮT ĐẦU EXTRACT DỮ LIỆU TỪ JSON SANG CSV")
    print("=" * 60)
    
    try:
        # Bước 1: Load danh sách links
        print("\n[Bước 1] Đang load danh sách links...")
        links = load_links_from_file(links_file_path)
        
        if not links:
            print("⚠️  Không có links để xử lý!")
            return
        
        # Bước 2: Extract JSON và convert sang CSV
        print(f"\n[Bước 2] Đang extract {len(links)} links sang CSV...")
        csv_files = extract_jsons_to_csv(links, output_dir)
        
        # Tổng kết
        print("\n" + "=" * 60)
        print("HOÀN THÀNH!")
        print("=" * 60)
        print(f" Đã tạo {len(csv_files)} file CSV:")
        for csv_file in csv_files:
            print(f"   - {csv_file}")
        
    except Exception as e:
        print(f"\n❌ Lỗi trong quá trình xử lý: {e}")
        raise

