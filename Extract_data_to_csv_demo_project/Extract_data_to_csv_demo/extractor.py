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
    
    Mỗi CSV file sẽ có tên: {type}_{YYYYMMDDHHMMSS}.csv
    Trong đó:
        - type: loại dữ liệu từ trường "type" trong JSON
        - YYYYMMDDHHMMSS: timestamp hiện tại khi tạo file
    
    Args:
        links: Danh sách URLs chứa JSON data
        output_dir: Thư mục lưu file CSV. Nếu None, sẽ lưu vào data/raw/
    
    Returns:
        List các đường dẫn file CSV đã tạo
    """
    if output_dir is None:
        output_dir = Path(__file__).parent / "data" / "raw"
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    csv_files = []
    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
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
            
            if not fields:
                print(f"    Cảnh báo: Không tìm thấy 'fields' trong JSON, bỏ qua link này")
                continue
            
            # Tạo tên file: {type}_{timestamp}.csv
            # Sanitize type để tránh ký tự không hợp lệ trong tên file (như "/" trong "heavyrain/snow")
            safe_type = str(data_type).replace("/", "_").replace("\\", "_").replace(":", "_")
            base_filename = f"{safe_type}_{current_timestamp}"
            if idx > 1:
                base_filename = f"{base_filename}_{idx}"
            csv_filename = f"{base_filename}.csv"
            csv_path = output_dir / csv_filename
            
            # Mở file CSV để ghi
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Ghi header
                writer.writerow(fields)
                
                # Ghi dữ liệu từ các timestamp keys
                # Bỏ qua các key không phải timestamp (type, fields)
                total_records = 0
                for key in json_data.keys():
                    if key not in ["type", "fields"]:
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

