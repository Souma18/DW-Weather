from database.base import session_scope
from clean.setup_db import *
# 2.5 Tạo kết nối với database "db_stage_clean"
engine_clean, SessionClean = connection_clean()
# 2.6.1 Tạo các table nếu chưa có
create_table_clean(engine_clean)

from etl_metadata.models import LogExtractEvent
from database.logger import log_db_status
import os
from pathlib import Path
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    JSON,
)

metadata = MetaData()
from clean.models import Fog, Gale, HeavyRain, Thunderstorms, TCTrack, TC
import csv
from clean.clean_functions import CLEAN_FUNCTIONS
from etl_metadata.models import CleanLog
from database.logger import log_dual_status


# DATA_DIR mount từ docker-compose.airflow.yaml: ./data -> /opt/airflow/data
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))

def get_success_logs():
    with session_scope(SessionELT) as session:
        logs = (
            session.query(LogExtractEvent)
            .filter(LogExtractEvent.status == "EXTRACTED")
            .all()
        )

        return [
            {
                "id": log.id,
                "file_name": log.file_name,
                "data_type": log.data_type,
                "record_count": log.record_count,
                "created_at": log.created_at
            }
            for log in logs
        ]

def get_all_raw_files():
    """
    Lấy danh sách tất cả file raw dưới thư mục data/raw (trong DATA_DIR).
    """
    root_path = DATA_DIR / "raw"
    file_paths = []
    for root, dirs, files in os.walk(root_path):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def get_extracted_files():
    logs = get_success_logs()
    success_files = []
    for log in logs:
        created_at = log["created_at"]
        file_name = log["file_name"]

        base_dir = DATA_DIR / "raw"
        dir_name = created_at.strftime("%Y%m%d")
        dir_path = base_dir / dir_name
        file_path = dir_path / file_name

        success_files.append(
            {
                "file_path": str(file_path),
                "data_type": standardize_table_type(log["data_type"]),
            }
        )
    return success_files

def standardize_table_type(original_type):
    count = original_type.count("_")
    if count>1:
        return original_type.rsplit("_",1)[0]
    else:
        return original_type

MODEL_MAP = {
    "fog": Fog,
    "gale": Gale,
    "heavyrain_snow": HeavyRain,
    "thunderstorms": Thunderstorms,
    "tc_track": TCTrack,
    "tc": TC
}

def get_model_by_data_type(data_type):
    return MODEL_MAP.get(standardize_table_type(data_type))

def parse_file(file_path):
    with open(file_path,"r",encoding="utf-8") as f:
        data = list(csv.reader(f))
    return data

def run_clean_and_insert_all():
    # 2.7. Lấy tất cả các file trong raw\data dựa vào status "EXTRACTED" của table "log_extract_event"
    # của database "db_etl_metadata"
    files = get_extracted_files()

    for file_info in files:
        file_path = file_info["file_path"]
        data_type = file_info["data_type"]

        print(f"\nProcessing: {file_path}")

        clean_fn = CLEAN_FUNCTIONS.get(data_type)
        if not clean_fn:
            # print(f"[ERROR] Không có hàm clean cho {data_type}")
            log_dual_status(clean_log, SessionELT,"Lỗi hệ thống DW-Weather",f"Không có hàm clean cho {data_type}")
            continue 

        Model = get_model_by_data_type(data_type)
        if not Model:
            print(f"[ERROR] Không có model cho {data_type}")
            continue 

        try:
            rows = parse_file(file_path)
        except Exception as e:
            # LOG lỗi đọc file
            clean_log = CleanLog(
                    file_name=os.path.basename(file_path),
                    status="FAILED",
                    total_rows=0,
                    inserted_rows=0,
                    error_msg=str(e),
                    start_index=None,
                    end_index=None,
                    fail_range=None,
                    table_type=data_type
                )
            
            # Gửi email thông báo lỗi đọc file
            log_dual_status(clean_log, SessionELT,"Lỗi hệ thống DW-Weather.",f"Lỗi khi đọc file: {file_path}")
            continue

        headers = rows[0]
        records = rows[1:]
        total_rows = len(records)
        batch = []
        fail_indices = []

        for idx, row in enumerate(records):
            try:
                row_dict = dict(zip(headers, row))
                # 2.8. Chuẩn hóa và làm sạch dữ liệu
                cleaned = clean_fn(row_dict)
                if cleaned:
                    batch.append(cleaned)
                else:
                    fail_indices.append(idx)
            except Exception:
                fail_indices.append(idx)
                continue

        inserted_count = len(batch)

        if fail_indices:
            fail_range = f"{fail_indices[0]}-{fail_indices[-1]}"
        else:
            fail_range = None

        # Insert DB
        try:
            with session_scope(SessionClean) as session:
                # 2.9. Lưu dữ liệu vào database "db_stage_clean" và log vào table "clean_log" của database "db_etl_metadata" với status CLEANED
                session.add_all(batch)
                print(f"[OK] Inserted {inserted_count} rows from {data_type}")

        except Exception as e:
            print(f"[DB ERROR] Lỗi insert data_clean: {e}")

            # LOG lỗi DB
            clean_log = CleanLog(
                    file_name=os.path.basename(file_path),
                    status="FAILED",
                    total_rows=total_rows,
                    inserted_rows=0,
                    error_msg=str(e),
                    start_index=0,
                    end_index=total_rows,
                    fail_range="0-" + str(total_rows),
                    table_type=data_type
                )

            # Gửi email thông báo lỗi DB
            log_dual_status(clean_log, SessionELT,"Lỗi hệ thống DW-Weather","Lỗi insert db vào data_clean")
            continue

        # Ghi CLEAN SUCCESS LOG + cập nhật LogExtractEvent
        try:
            with session_scope(SessionELT) as session:
                # update LogExtractEvent
                log_row = (
                    session.query(LogExtractEvent)
                    .filter(LogExtractEvent.file_name == os.path.basename(file_path))
                    .first()
                )

                if log_row:
                    log_row.status = "CLEANED"
                # ghi clean log
                clean_log = CleanLog(
                    file_name=os.path.basename(file_path),
                    status="CLEANED",
                    total_rows=total_rows,
                    inserted_rows=inserted_count,
                    error_msg=None,
                    start_index=0 if inserted_count else None,
                    end_index=inserted_count,
                    fail_range=fail_range,
                    table_type=data_type
                )
                session.add(clean_log)
                print(f"[LOG] Updated LogExtractEvent + CleanLog saved")

        except Exception as e:
            # print(f"[LOG ERROR] Không update được log: {e}")
            log_dual_status(clean_log, SessionELT, "Lỗi hệ thống DW-Weather",f"Không update được log: {e}")

