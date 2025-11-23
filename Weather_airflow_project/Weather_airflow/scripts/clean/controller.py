import os
import re
from datetime import datetime, timezone
from typing import List, Any

import pandas as pd
from database.setup_db import SessionClean, SessionELT
from database.logger import log_dual_status
from elt_metadata.models import LogExtractEvent, CleanLog

# Clean functions and ORM models (3-file layout)
from clean.clean_functions import CLEAN_FUNCTIONS

RAW_DIR = os.getenv("RAW_DIR", r"data\\raw")
RECIEVER_EMAIL = os.getenv("RECIEVER_EMAIL")


# -----------------------
# Utils
# -----------------------
def normalize_col_name(name: str) -> str:
    return re.sub(r"\s+", "_", str(name).strip().lower()) if name else ""


def remove_digits_from_name(name: str) -> str:
    return re.sub(r"\d+", "", name) if name else ""

def get_base_type(filename: str) -> str:
    # Lấy từ đầu đến phần đuôi có dạng _<số>
    return re.sub(r'_\d+.*$', '', filename)

# -----------------------
# Data Cleaning
# -----------------------
def process_raw_df_to_models(data_type: str, raw_df: pd.DataFrame) -> List[Any]:
    """
    Process raw DataFrame:
    1. Normalize column names
    2. Drop duplicates
    3. Apply cleaner function to each row -> returns Model Instance
    4. Collect valid instances
    """
    # 1. Normalize column names
    raw_df.columns = [normalize_col_name(c) for c in raw_df.columns]
    
    # 2. Drop duplicates on raw data first
    # Note: This drops exact duplicate rows in the raw CSV
    raw_df = raw_df.drop_duplicates()

    cleaner = CLEAN_FUNCTIONS.get(data_type)
    if not cleaner:
        return []

    valid_instances = []
    for _, row in raw_df.iterrows():
        try:
            # 3. Apply cleaner function
            # cleaner now returns a Model Instance (e.g., Fog(...)) or None
            instance = cleaner(row.to_dict())
            if instance:
                valid_instances.append(instance)
        except Exception:
            continue

    return valid_instances


# -----------------------
# File utilities
# -----------------------
def get_all_raw_files() -> List[tuple]:
    """Danh sách tất cả file CSV trong RAW_DIR và các folder con"""
    all_files = []
    for root, _, files in os.walk(RAW_DIR):
        for f in files:
            if f.lower().endswith(".csv"):
                all_files.append((os.path.join(root, f), f))
    return all_files


def find_file_path(filename: str):
    """Tìm file theo tên trong tất cả folder con"""
    for path, fname in get_all_raw_files():
        if fname == filename:
            return path
    return None


# -----------------------
# ETL Process
# -----------------------
def process_etl():

    with SessionELT() as meta_session, SessionClean() as target_session:
        pending_files = meta_session.query(LogExtractEvent).filter(LogExtractEvent.status == "SUCCESS").all()

        if not pending_files:
            print("Không có file cần ETL.")
            return

        for row in pending_files:
            filename = row.file_name
            raw_data_type = str(row.data_type).strip().lower()
            table_name = get_base_type(raw_data_type)

            print(f"\n=== ETL FILE: {filename} (type: {raw_data_type} → table: {table_name}) ===")

            # --- Tìm file ---
            file_path = find_file_path(filename)
            if not file_path:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="FAILED",
                        process_time=datetime.now(timezone.utc),
                        total_rows=None,
                        inserted_rows=None,
                        error_msg="File not found",
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name,
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "File ETL Missing",
                    f"File {filename} không tồn tại",
                )
                continue

            # --- Read CSV ---
            try:
                raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=[""])
            except Exception as e:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="FAILED",
                        process_time=datetime.now(timezone.utc),
                        total_rows=None,
                        inserted_rows=None,
                        error_msg=str(e),
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name,
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "Read CSV Failed",
                    f"Không thể đọc file {filename}: {e}",
                )
                continue

            total_rows = len(raw_df)
            
            # --- Process & Clean ---
            # Convert directly to Model Instances
            instances = process_raw_df_to_models(table_name, raw_df)

            # --- No valid rows ---
            if not instances:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="FAILED",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=0,
                        error_msg=f"File {filename} sau khi clean không còn dòng hợp lệ",
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name,
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "No Valid Rows",
                    f"File {filename} sau khi clean không còn dòng hợp lệ",
                )
                continue

            # --- Insert DB using ORM ---
            try:
                target_session.add_all(instances)
                target_session.commit()
                inserted_rows = len(instances)
                row.status = "SUCCESSED"
                meta_session.commit()

                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="SUCCESS",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=inserted_rows,
                        error_msg=None,
                        start_index=1,
                        end_index=inserted_rows,
                        fail_range=None,
                        table_type=table_name,
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "ETL Success",
                    f"File {filename} → bảng {table_name} (inserted {inserted_rows} rows)",
                )
                print(f"✔ DONE: {filename} → {table_name} ({inserted_rows} rows)")

            except Exception as e:
                try:
                    target_session.rollback()
                except Exception:
                    pass

                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="FAILED",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=None,
                        error_msg=str(e),
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name,
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "ETL Failed",
                    f"Lỗi khi insert file {filename}: {e}",
                )
                print(f"❌ Lỗi insert ORM: {e}")