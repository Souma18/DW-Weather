import os
import re
from datetime import datetime, timezone
from typing import List

import pandas as pd
from database.setup_db import engine_clean, SessionClean, engine_elt, SessionELT
from database.logger import log_dual_status
from clean.clean_functions import CLEAN_FUNCTIONS
from elt_metadata.models import LogExtractEvent, CleanLog

RAW_DIR = os.getenv("RAW_DIR", r"data\\raw")
RECIEVER_EMAIL = os.getenv("RECIEVER_EMAIL")


# -----------------------
# Utils
# -----------------------
def normalize_col_name(name: str) -> str:
    return re.sub(r"\s+", "_", str(name).strip().lower()) if name else ""


def remove_digits_from_name(name: str) -> str:
    return re.sub(r"\d+", "", name) if name else ""


def df_infer_sql_type(series: pd.Series):
    if pd.api.types.is_integer_dtype(series):
        return "INT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    if series.dropna().size > 0:
        maybe_dt = pd.to_datetime(series.dropna().iloc[:10], errors="coerce")
        if maybe_dt.notna().sum() >= max(1, int(0.5 * min(10, series.dropna().shape[0]))):
            return "DATETIME"
    return "TEXT"


# -----------------------
# Data Cleaning
# -----------------------
def clean_df_by_type(data_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    raw_df.columns = [normalize_col_name(c) for c in raw_df.columns]
    cleaner = CLEAN_FUNCTIONS.get(data_type)

    if not cleaner:
        return raw_df.drop_duplicates().fillna(pd.NA).dropna(axis=1, how="all")

    cleaned_rows = []
    for _, row in raw_df.iterrows():
        try:
            cleaned = cleaner(row.to_dict())
        except Exception:
            cleaned = None
        if cleaned:
            cleaned_rows.append(cleaned)

    if not cleaned_rows:
        return pd.DataFrame(columns=[])

    df_cleaned = pd.DataFrame(cleaned_rows)
    df_cleaned.columns = [normalize_col_name(c) for c in df_cleaned.columns]
    df_cleaned = df_cleaned.drop_duplicates().fillna(pd.NA)
    df_cleaned = df_cleaned.dropna(axis=1, how="all")

    for col in df_cleaned.columns:
        if df_cleaned[col].apply(lambda v: pd.notna(v) and isinstance(v, (pd.Timestamp, datetime))).any():
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors="coerce")
            except Exception:
                pass

    return df_cleaned


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
        pending_files = meta_session.query(LogExtractEvent).filter(LogExtractEvent.status == "success").all()

        if not pending_files:
            print("Không có file cần ETL.")
            return

        for row in pending_files:
            filename = row.file_name
            raw_data_type = str(row.data_type).strip().lower()
            table_name = remove_digits_from_name(raw_data_type)

            print(f"\n=== ETL FILE: {filename} (type: {raw_data_type} → table: {table_name}) ===")

            # --- Tìm file ---
            file_path = find_file_path(filename)
            if not file_path:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="file_missing",
                        process_time=datetime.now(timezone.utc),
                        total_rows=None,
                        inserted_rows=None,
                        error_msg="File not found",
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "File ETL Missing",
                    f"File {filename} không tồn tại"
                )
                continue

            # --- Read CSV ---
            try:
                raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=[""])
            except Exception as e:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="failed",
                        process_time=datetime.now(timezone.utc),
                        total_rows=None,
                        inserted_rows=None,
                        error_msg=str(e),
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "Read CSV Failed",
                    f"Không thể đọc file {filename}: {e}"
                )
                continue

            total_rows = len(raw_df)
            df_cleaned = clean_df_by_type(raw_data_type, raw_df)

            # --- No valid rows ---
            if df_cleaned.empty:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="no_valid_rows",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=0,
                        error_msg=None,
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "No Valid Rows",
                    f"File {filename} sau khi clean không còn dòng hợp lệ"
                )
                continue

            df_cleaned.columns = [re.sub(r"[^\w\d_]", "_", c) for c in df_cleaned.columns]

            # --- Insert DB ---
            try:
                df_cleaned.to_sql(table_name, target_session.get_bind(), if_exists="append", index=False)
                inserted_rows = len(df_cleaned)
                row.status = "successed"
                meta_session.commit()

                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="success",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=inserted_rows,
                        error_msg=None,
                        start_index=1,
                        end_index=inserted_rows,
                        fail_range=None,
                        table_type=table_name
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "ETL Success",
                    f"File {filename} → bảng {table_name} (inserted {inserted_rows} rows)"
                )
                print(f"✔ DONE: {filename} → {table_name} ({inserted_rows} rows)")

            except Exception as e:
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="failed",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=None,
                        error_msg=str(e),
                        start_index=None,
                        end_index=None,
                        fail_range=None,
                        table_type=table_name
                    ),
                    SessionELT,
                    RECIEVER_EMAIL,
                    "ETL Failed",
                    f"Lỗi khi insert file {filename}: {e}"
                )
                print(f"❌ Lỗi insert: {e}")


if __name__ == "__main__":
    process_etl()
