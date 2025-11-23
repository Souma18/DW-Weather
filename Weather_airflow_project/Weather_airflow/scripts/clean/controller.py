import os
import re
from datetime import datetime, timezone
import pandas as pd

from database.setup_db import engine_clean, SessionClean, engine_elt, SessionELT
from database.logger import log_dual_status
from clean.clean_functions import CLEAN_FUNCTIONS

from elt_metadata.models import LogExtractEvent,CleanLog

RAW_DIR = os.getenv("RAW_DIR", r"venv\\data\\raw")

# -----------------------
# Utils
# -----------------------
def normalize_col_name(name: str) -> str:
    if name is None:
        return ""
    return re.sub(r"\s+", "_", str(name).strip().lower())

def remove_digits_from_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r"\d+", "", name)

def df_infer_sql_type(series: pd.Series):
    if pd.api.types.is_integer_dtype(series):
        return "INT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATETIME"
    # fallback
    if series.dropna().size > 0:
        maybe_dt = pd.to_datetime(series.dropna().iloc[:10], errors="coerce")
        if maybe_dt.notna().sum() >= max(1, int(0.5 * min(10, series.dropna().shape[0]))):
            return "DATETIME"
    return "TEXT"

# -----------------------
# Cleaning
# -----------------------
def clean_df_by_type(data_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    raw_df.columns = [normalize_col_name(c) for c in raw_df.columns]
    cleaner = CLEAN_FUNCTIONS.get(data_type)
    if not cleaner:
        return raw_df.drop_duplicates().fillna(pd.NA).dropna(axis=1, how='all')

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
    df_cleaned = df_cleaned.dropna(axis=1, how='all')
    # convert datetime
    for col in df_cleaned.columns:
        if df_cleaned[col].apply(lambda v: pd.notna(v) and isinstance(v, (pd.Timestamp, datetime))).any():
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors="coerce")
            except Exception:
                pass
    return df_cleaned

# -----------------------
# ETL Process
# -----------------------
def process_etl():
    with SessionELT() as meta_session, SessionClean() as target_session:
        # Lấy các file cần ETL
        pending_files = meta_session.query(LogExtractEvent).filter(LogExtractEvent.status=="success").all()
        if not pending_files:
            print("Không có file cần ETL.")
            return

        for row in pending_files:
            row_id = row.id
            filename = row.file_name
            raw_data_type = str(row.data_type).strip().lower()
            table_name = remove_digits_from_name(raw_data_type)
            file_path = os.path.join(RAW_DIR, filename)

            print(f"\n=== ETL FILE: {filename} (type: {raw_data_type} → table: {table_name}) ===")

            if not os.path.exists(file_path):
                row.status = "file_missing"
                meta_session.commit()
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="file_missing",
                        process_time=datetime.now(timezone.utc),
                        error_msg="File not found"
                    ),
                    SessionELT,
                    os.getenv("RECIEVER_EMAIL"),
                    "File ETL Missing",
                    f"File {filename} không tồn tại"
                )
                continue

            # đọc CSV
            try:
                raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=[""])
            except Exception as e:
                row.status = "failed_read"
                meta_session.commit()
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="failed",
                        process_time=datetime.now(timezone.utc),
                        error_msg=str(e)
                    ),
                    SessionELT,
                    os.getenv("RECIEVER_EMAIL"),
                    "Read CSV Failed",
                    f"Không thể đọc file {filename}: {e}"
                )
                continue

            total_rows = len(raw_df)
            df_cleaned = clean_df_by_type(raw_data_type, raw_df)

            if df_cleaned.empty:
                row.status = "no_valid_rows"
                meta_session.commit()
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="no_valid_rows",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        inserted_rows=0
                    ),
                    SessionELT,
                    os.getenv("RECIEVER_EMAIL"),
                    "No Valid Rows",
                    f"File {filename} sau khi clean không còn dòng hợp lệ"
                )
                continue

            df_cleaned.columns = [re.sub(r"[^\w\d_]", "_", c) for c in df_cleaned.columns]

            # tạo bảng nếu chưa có
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
                        inserted_rows=inserted_rows
                    ),
                    SessionELT,
                    os.getenv("RECIEVER_EMAIL"),
                    "ETL Success",
                    f"File {filename} → bảng {table_name} (inserted {inserted_rows} rows)"
                )
                print(f"✔ DONE: {filename} → {table_name} ({inserted_rows} rows)")
            except Exception as e:
                row.status = "failed"
                meta_session.commit()
                log_dual_status(
                    CleanLog(
                        file_name=filename,
                        status="failed",
                        process_time=datetime.now(timezone.utc),
                        total_rows=total_rows,
                        error_msg=str(e)
                    ),
                    SessionELT,
                    os.getenv("RECIEVER_EMAIL"),
                    "ETL Failed",
                    f"Lỗi khi insert file {filename}: {e}"
                )
                print(f"❌ Lỗi insert: {e}")    
