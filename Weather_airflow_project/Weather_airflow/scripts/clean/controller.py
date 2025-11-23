import os
import re
from datetime import datetime, timezone
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from clean.clean_functions import CLEAN_FUNCTIONS

# -----------------------
# Configuration
# -----------------------
DB_META_URL = os.getenv("DB_META_URL", "mysql+pymysql://root:1@localhost:3306/db_etl_metadata")
DB_TARGET_URL = os.getenv("DB_TARGET_URL", "mysql+pymysql://root:1@localhost:3306/db_stage_clean")
RAW_DIR = os.getenv("RAW_DIR", r"venv\\data\\raw") #Thay đổi cho phù hợp 

# Engines
meta_engine: Engine = create_engine(DB_META_URL)
target_engine: Engine = create_engine(DB_TARGET_URL)

# -----------------------
# Utility helpers
# -----------------------
def normalize_col_name(name: str) -> str:
    if name is None:
        return ""
    return re.sub(r"\s+", "_", str(name).strip().lower())

def df_infer_sql_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series.dtype):
        return "INT"
    if pd.api.types.is_float_dtype(series.dtype):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series.dtype) or pd.api.types.is_timedelta64_dtype(series.dtype):
        return "DATETIME"
    # fallback: check first 10 non-null values
    if series.dropna().size > 0:
        try:
            maybe_dt = pd.to_datetime(series.dropna().iloc[:10], errors="coerce")
            if maybe_dt.notna().sum() >= max(1, int(0.5 * min(10, series.dropna().shape[0]))):
                return "DATETIME"
        except Exception:
            pass
    return "TEXT"

def get_pending_files() -> pd.DataFrame:
    query = """
        SELECT id, file_name, data_type
        FROM log_extract_event
        WHERE status = 'success'
    """
    return pd.read_sql(query, meta_engine)

def update_status(row_id: int, status: str):
    query = text("""UPDATE log_extract_event SET status = :status WHERE id = :id""")
    with meta_engine.begin() as conn:
        conn.execute(query, {"status": status, "id": row_id})

def write_clean_log(file_name: str, status: str, data_type: str,
                    total_rows: int = None, inserted_rows: int = None,
                    error_msg: str = None, start_index: int = None,
                    end_index: int = None, fail_range: str = None):
    table_type_clean = remove_digits_from_name(data_type)  # loại bỏ số trước khi ghi vào table_type

    query = text("""
        INSERT INTO clean_log (
            file_name, process_time, status, total_rows, inserted_rows,
            error_msg, start_index, end_index, fail_range, table_type
        )
        VALUES (
            :file_name, :process_time, :status, :total_rows, :inserted_rows,
            :error_msg, :start_index, :end_index, :fail_range, :table_type
        )
    """)
    with meta_engine.begin() as conn:
        conn.execute(query, {
            "file_name": file_name,
            "process_time": datetime.now(timezone.utc),
            "status": status,
            "total_rows": total_rows,
            "inserted_rows": inserted_rows,
            "error_msg": error_msg,
            "start_index": start_index,
            "end_index": end_index,
            "fail_range": fail_range,
            "table_type": table_type_clean  # dùng version không số
        })


# -----------------------
# Schema management
# -----------------------
def create_table_from_df(table: str, df: pd.DataFrame):
    cols = []
    for col in df.columns:
        sql_type = df_infer_sql_type(df[col])
        cols.append(f"`{col}` {sql_type}")
    col_defs = ",\n    ".join(cols) if cols else ""
    sql = f"""
    CREATE TABLE IF NOT EXISTS `{table}` (
        id INT AUTO_INCREMENT PRIMARY KEY
        {', ' if col_defs else ''}{col_defs}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    with target_engine.begin() as conn:
        conn.execute(text(sql))

def get_existing_columns(table: str) -> List[str]:
    try:
        df = pd.read_sql(f"SHOW COLUMNS FROM `{table}`", target_engine)
        return list(df["Field"].astype(str))
    except Exception:
        return []

def add_missing_columns(table: str, df: pd.DataFrame):
    existing = set(get_existing_columns(table))
    for col in df.columns:
        if col in existing:
            continue
        sql_type = df_infer_sql_type(df[col])
        with target_engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN `{col}` {sql_type};"))

# -----------------------
# Remove digits from string for table names
# -----------------------
def remove_digits_from_name(name: str) -> str:
    if not name:
        return ""
    # Remove all digits from string
    return re.sub(r"\d+", "", name)

# -----------------------
# Cleaning orchestration
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

    # convert datetime-like columns
    for col in df_cleaned.columns:
        if df_cleaned[col].apply(lambda v: pd.notna(v) and isinstance(v, (pd.Timestamp, datetime))).any():
            try:
                df_cleaned[col] = pd.to_datetime(df_cleaned[col], errors="coerce")
            except Exception:
                pass

    return df_cleaned

# -----------------------
# Main ETL process
# -----------------------
def process_etl():
    pending = get_pending_files()

    if pending.empty:
        print("Không có file cần ETL.")
        return

    for _, row in pending.iterrows():
        row_id = row["id"]
        filename = row["file_name"]
        raw_data_type = str(row["data_type"]).strip().lower()
        # Remove digits from data_type for table name
        data_type = remove_digits_from_name(raw_data_type)

        file_path = os.path.join(RAW_DIR, filename)

        print(f"\n=== ETL FILE: {filename} (raw type: {raw_data_type} -> table name: {data_type}) ===")

        if not os.path.exists(file_path):
            print(f"❌ File không tồn tại: {file_path}")
            update_status(row_id, "file_missing")
            write_clean_log(filename, "file_missing", raw_data_type, error_msg="File not found")
            continue

        try:
            raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=[""])
        except Exception as e:
            err = str(e)
            print(f"❌ Không thể đọc file CSV: {err}")
            update_status(row_id, "failed_read")
            write_clean_log(filename, "failed", raw_data_type, error_msg=err)
            continue

        total_rows = len(raw_df)
        df_cleaned = clean_df_by_type(raw_data_type, raw_df)

        if df_cleaned.empty:
            print("⚠️ Sau khi clean không còn hàng nào hợp lệ — bỏ qua.")
            update_status(row_id, "no_valid_rows")
            write_clean_log(filename, "no_valid_rows", raw_data_type, total_rows=total_rows, inserted_rows=0)
            continue

        df_cleaned.columns = [re.sub(r"[^\w\d_]", "_", c) for c in df_cleaned.columns]

        # create table and add missing columns
        create_table_from_df(data_type, df_cleaned)
        add_missing_columns(data_type, df_cleaned)

        # insert rows
        try:
            df_cleaned.to_sql(data_type, target_engine, if_exists="append", index=False)
            inserted_rows = len(df_cleaned)
            update_status(row_id, "successed")
            write_clean_log(
                filename, "success", raw_data_type,
                total_rows=total_rows,
                inserted_rows=inserted_rows,
                start_index=1,
                end_index=inserted_rows
            )
            print(f"✔ DONE: {filename} → bảng {data_type} (inserted {inserted_rows} rows)")
        except Exception as e:
            err = str(e)
            print(f"❌ Lỗi khi insert: {err}")
            update_status(row_id, "failed")
            write_clean_log(filename, "failed", raw_data_type, error_msg=err)

if __name__ == "__main__":
    process_etl()
