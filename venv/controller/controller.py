import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import re

# ======================================================================
# 1. DATABASE CONNECTION
# ======================================================================
DB_META_URL = os.getenv("DB_META_URL", "mysql+pymysql://root:1@localhost:3306/db_etl_metadata")
DB_TARGET_URL = os.getenv("DB_TARGET_URL", "mysql+pymysql://root:1@localhost:3306/db_stage_clean")

meta_engine = create_engine(DB_META_URL)
target_engine = create_engine(DB_TARGET_URL)

RAW_DIR = r"venv\\data\\raw"


# ======================================================================
# 2. Import clean functions
# ======================================================================
from clean_functions import CLEAN_FUNCTIONS


# ======================================================================
# 3. Clean DataFrame theo từng loại data_type
# ======================================================================
def clean_df(data_type, df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if data_type in CLEAN_FUNCTIONS:
        cleaner = CLEAN_FUNCTIONS[data_type]
        cleaned_rows = []

        for _, row in df.iterrows():
            cleaned = cleaner(row)
            if cleaned is not None:
                cleaned_rows.append(cleaned)

        df = pd.DataFrame(cleaned_rows)

    df = df.drop_duplicates()
    df = df.fillna("")
    return df


# ======================================================================
# 4. Lấy danh sách file cần ETL
# ======================================================================
def get_pending_files():
    query = """
        SELECT id, file_name, data_type
        FROM log_extract_event
        WHERE status = 'success'
    """
    return pd.read_sql(query, meta_engine)


# ======================================================================
# 5. Update status vào bảng log_extract_event
# ======================================================================
def update_status(row_id, status):
    query = text("""
        UPDATE log_extract_event
        SET status = :status
        WHERE id = :id
    """)
    with meta_engine.begin() as conn:
        conn.execute(query, {"status": status, "id": row_id})


# ======================================================================
# 6. Ghi log vào clean_log
# ======================================================================
def write_clean_log(file_name, status, data_type,
                    total_rows=None, inserted_rows=None,
                    error_msg=None, start_index=None,
                    end_index=None, fail_range=None):

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
            "process_time": datetime.utcnow(),
            "status": status,
            "total_rows": total_rows,
            "inserted_rows": inserted_rows,
            "error_msg": error_msg,
            "start_index": start_index,
            "end_index": end_index,
            "fail_range": fail_range,
            "table_type": data_type
        })


# ======================================================================
# 7. Auto-create clean_log table
# ======================================================================
def create_clean_log_table():
    sql = """
    CREATE TABLE IF NOT EXISTS clean_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        file_name VARCHAR(255) NOT NULL,
        process_time DATETIME NOT NULL,
        status VARCHAR(20) NOT NULL,
        total_rows INT NULL,
        inserted_rows INT NULL,
        error_msg TEXT NULL,
        start_index INT NULL,
        end_index INT NULL,
        fail_range VARCHAR(50) NULL,
        table_type VARCHAR(50) NULL
    );
    """
    with meta_engine.begin() as conn:
        conn.execute(text(sql))

create_clean_log_table()


# ======================================================================
# 8. ETL MAIN PROCESS
# ======================================================================
def process_etl():
    pending = get_pending_files()

    if pending.empty:
        print("Không có file cần ETL.")
        return

    for _, row in pending.iterrows():
        row_id = row["id"]
        filename = row["file_name"]
        data_type = row["data_type"].strip().lower()

        file_path = os.path.join(RAW_DIR, filename)
        print(f"\n=== ETL FILE: {filename} ({data_type}) ===")

        if not os.path.exists(file_path):
            print(f"❌ File không tồn tại: {file_path}")

            update_status(row_id, "file_missing")
            write_clean_log(filename, "file_missing", data_type, error_msg="File not found")
            continue

        try:
            df = pd.read_csv(file_path)
            total_rows = len(df)

            df = clean_df(data_type, df)

            df.to_sql(data_type, target_engine, if_exists="append", index=False)
            inserted_rows = len(df)

            update_status(row_id, "successed")

            write_clean_log(
                filename, "success", data_type,
                total_rows=total_rows,
                inserted_rows=inserted_rows,
                start_index=1,
                end_index=inserted_rows
            )

            print(f"✔ DONE: {filename} → bảng {data_type}")

        except Exception as e:
            error_msg = str(e)
            print(f"❌ Lỗi: {error_msg}")

            update_status(row_id, "failed")
            write_clean_log(filename, "failed", data_type, error_msg=error_msg)


# ======================================================================
# 9. Run ETL
# ======================================================================
if __name__ == "__main__":
    process_etl()
