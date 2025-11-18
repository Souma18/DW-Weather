import os
import pandas as pd
from sqlalchemy import Column, Integer, String, Float
from database import engine, Base, SessionLocal
from etl_metadata import LogSessionLocal, CleanLog, init_log_db
from datetime import datetime
from typing import Type


CSV_PATH = "venv\\clean\\weather\\raw\\"


def sqlalchemy_type_for_series(series: pd.Series):
    """Detect SQLAlchemy column type from pandas Series dtype."""
    if pd.api.types.is_integer_dtype(series):
        return Integer
    if pd.api.types.is_float_dtype(series):
        return Float
    return String(255)


def create_dynamic_model(tablename: str, df: pd.DataFrame) -> Type[Base]:
    """Dynamically create SQLAlchemy model for given DataFrame."""
    columns = {
        "__tablename__": tablename,
        "id": Column(Integer, primary_key=True, autoincrement=True),
    }
    for col in df.columns:
        col_type = sqlalchemy_type_for_series(df[col])
        columns[col] = Column(col_type)
    GeneratedModel = type(tablename, (Base,), columns)
    return GeneratedModel


def log_clean(file_name: str, status: str, total_rows=None, inserted_rows=None, error_msg=None,
              success_range=None, fail_range=None) -> None:
    session = LogSessionLocal()
    try:
        log = CleanLog(
            file_name=file_name,
            process_time=datetime.now(),
            status=status,
            total_rows=total_rows,
            inserted_rows=inserted_rows,
            error_msg=error_msg,
            success_range=success_range,
            fail_range=fail_range
        )
        session.add(log)
        session.commit()
    finally:
        session.close()


def process_csv_files(csv_path: str) -> None:
    """Process all CSV files in given path, store cleaned data in DB and log results."""
    from sqlalchemy.exc import SQLAlchemyError
    dynamic_models = {}
    init_log_db()

    for fname in os.listdir(csv_path):
        if fname.endswith(".csv"):
            filepath = os.path.join(csv_path, fname)
            try:
                tablename = os.path.splitext(fname)[0]
                df = pd.read_csv(filepath)

                # Basic preprocessing
                df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
                df = df.dropna()
                for c in df.select_dtypes("object").columns:
                    df[c] = df[c].astype(str).str.strip()

                Model = create_dynamic_model(tablename, df)
                dynamic_models[tablename] = (Model, df)
            except Exception as ex:
                # Log file-read failures, cả file coi như fail từ dòng 1~N
                fail_range = f"1~{len(pd.read_csv(filepath)) if os.path.exists(filepath) else 0}"
                log_clean(fname, status="fail", error_msg=str(ex), fail_range=fail_range)

    # Create tables for all valid CSV-derived models
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()
    try:
        for tablename, (Model, df) in dynamic_models.items():
            fname = f"{tablename}.csv"
            total_rows = len(df)
            try:
                data_dicts = df.to_dict(orient="records")
                objs = [Model(**row) for row in data_dicts]
                session.add_all(objs)
                session.commit()
                inserted_rows = len(objs)
                # Thành công từ dòng 1 ~ inserted_rows (với dữ liệu đã dropna)
                success_range = f"1~{inserted_rows}" if inserted_rows > 0 else None
                log_clean(fname, status="success", total_rows=total_rows, inserted_rows=inserted_rows,
                          success_range=success_range)
            except SQLAlchemyError as ex:
                session.rollback()
                # Fail toàn bộ, ghi số dòng bị fail
                fail_range = f"1~{total_rows}" if total_rows > 0 else None
                log_clean(fname, status="fail", total_rows=total_rows, error_msg=str(ex),
                          fail_range=fail_range)
    finally:
        session.close()


if __name__ == "__main__":
    process_csv_files(CSV_PATH)