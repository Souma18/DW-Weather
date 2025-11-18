"""
Khung ETL đơn giản (Load -> Transform -> Load) sử dụng:
- pandas để load/transform dữ liệu
- SQLAlchemy ORM để ghi vào DB transform
"""
import logging
import os
import sys
from typing import Optional

import pandas as pd

# Khi chạy trực tiếp từ file, đảm bảo thư mục chứa module được thêm vào sys.path
sys.path.append(os.path.dirname(__file__))

import database
import models

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")


def load_users_from_db(engine) -> Optional[pd.DataFrame]:
    """Load bảng `users` từ database nguồn bằng pandas.

    Trả về DataFrame hoặc None nếu không thể load.
    """
    try:
        logger.info("Loading users from source DB...")
        # Sử dụng query đơn giản; pandas sẽ dùng SQLAlchemy engine
        df = pd.read_sql("SELECT id, name, email FROM users", con=engine)
        logger.info("Loaded %d rows from source DB", len(df))
        return df
    except Exception as exc:  # pragma: no cover - demo fallback
        logger.warning("Could not load from source DB: %s", exc)
        return None


def transform_users(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data: chuẩn hóa tên (VI: chữ hoa toàn bộ) và trích domain email.

    - `name_upper`: tên viết hoa
    - `email_domain`: phần domain của email (nếu có)
    """
    logger.info("Transforming data...")
    df = df.copy()
    # Chuẩn hoá tên: viết hoa toàn bộ
    df["name_upper"] = df["name"].astype(str).str.strip().str.upper()
    # Tách domain email để ví dụ thêm cột
    df["email_domain"] = df["email"].astype(str).str.split("@").str[-1].where(df["email"].notna(), None)
    logger.info("Transformed %d rows", len(df))
    return df


def save_to_transform_db(df: pd.DataFrame, engine, SessionLocal) -> None:
    """Lưu DataFrame vào DB transform sử dụng SQLAlchemy ORM.

    - Tạo bảng nếu chưa tồn tại
    - Dùng `session.merge` để merge (insert/update) từng hàng
    """
    logger.info("Saving transformed data to transform DB...")

    # Tạo bảng nếu chưa tồn tại
    models.Base = database.Base  # ensure Base consistent (models imports Base from database)
    database.Base.metadata.create_all(engine)

    records = df.to_dict(orient="records")

    with database.session_scope(SessionLocal) as session:
        for rec in records:
            # Map DataFrame fields -> model fields (id, name, email)
            payload = {k: rec.get(k) for k in ("id", "name", "email")}
            # Dùng merge để insert/update theo primary key
            user = models.User(**payload)
            session.merge(user)

    logger.info("Saved %d rows to transform DB", len(records))


def main():
    # Lấy URL từ biến môi trường hoặc mặc định
    clean_url = os.getenv("CLEAN_DB_URL", database.DEFAULT_CLEAN_DB_URL)
    transform_url = os.getenv("TRANSFORM_DB_URL", database.DEFAULT_TRANSFORM_DB_URL)

    logger.info("CLEAN_DB_URL=%s", clean_url)
    logger.info("TRANSFORM_DB_URL=%s", transform_url)

    # Tạo engines
    try:
        clean_engine, CleanSession = database.create_engine_and_session(clean_url)
    except Exception as exc:  # pragma: no cover - connection errors
        logger.warning("Unable to create engine for CLEAN DB: %s", exc)
        clean_engine = None
        CleanSession = None

    try:
        transform_engine, TransformSession = database.create_engine_and_session(transform_url)
    except Exception as exc:  # pragma: no cover
        logger.error("Unable to create engine for TRANSFORM DB: %s", exc)
        raise

    # Load
    df = None
    if clean_engine is not None:
        df = load_users_from_db(clean_engine)

    # Nếu không load được từ DB, tạo dữ liệu mẫu để demo
    if df is None:
        logger.info("Using sample data for demo (no source DB)")
        df = pd.DataFrame(
            [
                {"id": 1, "name": "nguyen van a", "email": "a@example.com"},
                {"id": 2, "name": "tran thi b", "email": "b@domain.org"},
            ]
        )

    # Transform
    df_t = transform_users(df)

    # Save
    save_to_transform_db(df_t, transform_engine, TransformSession)


if __name__ == "__main__":
    main()
