from database.base import create_tables
from database.logger import log_email_status
from database.setup_db import setup_database
from database import BaseELT


def create_table(engine_elt):
    """
    1.0.3 (EXTRACT) - Tạo toàn bộ bảng metadata (log_extract_run, log_extract_event, ...)
    dùng chung cho các flow, dựa trên BaseELT.
    """
    import etl_metadata.models

    create_tables(engine_elt, BaseELT)


def connection_elt():
    """
    1.0.2 (EXTRACT) - Thiết lập kết nối tới DB metadata (db_elt_metadata):
        - Đọc URL cấu hình từ config.json thông qua setup_database("elt_db_url")
        - Kiểm tra kết nối (SELECT 1) và retry; nếu thất bại thì gửi email cảnh báo.
    """
    engine_elt, SessionELT = setup_database(
        "elt_db_url",
        # 3.4.2 Gửi mail thông báo.
        lambda: log_email_status(
            subject="LỖi hệ thống warehouse",
            content="Không thể kết nối với db db_elt_metadata",
        ),
        echo=False,
    )
    return engine_elt, SessionELT