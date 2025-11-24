import logging
import datetime
import os
from typing import Dict, List, Optional
from decimal import Decimal
from contextlib import contextmanager

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from sqlalchemy import select, func, MetaData
from sqlalchemy.exc import NoSuchTableError

from service.email_service import send_email
from etl_metadata.models import LoadLog, TransformLog
from database.setup_db import engine_transform, SessionELT, DEFAULT_RECIEVER_EMAIL


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
ALERT_EMAIL = DEFAULT_RECIEVER_EMAIL or "caominhhieunq@gmail.com"



@contextmanager
def session_scope(Session):
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"

    # Danh sách các bảng được phép load (chỉ kiểm tên bảng, không kiểm schema cứng)
    ALLOWED_SOURCE_TABLES = {
        "dim_location",
        "dim_cyclone",
        "fact_heavy_rain_snow",
        "fact_thunderstorm",
        "fact_fog",
        "fact_gale",
        "fact_cyclone_track",
    }

    SKIP_KEYWORDS = [
        "rỗng", "không có dữ liệu mới", "bảng nguồn rỗng",
        "không có dữ liệu để load", "bỏ qua load", "no new data", "empty"
    ]

    def __init__(self) -> None:
        # BigQuery client
        key_path = os.path.join(os.path.dirname(__file__), "bigquery-key.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy file: {key_path}")

        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)
        log.info("Khởi tạo BigQuery client thành công")

        # Đảm bảo dataset tồn tại
        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset '{self.DATASET_ID}' đã tồn tại")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Đã tạo dataset mới: {self.DATASET_ID}")

        # Reflect database staging
        self.staging_engine = engine_transform
        self.MetaSession = SessionELT
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.staging_engine)
        log.info(f"Reflect thành công {len(self.metadata.tables)} bảng từ staging DB")

    def get_mappings(self) -> List[Dict]:
        with session_scope(self.MetaSession) as session:
            rows = (
                session.query(LoadLog)
                .filter(LoadLog.is_active.is_(True))
                .order_by(LoadLog.load_order)
                .all()
            )
            return [
                {
                    "id": r.id,
                    "source_table": r.source_table,
                    "target_table": r.target_table,
                    "timestamp_column": r.timestamp_column,
                    "load_type": r.load_type,
                }
                for r in rows
            ]

    def get_last_load_ts(self, mapping_id: int) -> Optional[datetime.datetime]:
        with session_scope(self.MetaSession) as session:
            row = session.get(LoadLog, mapping_id)
            return row.last_loaded_timestamp if row else None

    def update_last_load_ts(self, mapping_id: int, new_ts: Optional[datetime.datetime]) -> None:
        if new_ts is None:
            return
        if new_ts.tzinfo is not None:
            new_ts = new_ts.replace(tzinfo=None)

        with session_scope(self.MetaSession) as session:
            row = session.get(LoadLog, mapping_id)
            if row:
                row.last_loaded_timestamp = new_ts

    def fetch_data(self, table_name: str, ts_col: Optional[str], since: Optional[datetime.datetime]) -> List[Dict]:
        if table_name not in self.metadata.tables:
            raise NoSuchTableError(f"Bảng '{table_name}' không tồn tại trong staging DB")

        table = self.metadata.tables[table_name]
        query = select(table)
        if ts_col and since:
            query = query.where(table.c[ts_col] > since).order_by(table.c[ts_col])

        with self.staging_engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row._mapping) for row in result.fetchall()]

    def transform_rows(self, rows: List[Dict]) -> List[Dict]:
        transformed = []
        for row in rows:
            new_row = {}
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    new_row[k] = v.isoformat(timespec="seconds")
                elif isinstance(v, Decimal):
                    new_row[k] = float(v)
                else:
                    new_row[k] = v
            transformed.append(new_row)
        return transformed

    def load_to_bq(self, rows: List[Dict], target_table: str, load_type: str) -> bool:
        if not rows:
            return True

        table_id = f"{self.PROJECT_ID}.{self.DATASET_ID}.{target_table}"
        disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if load_type == "full"
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=disposition,
            autodetect=True,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        if load_type == "incremental":
            config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        try:
            job = self.bq.load_table_from_json(rows, table_id, job_config=config)
            job.result()
            log.info(f"Loaded {len(rows):,} rows → {table_id}")
            return True
        except Exception as e:
            log.error(f"Load thất bại vào {target_table}: {e}")
            return False

    def is_table_allowed(self, table_name: str) -> bool:
        return table_name in self.ALLOWED_SOURCE_TABLES

    def process_table(self, mapping: Dict) -> bool:
        mapping_id = mapping["id"]
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]
        start_time = datetime.datetime.now()

        log.info(f"→ Bắt đầu xử lý: {src} → {dst} [{load_type}]")

        # 1. Kiểm tra bảng có được phép load không
        if not self.is_table_allowed(src):
            msg = f"Bảng '{src}' không nằm trong danh sách được phép load"
            self.log_and_notify(src, dst, "FAILED", 0, start_time, msg)
            return False

        # 2. Kiểm tra bảng có tồn tại trong DB không
        if src not in self.metadata.tables:
            msg = f"Bảng nguồn '{src}' không tồn tại trong staging DB"
            self.log_and_notify(src, dst, "FAILED", 0, start_time, msg)
            return False

        # 3. Kiểm tra bảng rỗng
        table = self.metadata.tables[src]
        with self.staging_engine.connect() as conn:
            row_count = conn.execute(select(func.count()).select_from(table)).scalar()

        if row_count == 0:
            msg = "Bảng nguồn rỗng → bỏ qua load"
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, msg)
            return True

        # 4. Lấy dữ liệu
        since = self.get_last_load_ts(mapping_id) if load_type == "incremental" and ts_col else None
        try:
            data = self.fetch_data(src, ts_col, since)
        except Exception as e:
            msg = f"Lỗi truy vấn dữ liệu: {e}"
            self.log_and_notify(src, dst, "FAILED", 0, start_time, msg)
            return False

        if not data:
            msg = "Không có dữ liệu mới để load"
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, msg)
            return True

        # 5. Transform + Load
        transformed = self.transform_rows(data)
        success = self.load_to_bq(transformed, dst, load_type)

        # 6. Log + cập nhật timestamp
        if success:
            msg = f"Load thành công {len(data):,} bản ghi"
            self.log_and_notify(src, dst, "SUCCESS", len(data), start_time, msg)

            if load_type == "incremental" and ts_col and data:
                max_ts = max((row.get(ts_col) for row in data if row.get(ts_col)), default=None)
                self.update_last_load_ts(mapping_id, max_ts)
        else:
            msg = "Load thất bại vào BigQuery"
            self.log_and_notify(src, dst, "FAILED", len(data), start_time, msg)

        return success

    def log_and_notify(self, src: str, dst: str, status: str, count: int, start_at: datetime.datetime, message: str):
        end_at = datetime.datetime.now()
        log_obj = TransformLog(
            status=status,
            record_count=count,
            source_name=src,
            table_name=dst,
            message=message,
            start_at=start_at,
            end_at=end_at,
        )
        with session_scope(self.MetaSession) as session:
            session.add(log_obj)

        is_skip = any(kw.lower() in message.lower() for kw in self.SKIP_KEYWORDS)

        if status == "FAILED":
            log.error(f"FAILED | {dst} | {message}")
            try:
                send_email(
                    to_email=ALERT_EMAIL,
                    subject="LỖI LOAD DỮ LIỆU WEATHER",
                    error_message=message,
                    table_name=dst,
                    subject_prefix="CẢNH BÁO",
                )
            except Exception as e:
                log.error(f"Gửi mail lỗi thất bại: {e}")
        elif is_skip:
            log.info(f"SKIP   | {dst} | {message}")
        else:
            log.info(f"SUCCESS| {dst} | {message}")

    def run(self) -> None:
        log.info("=== BẮT ĐẦU ETL: STAGING → BIGQUERY ===")
        mappings = self.get_mappings()

        if not mappings:
            msg = "Không có mapping nào active trong load_log"
            log.error(msg)
            send_email(to_email=ALERT_EMAIL, subject="FATAL: Không có mapping", error_message=msg, table_name="HỆ THỐNG")
            return

        results = [self.process_table(m) for m in mappings]
        success = sum(results)
        failed = len(results) - success

        log.info(f"HOÀN TẤT: {success}/{len(mappings)} bảng thành công")

        print("\n" + "="*70)
        print("    WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("="*70)
        print(f"  Tổng bảng     : {len(mappings)}")
        print(f"  Thành công    : {success}")
        print(f"  Thất bại      : {failed}")
        print("="*70 + "\n")


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()