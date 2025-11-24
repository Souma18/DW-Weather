import logging
import datetime
import os
from typing import Dict, List, Optional
from decimal import Decimal

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from sqlalchemy import create_engine, select, func, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError

# Import đúng theo cấu trúc của bạn
from service.email_service import send_email
from database.base import session_scope
from elt_metadata.models import LoadLog, MappingInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
ALERT_EMAIL = "caominhhieunq@gmail.com"


class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"

    STAGING_DB_URL = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_stage_transform"
    META_DB_URL = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_etl_metadata"

    EXACT_SCHEMA = {
        "dim_location": {"id", "station", "lat", "lon", "hp", "country", "gc", "createdAt"},
        "dim_cyclone": {"id", "name", "intensity", "start_time", "latest_time", "updatedAt"},
        "fact_heavy_rain": {"location_id", "event_datetime", "rainfall_mm", "createdAt"},
        "fact_thunderstorm": {"location_id", "event_datetime", "thunderstorm_index", "createdAt"},
        "fact_fog": {"location_id", "event_datetime", "fog_index", "visibility", "createdAt"},
        "fact_gale": {"location_id", "event_datetime", "knots", "ms", "degrees", "direction", "createdAt"},
        "fact_cyclone_track": {
            "cyclone_id", "event_datetime", "lat", "lon", "intensity", "pressure",
            "max_wind_speed", "gust", "speed_of_movement", "movement_direction",
            "wind_radis", "center_id", "createdAt",
        },
    }

    # Từ khóa để nhận diện trạng thái "SKIP" (không gửi mail)
    SKIP_KEYWORDS = [
        "rỗng", "không có dữ liệu mới", "bảng nguồn rỗng",
        "không có dữ liệu để load", "bỏ qua load", "no new data", "empty"
    ]

    def __init__(self):
        key_path = os.path.join(os.path.dirname(__file__), "bigquery-key.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy file: {key_path}")

        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)
        log.info("Khởi tạo BigQuery client thành công")

        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset '{self.DATASET_ID}' đã tồn tại")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Đã tạo dataset mới: {self.DATASET_ID}")

        self.staging_engine = create_engine(self.STAGING_DB_URL, echo=False, future=True)
        self.meta_engine = create_engine(self.META_DB_URL, echo=False, future=True)
        self.MetaSession = sessionmaker(bind=self.meta_engine)

        metadata = MetaData()
        try:
            metadata.reflect(bind=self.staging_engine)
            self.staging_metadata = metadata
            log.info(f"Reflect thành công {len(metadata.tables)} bảng từ staging DB")
        except Exception as e:
            log.error(f"Lỗi reflect metadata: {e}")
            self.staging_metadata = MetaData()

    def get_mappings(self) -> List[Dict]:
        with session_scope(self.MetaSession) as session:
            rows = session.query(MappingInfo).filter(MappingInfo.is_active.is_(True)).order_by(MappingInfo.load_order).all()
            return [r.__dict__ for r in rows] if rows else []

    def get_last_load_ts(self, source_table: str) -> Optional[datetime.datetime]:
        with session_scope(self.MetaSession) as session:
            ts = session.query(func.max(LoadLog.end_at))\
                        .filter(LoadLog.source_name == source_table, LoadLog.status == "SUCCESS")\
                        .scalar()
            if ts and ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    def fetch_data(self, table_name: str, ts_col: Optional[str], since: Optional[datetime.datetime]) -> List[Dict]:
        if table_name not in self.staging_metadata.tables:
            raise NoSuchTableError(f"Bảng '{table_name}' không tồn tại trong staging DB")

        table = self.staging_metadata.tables[table_name]
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
                    if v.tzinfo is None:
                        v = v.replace(tzinfo=datetime.timezone.utc)
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
        disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if load_type == "full" else bigquery.WriteDisposition.WRITE_APPEND

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
            return True
        except Exception as e:
            log.error(f"Load thất bại vào BigQuery table {target_table}: {e}")
            return False

    def check_exact_schema(self, table_name: str) -> Optional[str]:
        expected = self.EXACT_SCHEMA.get(table_name.lower())
        if not expected:
            return f"Bảng '{table_name}' không được phép load (không có trong danh sách cho phép)"

        if table_name not in self.staging_metadata.tables:
            return f"Bảng nguồn '{table_name}' không tồn tại trong staging DB"

        actual = set(self.staging_metadata.tables[table_name].columns.keys())
        if actual != expected:
            missing = ", ".join(sorted(expected - actual))
            extra = ", ".join(sorted(actual - expected))
            errors = []
            if missing:
                errors.append(f"thiếu cột: {missing}")
            if extra:
                errors.append(f"cột thừa: {extra}")
            return f"Schema không khớp cho bảng '{table_name}': {'; '.join(errors)}"
        return None

    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]
        start_time = datetime.datetime.now(datetime.timezone.utc)

        log.info(f"→ Bắt đầu xử lý: {src} → {dst} [{load_type}]")

        # 1. Kiểm tra schema
        if err := self.check_exact_schema(src):
            self.log_and_notify(src, dst, "FAILED", 0, start_time, err)
            return False

        # 2. Kiểm tra bảng rỗng
        with self.staging_engine.connect() as conn:
            row_count = conn.execute(select(func.count()).select_from(self.staging_metadata.tables[src])).scalar()
        if row_count == 0:
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, "Bảng nguồn rỗng → bỏ qua load")
            return True

        # 3. Lấy dữ liệu
        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None
        try:
            data = self.fetch_data(src, ts_col, since)
        except Exception as e:
            self.log_and_notify(src, dst, "FAILED", 0, start_time, f"Lỗi truy vấn dữ liệu từ bảng '{src}': {e}")
            return False

        if not data:
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, "Không có dữ liệu mới để load")
            return True

        # 4. Load lên BigQuery
        transformed = self.transform_rows(data)
        success = self.load_to_bq(transformed, dst, load_type)

        # 5. Ghi log kết quả cuối cùng (chỉ 1 lần)
        if success:
            self.log_and_notify(src, dst, "SUCCESS", len(data), start_time, f"Load thành công {len(data):,} bản ghi vào {dst}")
        else:
            self.log_and_notify(src, dst, "FAILED", len(data), start_time, f"Load thất bại vào BigQuery table '{dst}'")

        return success

    def log_and_notify(
        self,
        src: str,
        dst: str,
        status: str,
        count: int,
        start_at: datetime.datetime,
        message: str,
    ):
        end_at = datetime.datetime.now(datetime.timezone.utc)
        log_obj = LoadLog(
            status=status,
            record_count=count,
            source_name=src,
            table_name=dst,
            message=message,
            start_at=start_at,
            end_at=end_at,
        )

        # Ghi log vào DB
        with session_scope(self.MetaSession) as session:
            session.add(log_obj)

        # Kiểm tra có phải là trạng thái bỏ qua không
        is_skip = any(kw.lower() in message.lower() for kw in self.SKIP_KEYWORDS)

        # Gửi email CHỈ khi FAILED và không phải skip
        if status == "FAILED":
            log.error(f"FAILED | {dst} | {message}")
            send_email(
                to_email=ALERT_EMAIL,
                subject="LỖI LOAD DỮ LIỆU WEATHER",
                content="",
                error_message=message,
                table_name=dst,
                subject_prefix="CẢNH BÁO LỖI ETL"
            )
        elif is_skip:
            log.info(f"SKIP   | {dst} | {message}")
        else:
            log.info(f"SUCCESS| {dst} | Load {count:,} bản ghi thành công")

    def run(self):
        log.info("=== BẮT ĐẦU CHẠY ETL: STAGING → BIGQUERY ===")
        mappings = self.get_mappings()

        if not mappings:
            error_msg = "Không tìm thấy mapping nào có is_active = True"
            log.error(error_msg)
            with session_scope(self.MetaSession) as session:
                session.add(LoadLog(
                    status="FAILED",
                    message=error_msg,
                    start_at=datetime.datetime.now(datetime.timezone.utc),
                    end_at=datetime.datetime.now(datetime.timezone.utc),
                ))
            send_email(
                to_email=ALERT_EMAIL,
                subject="KHÔNG CÓ MAPPING HOẠT ĐỘNG",
                content="",
                error_message=error_msg,
                table_name="HỆ THỐNG",
                subject_prefix="FATAL ERROR"
            )
            return

        results = [self.process_table(m) for m in mappings]
        success_count = sum(results)
        failed_count = len(results) - success_count

        log.info(f"HOÀN TẤT TOÀN BỘ: {success_count}/{len(mappings)} bảng thành công")

        print("\n" + "=" * 70)
        print("          WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("=" * 70)
        print(f"  Tổng số bảng      : {len(mappings)}")
        print(f"  Thành công        : {success_count}")
        print(f"  Thất bại          : {failed_count}")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()