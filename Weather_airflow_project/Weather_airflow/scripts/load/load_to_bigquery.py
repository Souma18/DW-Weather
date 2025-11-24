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

# Import đúng theo cấu trúc dự án của bạn (dựa vào logger.py)
from service.email_service import send_email  # ĐÚNG RỒI ĐÂY!
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

    SKIP_KEYWORDS = [
        "rỗng", "không có dữ liệu mới", "bảng nguồn rỗng",
        "không có dữ liệu để load", "bỏ qua load", "no new data", "empty"
    ]

    def __init__(self):
        key_path = os.path.join(os.path.dirname(__file__), "bigquery-key.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy bigquery-key.json tại: {key_path}")

        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)
        log.info("Khởi tạo client BigQuery thành công")

        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset '{self.DATASET_ID}' đã tồn tại.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Đã tạo dataset mới: {self.DATASET_ID}")

        self.staging_engine = create_engine(self.STAGING_DB_URL, echo=False, future=True)
        self.meta_engine = create_engine(self.META_DB_URL, echo=False, future=True)

        self.StagingSession = sessionmaker(bind=self.staging_engine)
        self.MetaSession = sessionmaker(bind=self.meta_engine)

        metadata = MetaData()
        try:
            metadata.reflect(bind=self.staging_engine)
            self.staging_metadata = metadata
            log.info(f"Reflect thành công {len(metadata.tables)} bảng từ staging DB")
        except Exception as e:
            log.error(f"Lỗi reflect metadata staging: {e}")
            self.staging_metadata = MetaData()

    def get_mappings(self) -> List[Dict]:
        with session_scope(self.MetaSession) as session:
            rows = (
                session.query(MappingInfo)
                .filter(MappingInfo.is_active.is_(True))
                .order_by(MappingInfo.load_order)
                .all()
            )
            return [
                {
                    "source_table": r.source_table,
                    "target_table": r.target_table,
                    "timestamp_column": r.timestamp_column,
                    "load_type": r.load_type,
                }
                for r in rows
            ]

    def get_last_load_ts(self, source_table: str) -> Optional[datetime.datetime]:
        with session_scope(self.MetaSession) as session:
            ts = (
                session.query(func.max(LoadLog.end_at))
                .filter(LoadLog.source_name == source_table, LoadLog.status == "SUCCESS")
                .scalar()
            )
            if ts and ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    def fetch_data(self, table_name: str, ts_col: Optional[str], since: Optional[datetime.datetime]) -> List[Dict]:
        if table_name not in self.staging_metadata.tables:
            raise NoSuchTableError(f"Bảng nguồn '{table_name}' không tồn tại trong db_stage_transform")

        table = self.staging_metadata.tables[table_name]
        query = select(table)
        if ts_col and since:
            query = query.where(table.c[ts_col] > since).order_by(table.c[ts_col])

        with self.staging_engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row._mapping) for row in result.fetchall()]

    def transform_rows(self, rows: List[Dict]) -> List[Dict]:
        result = []
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
            result.append(new_row)
        return result

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
            parts = []
            if missing:
                parts.append(f"thiếu cột: {missing}")
            if extra:
                parts.append(f"cột thừa: {extra}")
            return f"Schema không khớp với yêu cầu cho bảng '{table_name}': {'; '.join(parts)}"
        return None

    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]
        start_time = datetime.datetime.now(datetime.timezone.utc)

        log.info(f"Bắt đầu xử lý: {src} → {dst} [{load_type}]")

        if schema_err := self.check_exact_schema(src):
            self.log_and_notify(src, dst, "FAILED", 0, start_time, schema_err)
            return False

        table = self.staging_metadata.tables[src]
        with self.staging_engine.connect() as conn:
            row_count = conn.execute(select(func.count()).select_from(table)).scalar()
            if row_count == 0:
                msg = "Bảng nguồn rỗng → bỏ qua load"
                self.log_and_notify(src, dst, "SUCCESS", 0, start_time, msg)
                return True

        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None

        try:
            data = self.fetch_data(src, ts_col, since)
        except Exception as e:
            error_msg = f"Lỗi truy vấn dữ liệu từ bảng nguồn '{src}': {str(e)}"
            self.log_and_notify(src, dst, "FAILED", 0, start_time, error_msg)
            return False

        if not data:
            msg = "Không có dữ liệu mới để load"
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, msg)
            return True

        transformed = self.transform_rows(data)
        success = self.load_to_bq(transformed, dst, load_type)

        if success:
            msg = f"Load thành công {len(data):,} bản ghi vào {dst}"
            self.log_and_notify(src, dst, "SUCCESS", len(data), start_time, msg)
        else:
            msg = f"Load thất bại vào BigQuery table '{dst}'. Vui lòng kiểm tra log và dữ liệu nguồn."
            self.log_and_notify(src, dst, "FAILED", len(data), start_time, msg)

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

        # Ghi log vào DB (luôn luôn)
        with session_scope(self.MetaSession) as session:
            session.add(log_obj)

        # Kiểm tra trạng thái SKIP
        is_skip = any(keyword.lower() in message.lower() for keyword in self.SKIP_KEYWORDS)

        if status == "FAILED":
            log.error(f"FAILED | {dst} | {message}")
            send_email(
                to_email=ALERT_EMAIL,
                subject="LỖI LOAD DỮ LIỆU WEATHER",
                content="",
                error_message=message,           # ĐÃ SỬA: BẮT BUỘC PHẢI CÓ
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
            error_msg = "Không tìm thấy mapping nào có is_active = True trong bảng MappingInfo"
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
                error_message=error_msg,        # ĐÃ SỬA: THÊM DÒNG NÀY
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