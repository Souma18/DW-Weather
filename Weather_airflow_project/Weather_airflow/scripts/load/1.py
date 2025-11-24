import logging
import datetime
import os
from typing import Dict, Optional, List
from decimal import Decimal

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from database.base import create_engine_and_session, session_scope
from database.logger import log_dual_status
from scripts.elt_metadata.models import LoadLog

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

ALERT_EMAIL = "caominhhieunnq@gmail.com"


class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"

    STAGING_DB_URL = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_stage_transform"
    META_DB_URL    = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_etl_metadata"

    # SCHEMA CHÍNH XÁC 100% – PHẢI KHỚP HOÀN TOÀN VỚI BẢNG THỰC TẾ
    EXACT_SCHEMA = {
        'dim_location': {'id', 'station', 'lat', 'lon', 'hp', 'country', 'gc', 'createdAt'},
        'dim_cyclone': {'id', 'name', 'intensity', 'start_time', 'latest_time', 'updatedAt'},
        'fact_heavy_rain': {'location_id', 'event_datetime', 'rainfall_mm', 'createdAt'},
        'fact_thunderstorm': {'location_id', 'event_datetime', 'thunderstorm_index', 'createdAt'},
        'fact_fog': {'location_id', 'event_datetime', 'fog_index', 'visibility', 'createdAt'},
        'fact_gale': {'location_id', 'event_datetime', 'knots', 'ms', 'degrees', 'direction', 'createdAt'},
        'fact_cyclone_track': {
            'cyclone_id', 'event_datetime', 'lat', 'lon', 'intensity', 'pressure',
            'max_wind_speed', 'gust', 'speed_of_movement', 'movement_direction',
            'wind_radis', 'center_id', 'createdAt'
        },
    }

    def __init__(self):
        # BigQuery client
        current_dir = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(current_dir, "bigquery-key.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy bigquery-key.json tại: {key_path}")

        log.info("Đang kết nối BigQuery...")
        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)

        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset '{self.DATASET_ID}' đã tồn tại.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Tự động tạo dataset '{self.DATASET_ID}' thành công!")

        # SQLAlchemy sessions
        _, self.StagingSession = create_engine_and_session(self.STAGING_DB_URL, log, echo=False)
        _, self.MetaSession    = create_engine_and_session(self.META_DB_URL, log, echo=False)

    # ==================================================================
    # LẤY DANH SÁCH MAPPING
    # ==================================================================
    def get_mappings(self) -> List[Dict]:
        query = text("""
            SELECT * FROM mapping_info 
            WHERE is_active = 1 
            ORDER BY load_order
        """)
        with session_scope(self.MetaSession) as session:
            result = session.execute(query)
            return [row._asdict() for row in result.fetchall()]

    # ==================================================================
    # LẤY THỜI GIAN LOAD CUỐI CÙNG
    # ==================================================================
    def get_last_load_ts(self, table_name: str) -> Optional[datetime.datetime]:
        query = text("""
            SELECT MAX(end_at) AS last_end
            FROM load_log
            WHERE table_name = :table_name AND status = 'SUCCESS'
        """)
        with session_scope(self.MetaSession) as session:
            result = session.execute(query, {"table_name": table_name})
            row = result.fetchone()
            ts = row.last_end if row and row.last_end else None
            if ts and ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    # ==================================================================
    # LẤY DỮ LIỆU TỪ STAGING
    # ==================================================================
    def fetch_data(self, table: str, ts_col: Optional[str], since: Optional[datetime.datetime]) -> List[Dict]:
        sql = f"SELECT * FROM `{table}`"
        params = {}

        if ts_col and since:
            sql += f" WHERE `{ts_col}` > :since ORDER BY `{ts_col}`"
            params["since"] = since

        with session_scope(self.StagingSession) as session:
            result = session.execute(text(sql), params)
            return [row._asdict() for row in result.fetchall()]

    # ==================================================================
    # CHUYỂN ĐỔI DỮ LIỆU CHO BIGQUERY
    # ==================================================================
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

    # ==================================================================
    # LOAD VÀO BIGQUERY
    # ==================================================================
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
            log.info(f"ĐÃ LOAD {len(rows):,} dòng → {target_table} ({load_type})")
            return True
        except Exception as e:
            log.error(f"LỖI LOAD {target_table}: {e}")
            return False

    # ==================================================================
    # KIỂM TRA SCHEMA CHÍNH XÁC TUYỆT ĐỐI
    # ==================================================================
    def check_exact_schema(self, table: str, actual_columns: set) -> Optional[str]:
        expected = self.EXACT_SCHEMA.get(table.lower())
        if not expected:
            defined = ', '.join(sorted(self.EXACT_SCHEMA.keys()))
            return f"LỖI CẤU HÌNH: Bảng '{table}' không tồn tại trong EXACT_SCHEMA. Chỉ chấp nhận: {defined}"

        if actual_columns != expected:
            missing = expected - actual_columns
            extra = actual_columns - expected
            errors = []
            if missing:
                errors.append(f"thiếu cột: {', '.join(sorted(missing))}")
            if extra:
                errors.append(f"dư cột: {', '.join(sorted(extra))}")
            return f"Schema không khớp bảng '{table}': {'; '.join(errors)}"
        return None

    # ==================================================================
    # GHI LOG + GỬI EMAIL CẢNH BÁO
    # ==================================================================
    def log_and_notify(self, src: str, dst: str, status: str, count: int,
                       start_at: datetime.datetime, message: str):
        end_at = datetime.datetime.now(datetime.timezone.utc)

        log_obj = LoadLog(
            status=status,
            record_count=count,
            source_name=src,
            table_name=dst,
            message=message,
            start_at=start_at,
            end_at=end_at
        )

        log_dual_status(
            log_obj=log_obj,
            SessionLocal=self.MetaSession,
            to_email=ALERT_EMAIL,
            subject=f"[Weather ETL] {status} - {dst}",
            content=message
        )

    # ==================================================================
    # XỬ LÝ MỖI BẢNG
    # ==================================================================
    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]

        start_time = datetime.datetime.now(datetime.timezone.utc)
        log.info(f"Đang xử lý: {src} → {dst} [{load_type}] (ts_col={ts_col or 'FULL'})")

        # 1. Kiểm tra bảng nguồn + lấy schema thực tế
        try:
            with session_scope(self.StagingSession) as session:
                result = session.execute(text(f"SELECT * FROM `{src}` LIMIT 1"))
                first_row = result.fetchone()
                if not first_row:
                    log.info(f"Bảng {src} rỗng → bỏ qua")
                    self.log_and_notify(src, dst, "SUCCESS", 0, start_time,
                                        "Bảng rỗng hoặc không có dữ liệu mới → bỏ qua")
                    return True
                actual_columns = set(first_row._mapping.keys())
        except Exception as e:
            error_msg = str(e)
            if "no such table" in error_msg.lower() or "doesn't exist" in error_msg.lower():
                error_msg = f"Bảng nguồn '{src}' không tồn tại!"
            log.error(f"LỖI TRUY VẤN: {error_msg}")
            self.log_and_notify(src, dst, "FAILED", 0, start_time, error_msg)
            return False

        # 2. Kiểm tra schema chính xác
        schema_error = self.check_exact_schema(src, actual_columns)
        if schema_error:
            log.error(f"SCHEMA ERROR: {schema_error}")
            self.log_and_notify(src, dst, "FAILED", 0, start_time, schema_error)
            return False

        # 3. Lấy dữ liệu
        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None
        raw_data = self.fetch_data(src, ts_col, since)

        if not raw_data:
            log.info(f"Không có dữ liệu mới cho {src}")
            self.log_and_notify(src, dst, "SUCCESS", 0, start_time, "Không có dữ liệu mới")
            return True

        # 4. Transform + Load
        transformed_data = self.transform_rows(raw_data)
        success = self.load_to_bq(transformed_data, dst, load_type)

        end_time = datetime.datetime.now(datetime.timezone.utc)
        if success:
            msg = f"Load thành công {len(transformed_data):,} bản ghi"
            self.log_and_notify(src, dst, "SUCCESS", len(transformed_data), start_time, msg)
        else:
            self.log_and_notify(src, dst, "FAILED", 0, start_time, "Load lên BigQuery thất bại")

        return success

    # ==================================================================
    # CHẠY TOÀN BỘ
    # ==================================================================
    def run(self):
        log.info("=== BẮT ĐẦU LOAD WEATHER → BIGQUERY ===")
        mappings = self.get_mappings()

        if not mappings:
            msg = "KHÔNG TÌM THẤY MAPPING! Kiểm tra bảng mapping_info (is_active = 1)"
            log.error(msg)
            log_dual_status(
                log_obj=LoadLog(status="FAILED", message=msg),
                SessionLocal=self.MetaSession,
                to_email=ALERT_EMAIL,
                subject="[Weather ETL] Không có mapping",
                content=msg
            )
            return

        success_count = sum(1 for m in mappings if self.process_table(m))

        log.info(f"HOÀN TẤT: {success_count}/{len(mappings)} bảng thành công")

        print("\n" + "="*70)
        print("     WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("="*70)
        print(f"   Tổng số bảng     : {len(mappings)}")
        print(f"   Thành công       : {success_count}")
        print(f"   Thất bại         : {len(mappings) - success_count}")
        print("="*70 + "\n")


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()