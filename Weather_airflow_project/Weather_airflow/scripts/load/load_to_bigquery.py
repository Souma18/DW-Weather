import logging
import datetime
import os
from typing import Dict, Optional, List
from decimal import Decimal

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from database.base import create_engine_and_session, session_scope
from database.logger import log_dual_status
from models.log_model import LoadLog  # bạn sẽ cần tạo model này (xem cuối cùng)

# ============================= CONFIG =============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)

# Email nhận thông báo (có thể đưa ra env var sau)
ALERT_EMAIL = "your_email@gmail.com"  # thay bằng email thật

class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"

    # DATABASE URLs - nên đưa ra env var, tạm hardcode để test
    STAGING_DB_URL = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_stage_transform"
    META_DB_URL    = "mysql+pymysql://etl_user:etl_password@localhost:3306/db_etl_metadata"

    # SCHEMA PHẢI KHỚP CHÍNH XÁC 100%
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
            raise FileNotFoundError(f"Không tìm thấy file key: {key_path}")

        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)

        # Tạo dataset nếu chưa có
        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset {self.DATASET_ID} đã tồn tại")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Tạo dataset {self.DATASET_ID} thành công")

        # Khởi tạo SQLAlchemy engine + session (có retry tự động)
        _, self.StagingSession = create_engine_and_session(self.STAGING_DB_URL, log, echo=False)
        _, self.MetaSession     = create_engine_and_session(self.META_DB_URL,    log, echo=False)

    def get_mappings(self) -> List[Dict]:
        with session_scope(self.MetaSession) as session:
            return session.query("SELECT * FROM mapping_info WHERE is_active = 1 ORDER BY load_order").all()

    def get_last_load_ts(self, table_name: str) -> Optional[datetime.datetime]:
        with session_scope(self.MetaSession) as session:
            result = session.execute(
                """
                SELECT MAX(end_at) AS last_end
                FROM load_log
                WHERE table_name = :table AND status = 'SUCCESS'
                """,
                {"table": table_name}
            ).fetchone()
            ts = result.last_end if result and result.last_end else None
            if ts and ts.tzinfo is None:
                return ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    def fetch_data(self, table: str, ts_col: Optional[str], since: Optional[datetime.datetime]):
        query = f"SELECT * FROM `{table}`"
        params = {}
        if ts_col and since:
            query += f" WHERE `{ts_col}` > :since ORDER BY `{ts_col}`"
            params["since"] = since

        with session_scope(self.StagingSession) as session:
            rows = session.execute(query, params).fetchall()
            return [row._asdict() for row in rows]  # chuyển Row → dict

    def transform_rows(self, rows: list) -> list:
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

    def load_to_bq(self, rows: list, target_table: str, load_type: str) -> bool:
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
            log.error(f"LOAD THẤT BẠI {target_table}: {e}")
            return False

    def check_exact_schema(self, table: str, actual_columns: set) -> Optional[str]:
        expected = self.EXACT_SCHEMA.get(table.lower())
        if not expected:
            return f"Bảng '{table}' không có trong EXACT_SCHEMA"
        if actual_columns != expected:
            missing = expected - actual_columns
            extra = actual_columns - expected
            errs = []
            if missing: errs.append(f"thiếu: {', '.join(sorted(missing))}")
            if extra:   errs.append(f"dư: {', '.join(sorted(extra))}")
            return f"Schema không khớp '{table}': {', '.join(errs)}"
        return None

    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]

        log.info(f"XỬ LÝ: {src} → {dst} [{load_type}] (ts_col={ts_col or 'FULL'})")
        start_time = datetime.datetime.now(datetime.timezone.utc)

        try:
            # 1. Kiểm tra bảng có tồn tại + lấy cột thực tế
            with session_scope(self.StagingSession) as session:
                sample = session.execute(f"SELECT * FROM `{src}` LIMIT 1").fetchone()
                if not sample:
                    raise Exception("Bảng rỗng hoặc không tồn tại")
                actual_columns = set(sample.keys())
        except Exception as e:
            self._log_and_notify(src, dst, "FAILED", 0, start_time, str(e))
            return False

        # 2. Kiểm tra schema chính xác tuyệt đối
        schema_err = self.check_exact_schema(src, actual_columns)
        if schema_err:
            self._log_and_notify(src, dst, "FAILED", 0, start_time, schema_err)
            return False

        # 3. Lấy dữ liệu
        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None
        raw_data = self.fetch_data(src, ts_col, since)

        record_count = len(raw_data)
        if record_count == 0:
            self._log_and_notify(src, dst, "SUCCESS", 0, start_time,
                                 "Không có dữ liệu mới → bỏ qua")
            return True

        # 4. Transform + Load
        transformed = self.transform_rows(raw_data)
        success = self.load_to_bq(transformed, dst, load_type)

        end_time = datetime.datetime.now(datetime.timezone.utc)
        status = "SUCCESS" if success else "FAILED"
        message = f"Load thành công {record_count:,} bản ghi" if success else "Load lên BigQuery thất bại"

        self._log_and_notify(src, dst, status, record_count, start_time, end_time, message)
        return success

    def _log_and_notify(
        self,
        source_table: str,
        target_table: str,
        status: str,
        record_count: int,
        start_at: datetime.datetime,
        end_at_or_message: datetime.datetime | str,
        message: str = None
    ):
        if isinstance(end_at_or_message, datetime.datetime):
            end_at = end_at_or_message
        else:
            end_at = datetime.datetime.now(datetime.timezone.utc)
            message = end_at_or_message or "Lỗi không xác định"

        # Tạo object log
        log_obj = LoadLog(
            status=status,
            record_count=record_count,
            source_name=source_table,
            table_name=target_table,
            message=message,
            start_at=start_at,
            end_at=end_at
        )

        # Nội dung email
        email_content = f"""
        [WEATHER ETL] {status} - {target_table}
        Thời gian: {start_at.strftime('%Y-%m-%d %H:%M:%S')} → {end_at.strftime('%Y-%m-%d %H:%M:%S')}
        Số bản ghi: {record_count:,}
        Message: {message}
        """

        # Ghi log DB + gửi email (dù DB lỗi vẫn cố gửi mail)
        log_dual_status(
            log_obj=log_obj,
            SessionLocal=self.MetaSession,
            to_email=ALERT_EMAIL,
            subject=f"[Weather ETL] {status} - {target_table}",
            content=email_content.strip()
        )

    def run(self):
        log.info("=== BẮT ĐẦU LOAD WEATHER → BIGQUERY ===")
        mappings = self.get_mappings()

        if not mappings:
            error_msg = "KHÔNG TÌM THẤY MAPPING trong bảng mapping_info!"
            log.error(error_msg)
            # Vẫn gửi mail dù không có mapping
            log_dual_status(
                log_obj=LoadLog(status="FAILED", message=error_msg),
                SessionLocal=self.MetaSession,
                to_email=ALERT_EMAIL,
                subject="[Weather ETL] CRITICAL - Không có mapping",
                content=error_msg
            )
            return

        success_count = sum(1 for m in mappings if self.process_table(m))

        summary = f"""
        HOÀN TẤT LOAD WEATHER → BIGQUERY
        Tổng bảng: {len(mappings)}
        Thành công: {success_count}
        Thất bại: {len(mappings) - success_count}
        """
        log.info(summary)

        print("\n" + "="*70)
        print(" WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("="*70)
        print(summary.strip())
        print("="*70)


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()