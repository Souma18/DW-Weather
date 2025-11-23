import logging
import datetime
import os
from typing import Dict, Optional
from decimal import Decimal

import pymysql
from pymysql.cursors import DictCursor
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account


# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(__name__)


class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"
    STAGING_DB = "db_stage_transform"
    META_DB = "db_etl_metadata"

    # SCHEMA CHÍNH XÁC 100% – PHẢI KHỚP HOÀN TOÀN VỚI CẤU TRÚC BẢNG THỰC TẾ
    EXACT_SCHEMA = {
        # Dim
        'dim_location': {
            'id', 'station', 'lat', 'lon', 'hp', 'country', 'gc', 'createdAt'
        },
        'dim_cyclone': {
            'id', 'name', 'intensity', 'start_time', 'latest_time', 'updatedAt'
        },
        # Fact
        'fact_heavy_rain': {
            'location_id', 'event_datetime', 'rainfall_mm', 'createdAt'
        },
        'fact_thunderstorm': {
            'location_id', 'event_datetime', 'thunderstorm_index', 'createdAt'
        },
        'fact_fog': {
            'location_id', 'event_datetime', 'fog_index', 'visibility', 'createdAt'
        },
        'fact_gale': {
            'location_id', 'event_datetime', 'knots', 'ms', 'degrees', 'direction', 'createdAt'
        },
        'fact_cyclone_track': {
            'cyclone_id', 'event_datetime', 'lat', 'lon', 'intensity', 'pressure',
            'max_wind_speed', 'gust', 'speed_of_movement', 'movement_direction',
            'wind_radis', 'center_id', 'createdAt'
        },
    }

    def __init__(self):
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

        self.mysql_cfg = {
            "host": "localhost",
            "port": 3306,
            "user": "etl_user",
            "password": "etl_password",
        }

    def mysql_conn(self, db_name: str):
        cfg = self.mysql_cfg.copy()
        cfg["database"] = db_name
        cfg["cursorclass"] = DictCursor
        cfg["autocommit"] = True
        return pymysql.connect(**cfg)

    def get_mappings(self):
        with self.mysql_conn(self.META_DB) as conn, conn.cursor() as cur:
            cur.execute("SELECT * FROM mapping_info WHERE is_active = 1 ORDER BY load_order")
            return cur.fetchall()

    def get_last_load_ts(self, table_name: str):
        with self.mysql_conn(self.META_DB) as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(end_at) AS last_end 
                FROM load_log 
                WHERE table_name = %s AND status = 'SUCCESS'
            """, (table_name,))
            row = cur.fetchone()
            ts = row.get("last_end") if row else None
            if ts and ts.tzinfo is None:
                return ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    def fetch_data(self, table: str, ts_col: Optional[str], since: Optional[datetime.datetime]):
        sql = f"SELECT * FROM `{self.STAGING_DB}`.`{table}`"
        params = []
        if ts_col and since:
            sql += f" WHERE `{ts_col}` > %s ORDER BY `{ts_col}`"
            params = [since.strftime("%Y-%m-%d %H:%M:%S")]
        with self.mysql_conn(self.STAGING_DB) as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

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
            log.info(f"ĐÃ LOAD {len(rows):,} dòng → {target_table} ({load_type})")
            return True
        except Exception as e:
            log.error(f"LỖI LOAD {target_table}: {e}")
            return False

    def check_exact_schema(self, table: str, actual_columns: set) -> Optional[str]:
        expected = self.EXACT_SCHEMA.get(table.lower())  # không phân biệt hoa thường

        if not expected:
            defined_tables = ', '.join(sorted(self.EXACT_SCHEMA.keys()))
            return (f"LỖI CẤU HÌNH: Bảng nguồn '{table}' không tồn tại hoặc sai chính tả trong EXACT_SCHEMA. "
                    f"Chỉ chấp nhận: {defined_tables}")

        if actual_columns != expected:
            missing = expected - actual_columns
            extra = actual_columns - expected
            errors = []
            if missing:
                errors.append(f"thiếu cột: {', '.join(sorted(missing))}")
            if extra:
                errors.append(f"dư cột: {', '.join(sorted(extra))}")
            return f"Schema không khớp bảng '{table}': {', '.join(errors)}"
        return None

    def log_run(self, source_table: str, target_table: str, status: str, record_count: int,
                start_at: datetime.datetime, end_at: datetime.datetime, message: str = None):
        if not message:
            if record_count == 0 and status == "SUCCESS":
                message = "Bảng rỗng hoặc không có dữ liệu mới → bỏ qua"
            elif status == "SUCCESS":
                message = f"Load thành công {record_count:,} bản ghi"
            else:
                message = "Load thất bại"

        try:
            with self.mysql_conn(self.META_DB) as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO load_log
                    (status, record_count, source_name, table_name, message, start_at, end_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    status, record_count, source_table, target_table,
                    message, start_at, end_at
                ))
            log.info(f"LOG [{status}]: {message}")
        except Exception as e:
            log.warning(f"Không ghi log được: {e}")

    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]           # tên bảng trong MySQL (ví dụ: fact_heavy_rain)
        dst = mapping["target_table"]           # tên bảng trong BigQuery (có thể giống hoặc khác)
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]

        log.info(f"Đang xử lý: {src} → {dst} [{load_type}] (ts_col={ts_col or 'FULL'})")
        start_time = datetime.datetime.now(datetime.timezone.utc)

        # 1. Lấy cột thực tế từ bảng nguồn
        try:
            with self.mysql_conn(self.STAGING_DB) as conn, conn.cursor() as cur:
                cur.execute(f"SELECT * FROM `{src}` LIMIT 1")
                first_row = cur.fetchone()

                if not first_row:
                    log.info(f"Bảng {src} rỗng → bỏ qua")
                    self.log_run(src, dst, "SUCCESS", 0, start_time,
                                 datetime.datetime.now(datetime.timezone.utc))
                    return True

                actual_columns = set(first_row.keys())

        except Exception as e:
            end_time = datetime.datetime.now(datetime.timezone.utc)
            error_msg = str(e)
            if "doesn't exist" in error_msg.lower():
                error_msg = f"Bảng nguồn '{src}' không tồn tại trong {self.STAGING_DB}"
            log.error(f"LỖI TRUY VẤN: {error_msg}")
            self.log_run(src, dst, "FAILED", 0, start_time, end_time, error_msg)
            return False

        # 2. Kiểm tra schema chính xác tuyệt đối
        schema_error = self.check_exact_schema(src, actual_columns)
        if schema_error:
            end_time = datetime.datetime.now(datetime.timezone.utc)
            log.error(f"SCHEMA ERROR: {schema_error}")
            self.log_run(src, dst, "FAILED", 0, start_time, end_time, schema_error)
            return False

        # 3. Lấy dữ liệu (incremental hoặc full)
        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None
        raw_data = self.fetch_data(src, ts_col, since)

        if not raw_data:
            end_time = datetime.datetime.now(datetime.timezone.utc)
            log.info(f"Không có dữ liệu mới cho {src}")
            self.log_run(src, dst, "SUCCESS", 0, start_time, end_time)
            return True

        # 4. Transform + Load
        data = self.transform_rows(raw_data)
        success = self.load_to_bq(data, dst, load_type)

        end_time = datetime.datetime.now(datetime.timezone.utc)
        if success:
            self.log_run(src, dst, "SUCCESS", len(data), start_time, end_time)
        else:
            self.log_run(src, dst, "FAILED", 0, start_time, end_time, "Load lên BigQuery thất bại")

        return success

    def run(self):
        log.info("=== BẮT ĐẦU LOAD WEATHER → BIGQUERY ===")
        mappings = self.get_mappings()
        if not mappings:
            log.error("KHÔNG TÌM THẤY MAPPING! Hãy kiểm tra bảng mapping_info trong db_etl_metadata")
            return

        success_count = 0
        for m in mappings:
            if self.process_table(m):
                success_count += 1

        log.info(f"HOÀN TẤT: {success_count}/{len(mappings)} bảng thành công")

        print("\n" + "="*70)
        print("           WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("="*70)
        print(f"   Tổng số bảng     : {len(mappings)}")
        print(f"   Thành công       : {success_count}")
        print(f"   Thất bại         : {len(mappings) - success_count}")
        print("="*70)


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()