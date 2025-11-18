import logging
import datetime
import os
from typing import Dict, List, Optional
from decimal import Decimal

import pymysql
from pymysql.cursors import DictCursor
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

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
            log.info(f"Đã tự động tạo dataset '{self.DATASET_ID}' thành công!")

        self.mysql_cfg = {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "1",
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
            cur.execute("SELECT MAX(load_timestamp) AS ts FROM load_log WHERE table_name = %s AND status = 'SUCCESS'", (table_name,))
            row = cur.fetchone()
            ts = row.get("ts") if row else None
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

    def transform_rows(self, rows: List[Dict]) -> List[Dict]:
        result = []
        for row in rows:
            new_row = {}
            for k, v in row.items():
                if isinstance(v, datetime.datetime):
                    if v.tzinfo is None:
                        v = v.replace(tzinfo=datetime.timezone.utc)
                    new_row[k] = v.isoformat(timespec="seconds")
                elif isinstance(v, Decimal):  # ✅ SỬA LỖI DECIMAL
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
        
        # ✅ CHỈ DÙNG schema_update_options KHI APPEND
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

    def log_run(self, table: str, status: str, rows: int, error: str = None):
        try:
            with self.mysql_conn(self.META_DB) as conn, conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO load_log (table_name, load_timestamp, status, rows_loaded, error_message)
                    VALUES (%s, UTC_TIMESTAMP(), %s, %s, %s)
                """, (table, status, rows, str(error)[:1000] if error else None))
        except Exception as e:
            log.warning(f"Không ghi được log: {e}")

    def process_table(self, mapping: Dict) -> bool:
        src = mapping["source_table"]
        dst = mapping["target_table"]
        ts_col = mapping.get("timestamp_column")
        load_type = mapping["load_type"]
        log.info(f"Đang xử lý: {src} → {dst} [{load_type}] (timestamp_column={ts_col})")

        try:
            with self.mysql_conn(self.STAGING_DB) as conn, conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM `{src}` LIMIT 1")
                if not cur.fetchone():
                    log.info(f"Bảng {src} rỗng → bỏ qua")
                    self.log_run(src, "SUCCESS", 0)
                    return True
        except Exception as e:
            log.error(f"Không truy vấn được bảng {src}: {e}")
            self.log_run(src, "FAILED", 0, str(e))
            return False

        since = self.get_last_load_ts(src) if load_type == "incremental" and ts_col else None
        raw_data = self.fetch_data(src, ts_col, since)
        if not raw_data:
            log.info(f"Không có dữ liệu mới cho {src}")
            self.log_run(src, "SUCCESS", 0)
            return True

        data = self.transform_rows(raw_data)
        success = self.load_to_bq(data, dst, load_type)
        self.log_run(src, "SUCCESS" if success else "FAILED", len(data))
        return success

    def run(self):
        log.info("BẮT ĐẦU LOAD WEATHER → BIGQUERY")
        mappings = self.get_mappings()
        if not mappings:
            log.error("KHÔNG TÌM THẤY MAPPING! Kiểm tra bảng mapping_info")
            return

        success = sum(1 for m in mappings if self.process_table(m))
        log.info(f"HOÀN TẤT: {success}/{len(mappings)} bảng thành công")

        print("\n" + "="*70)
        print("       WEATHER → BIGQUERY LOAD HOÀN TẤT")
        print("="*70)
        print(f"   Tổng số bảng   : {len(mappings)}")
        print(f"   Thành công     : {success}")
        print(f"   Thất bại       : {len(mappings) - success}")
        print("="*70)


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()