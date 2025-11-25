import logging
import datetime
import os
from typing import Dict, List, Optional
from decimal import Decimal

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from sqlalchemy import select, func, MetaData, Integer, String, Numeric, DateTime, Text
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError

from database.setup_db import setup_database
from etl_metadata.models import MappingInfo, LoadLog
from transform.models import (
    DimLocation, DimCyclone,
    FactHeavyRainSnow, FactThunderstorm,
    FactFog, FactGale, FactCycloneTrack,
)
from database.base import session_scope
from service.email_service import send_email

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
ALERT_EMAIL = os.getenv("ALERT_EMAIL") or "caominhhieunq@gmail.com"

PROJECT_ID = "datawarehouse-478311"
DATASET_ID = "dataset_weather"

TABLE_TO_MODEL = {
    "dim_location": DimLocation,
    "dim_cyclone": DimCyclone,
    "fact_heavy_rain_snow": FactHeavyRainSnow,
    "fact_thunderstorm": FactThunderstorm,
    "fact_fog": FactFog,
    "fact_gale": FactGale,
    "fact_cyclone_track": FactCycloneTrack,
}

class WeatherLoadToBigQuery:
    def __init__(self):
        key_path = os.path.join(os.path.dirname(__file__), "bigquery-key.json")
        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy bigquery-key.json tại: {key_path}")

        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=PROJECT_ID)
        self._ensure_dataset()

        self.staging_engine, _ = setup_database(
            "transform_db_url",
            lambda: log.error("Không thể kết nối đến Transform DB"),
            echo=False
        )
        
        _, self.MetaSession = setup_database(
            "elt_db_url",
            lambda: log.error("Không thể kết nối đến ELT Metadata DB"),
            echo=False
        )

        self.metadata = MetaData()
        self.metadata.reflect(bind=self.staging_engine)
        log.info(f"Reflect {len(self.metadata.tables)} bảng từ db_stage_transform")

    def _ensure_dataset(self):
        dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Tạo dataset mới: {DATASET_ID}")

    def generate_bq_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        model = TABLE_TO_MODEL[table_name]
        fields = []
        for col in model.__table__.columns:
            t = type(col.type)
            bq_type = {
                Integer: "INTEGER",
                Numeric: "FLOAT",
                DateTime: "TIMESTAMP",
                String: "STRING",
                Text: "STRING",
            }.get(t, "STRING")
            mode = "REQUIRED" if col.primary_key else "NULLABLE"
            fields.append(bigquery.SchemaField(col.name, bq_type, mode=mode))
        return fields

    def has_previous_success_with_data(self, src: str) -> bool:
        with session_scope(self.MetaSession) as s:
            return (
                s.query(LoadLog)
                .filter(
                    LoadLog.source_name == src,
                    LoadLog.status == "SUCCESS",
                    LoadLog.record_count > 0,
                )
                .first()
                is not None
            )

    def get_mappings(self) -> List[Dict]:
        with session_scope(self.MetaSession) as s:
            rows = (
                s.query(MappingInfo)
                .filter(MappingInfo.is_active)
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
                if r.source_table in TABLE_TO_MODEL
            ]

    def get_last_load_ts(self, table: str) -> Optional[datetime.datetime]:
        with session_scope(self.MetaSession) as s:
            ts = (
                s.query(func.max(LoadLog.end_at))
                .filter(LoadLog.source_name == table, LoadLog.status == "SUCCESS")
                .scalar()
            )
            if ts and ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            return ts

    def fetch_data(
        self,
        table_name: str,
        ts_col: Optional[str],
        since: Optional[datetime.datetime],
    ) -> List[Dict]:
        table = self.metadata.tables[table_name]
        query = select(table)
        if ts_col and since:
            query = query.where(table.c[ts_col] > since)
        with self.staging_engine.connect() as conn:
            return [dict(row._mapping) for row in conn.execute(query)]

    def transform_rows(self, rows: List[Dict]) -> List[Dict]:
        return [
            {
                k: (
                    v.replace(tzinfo=datetime.timezone.utc).isoformat(timespec="seconds")
                    if isinstance(v, datetime.datetime) and v.tzinfo is None
                    else v.isoformat(timespec="seconds")
                    if isinstance(v, datetime.datetime)
                    else float(v)
                    if isinstance(v, Decimal)
                    else v
                )
                for k, v in row.items()
            }
            for row in rows
        ]

    def load_to_bq(self, rows: List[Dict], target_table: str, load_type: str) -> bool:
        if not rows:
            return True

        schema = self.generate_bq_schema(target_table)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{target_table}"

        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if load_type == "full"
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        # Incremental: cho phép thêm field mới nếu model cập nhật
        if load_type == "incremental":
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]

        try:
            job = self.bq.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            log.info(f"Load thành công {len(rows):,} bản ghi → {table_id}")
            return True
        except Exception as e:
            log.error(f"Load thất bại {target_table}: {e}")
            return False

    def log(
        self,
        status: str,
        src: str,
        dst: str,
        count: int,
        start: datetime.datetime,
        msg: str,
    ):
        end = datetime.datetime.now(datetime.timezone.utc)

        with session_scope(self.MetaSession) as s:
            s.add(
                LoadLog(
                    status=status,
                    record_count=count,
                    source_name=src,
                    table_name=dst,
                    message=msg,
                    start_at=start,
                    end_at=end,
                )
            )

        color = "\033[92m" if status == "SUCCESS" else "\033[91m"
        icon = "THÀNH CÔNG" if status == "SUCCESS" else "THẤT BẠI"
        print(
            f"{color}[{icon}]\033[0m {dst.ljust(28)} | {count:>8,} bản ghi | {msg}"
        )

        if status == "FAILED" and ALERT_EMAIL:
            subject = f"[Weather ETL] LOAD FAILED - {dst}"
            content = f"""
Source      : {src}
Target      : {dst}
Status      : {status}
Records     : {count:,}
Start       : {start}
End         : {end}

Chi tiết: {msg}
            """.strip()
            try:
                send_email(ALERT_EMAIL, subject, content, html=False)
            except Exception as e:
                log.error(f"Gửi email thất bại: {e}")

    def process_table(self, m: Dict) -> bool:
        src = m["source_table"]
        dst = m["target_table"]
        ts_col = m.get("timestamp_column")
        load_type = m["load_type"]
        start = datetime.datetime.now(datetime.timezone.utc)

        log.info(f"→ {src} → {dst} [{load_type}]")

        try:
            if src not in self.metadata.tables:
                self.log(
                    "FAILED",
                    src,
                    dst,
                    0,
                    start,
                    "Bảng nguồn không tồn tại trong MySQL (metadata)",
                )
                return False

            table_obj = self.metadata.tables[src]

            with self.staging_engine.connect() as conn:
                count = (
                    conn.execute(
                        select(func.count()).select_from(table_obj)
                    ).scalar()
                )

            if count == 0:
                if self.has_previous_success_with_data(src):
                    self.log(
                        "FAILED",
                        src,
                        dst,
                        0,
                        start,
                        "Bảng rỗng bất thường (trước đó từng có dữ liệu)",
                    )
                    return False
                self.log("SUCCESS", src, dst, 0, start, "Bảng rỗng (chưa có dữ liệu)")
                return True

            since = (
                self.get_last_load_ts(src)
                if load_type == "incremental" and ts_col
                else None
            )
            data = self.fetch_data(src, ts_col, since)
            if not data:
                self.log("SUCCESS", src, dst, 0, start, "Không có dữ liệu mới")
                return True

            transformed = self.transform_rows(data)
            success = self.load_to_bq(transformed, dst, load_type)

            status = "SUCCESS" if success else "FAILED"
            msg = f"Load {'thành công' if success else 'thất bại'} {len(data):,} bản ghi"
            self.log(status, src, dst, len(data), start, msg)
            return success

        except (NoSuchTableError, SQLAlchemyError) as e:
            self.log(
                "FAILED",
                src,
                dst,
                0,
                start,
                f"Lỗi DB MySQL: {e}",
            )
            return False
        except Exception as e:
            self.log(
                "FAILED",
                src,
                dst,
                0,
                start,
                f"Lỗi không mong đợi: {e}",
            )
            return False

    def run(self):
        print("\n" + "=" * 88)
        print("     WEATHER → BIGQUERY ETL - BẮT ĐẦU CHẠY")
        print("=" * 88)

        mappings = self.get_mappings()
        if not mappings:
            print("Không có mapping nào active!")
            return

        results = [self.process_table(m) for m in mappings]
        success = sum(results)
        failed = len(mappings) - success

        print("\n" + "=" * 88)
        print(f"  TỔNG: {len(mappings)} bảng | THÀNH CÔNG: {success} | THẤT BẠI: {failed}")
        print("=" * 88 + "\n")

        if failed and ALERT_EMAIL:
            content = (
                f"Tổng: {len(mappings)} bảng\n"
                f"Thành công: {success}\n"
                f"Thất bại: {failed}\n"
                f"Xem chi tiết trong hệ thống."
            )
            try:
                send_email(
                    ALERT_EMAIL,
                    "[Weather ETL] Có bảng LOAD FAILED",
                    content,
                    html=False,
                )
            except Exception as e:
                log.error(f"Gửi email tổng kết thất bại: {e}")


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()
