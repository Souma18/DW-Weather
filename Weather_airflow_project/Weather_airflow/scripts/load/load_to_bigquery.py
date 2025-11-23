import os
import logging
from datetime import datetime
from typing import List, Dict, Any

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

from database.setup_db import SessionELT, DEFAULT_RECIEVER_EMAIL
from database.base import session_scope
from database.logger import log_dual_status
from elt_metadata.models import LoadLog

# --------------------------------------------------------------------------- #
#                               CẤU HÌNH LOGGING                              #
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#                          LOAD WEATHER → BIGQUERY CLASS                      #
# --------------------------------------------------------------------------- #
class WeatherLoadToBigQuery:
    PROJECT_ID = "datawarehouse-478311"
    DATASET_ID = "dataset_weather"

    # Schema chính xác 100% – dùng để validate trước khi load
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

    def __init__(self):
        # 1. Kết nối BigQuery
        current_dir = os.path.dirname(os.path.abspath(__file__))
        key_path = os.path.join(current_dir, "bigquery-key.json")

        if not os.path.exists(key_path):
            raise FileNotFoundError(f"Không tìm thấy file key: {key_path}")

        log.info("Khởi tạo client BigQuery...")
        creds = service_account.Credentials.from_service_account_file(key_path)
        self.bq = bigquery.Client(credentials=creds, project=self.PROJECT_ID)

        # 2. Đảm bảo dataset tồn tại
        dataset_ref = f"{self.PROJECT_ID}.{self.DATASET_ID}"
        try:
            self.bq.get_dataset(dataset_ref)
            log.info(f"Dataset '{self.DATASET_ID}' đã sẵn sàng.")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "asia-southeast1"
            self.bq.create_dataset(dataset)
            log.info(f"Đã tạo dataset mới: {self.DATASET_ID}")

    # ------------------------------------------------------------------- #
    #                     LẤY MAPPING TỪ DB (giống cũ)                    #
    # ------------------------------------------------------------------- #
    def get_active_mappings(self) -> List[Dict[str, Any]]:
        with session_scope(SessionELT) as session:
            # Giả sử bạn có model MappingInfo trong elt_metadata.models
            # Nếu chưa có thì tạo nhanh: table mapping_info với các cột như dưới
            from elt_metadata.models import MappingInfo
            return (
                session.query(MappingInfo)
                .filter(MappingInfo.is_active == True)
                .order_by(MappingInfo.load_order)
                .all()
            )

    # ------------------------------------------------------------------- #
    #                     TRANSFORM DỮ LIỆU CHO BIGQUERY                  #
    # ------------------------------------------------------------------- #
    @staticmethod
    def transform_row_for_bq(row: Dict[str, Any]) -> Dict[str, Any]:
        transformed = {}
        for k, v in row.items():
            if isinstance(v, datetime):
                if v.tzinfo is None:
                    v = v.replace(tzinfo=datetime.timezone.utc)
                transformed[k] = v.isoformat(timespec="seconds")
            elif isinstance(v, (int, float, str, bool)) or v is None:
                transformed[k] = v
            else:
                transformed[k] = str(v)  # fallback
        return transformed

    # ------------------------------------------------------------------- #
    #                          LOAD MỘT BẢNG                               #
    # ------------------------------------------------------------------- #
    def load_table(self, mapping) -> bool:
        src_table = mapping.source_table
        dst_table = mapping.target_table
        ts_column = mapping.timestamp_column
        load_type = mapping.load_type.lower()  # "full" hoặc "incremental"

        log.info(f"→ Bắt đầu load: {src_table} → {dst_table} [{load_type}]")

        start_at = datetime.now(datetime.timezone.utc)

        try:
            # 1. Lấy dữ liệu từ bảng Transform (MySQL)
            with session_scope(SessionELT) as session:
                model_cls = self.get_model_by_table_name(src_table)
                query = session.query(model_cls)

                if load_type == "incremental" and ts_column:
                    last_ts = self.get_last_successful_load_ts(dst_table)
                    if last_ts:
                        query = query.filter(getattr(model_cls, ts_column) > last_ts)

                rows = query.all()

            if not rows:
                self._log_success(src_table, dst_table, 0, start_at, "Không có dữ liệu mới")
                return True

            # 2. Kiểm tra schema chính xác
            actual_cols = set(rows[0].__dict__.keys()) - {"_sa_instance_state"}
            schema_error = self._validate_schema(src_table, actual_cols)
            if schema_error:
                raise ValueError(schema_error)

            # 3. Transform + Load lên BigQuery
            json_rows = [self.transform_row_for_bq(r.__dict__) for r in rows]

            table_id = f"{self.PROJECT_ID}.{self.DATASET_ID}.{dst_table}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if load_type == "full"
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
                autodetect=False,  # tự động detect nhưng vẫn an toàn vì schema đã validate
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            )

            job = self.bq.load_table_from_json(json_rows, table_id, job_config=job_config)
            job.result()  # chờ hoàn thành

            record_count = len(json_rows)
            log.info(f"✓ Load thành công {record_count:,} bản ghi → {dst_table}")
            self._log_success(src_table, dst_table, record_count, start_at)

            return True

        except Exception as e:
            end_at = datetime.now(datetime.timezone.utc)
            error_msg = f"Load thất bại bảng {dst_table}: {str(e)}"
            log.error(error_msg)
            self._log_failure(src_table, dst_table, start_at, end_at, error_msg)
            return False

    # ------------------------------------------------------------------- #
    #                           HELPER METHODS                             #
    # ------------------------------------------------------------------- #
    def get_model_by_table_name(self, table_name: str):
        """Map tên bảng → SQLAlchemy model trong transform.models"""
        from transform.models import (
            DimLocation, DimCyclone,
            FactHeavyRain, FactThunderstorm, FactFog, FactGale, FactCycloneTrack,
        )
        mapping = {
            "dim_location": DimLocation,
            "dim_cyclone": DimCyclone,
            "fact_heavy_rain": FactHeavyRain,
            "fact_thunderstorm": FactThunderstorm,
            "fact_fog": FactFog,
            "fact_gale": FactGale,
            "fact_cyclone_track": FactCycloneTrack,
        }
        model = mapping.get(table_name.lower())
        if not model:
            raise ValueError(f"Không tìm thấy model cho bảng: {table_name}")
        return model

    def get_last_successful_load_ts(self, target_table: str) -> datetime | None:
        with session_scope(SessionELT) as session:
            last = (
                session.query(LoadLog.end_at)
                .filter(LoadLog.table_name == target_table, LoadLog.status == "SUCCESS")
                .order_by(LoadLog.end_at.desc())
                .first()
            )
            return last[0] if last else None

    def _validate_schema(self, table_name: str, actual: set) -> str | None:
        expected = self.EXACT_SCHEMA.get(table_name.lower())
        if not expected:
            return f"Bảng '{table_name}' không được định nghĩa trong EXACT_SCHEMA"
        if actual != expected:
            missing = expected - actual
            extra = actual - expected
            parts = []
            if missing:
                parts.append(f"thiếu: {', '.join(sorted(missing))}")
            if extra:
                parts.append(f"thừa: {', '.join(sorted(extra))}")
            return f"Schema không khớp [{table_name}]: {'; '.join(parts)}"
        return None

    def _log_success(self, src, dst, count, start_at, msg=None):
        end_at = datetime.now(datetime.timezone.utc)
        log_obj = LoadLog(
            status="SUCCESS",
            record_count=count,
            source_name=src,
            table_name=dst,
            message=msg or f"Load thành công {count:,} bản ghi",
            start_at=start_at,
            end_at=end_at,
        )
        with session_scope(SessionELT) as session:
            log_dual_status(
                log_obj,
                session,
                to_email=DEFAULT_RECIEVER_EMAIL,
                subject=f"[LOAD SUCCESS] {dst}",
                content=f"Đã load thành công {count:,} bản ghi từ {src} → BigQuery.{dst}",
            )

    def _log_failure(self, src, dst, start_at, end_at, error_detail):
        log_obj = LoadLog(
            status="FAILURE",
            record_count=0,
            source_name=src,
            table_name=dst,
            message=error_detail,
            start_at=start_at,
            end_at=end_at,
        )
        with session_scope(SessionELT) as session:
            log_dual_status(
                log_obj,
                session,
                to_email=DEFAULT_RECIEVER_EMAIL,
                subject=f"[LOAD FAILED] {dst}",
                content=f"Load thất bại!\nBảng: {dst}\nLỗi: {error_detail}",
            )

    # ------------------------------------------------------------------- #
    #                               RUN MAIN                               #
    # ------------------------------------------------------------------- #
    def run(self):
        log.info("=== BẮT ĐẦU LOAD WEATHER → BIGQUERY ===")
        mappings = self.get_active_mappings()

        if not mappings:
            error = "Không có mapping nào active trong bảng mapping_info!"
            log.error(error)
            log_dual_status(
                LoadLog(status="FAILURE", message=error, table_name="ALL", record_count=0),
                SessionELT(),
                to_email=DEFAULT_RECIEVER_EMAIL,
                subject="[LOAD CRITICAL] Không có mapping active",
                content=error,
            )
            return

        success = 0
        failed_tables = []

        for m in mappings:
            if self.load_table(m):
                success += 1
            else:
                failed_tables.append(m.target_table)

        # Báo cáo cuối cùng
        total = len(mappings)
        log.info(f"HOÀN TẤT: {success}/{total} bảng thành công")

        summary = f"""
WEATHER → BIGQUERY LOAD HOÀN TẤT
Tổng bảng: {total}
Thành công: {success}
Thất bại: {len(failed_tables)}
{"Bảng lỗi: " + ", ".join(failed_tables) if failed_tables else ""}
        """.strip()

        print("\n" + "=" * 70)
        print(summary.replace("\n", "\n   "))
        print("=" * 70)

        # Gửi email tổng hợp
        subject = "[LOAD SUCCESS] Toàn bộ thành công" if not failed_tables else "[LOAD ERROR] Có bảng thất bại"
        log_dual_status(
            LoadLog(status="SUCCESS" if not failed_tables else "FAILURE", message=summary, table_name="SUMMARY"),
            SessionELT(),
            to_email=DEFAULT_RECIEVER_EMAIL,
            subject=subject,
            content=summary,
        )


if __name__ == "__main__":
    WeatherLoadToBigQuery().run()