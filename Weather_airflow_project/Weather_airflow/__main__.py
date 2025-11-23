# =============================================================================
# main.py
# Đường dẫn: D:\DW-Weather\Weather_airflow_project\Weather_airflow\main.py
# Mục đích: Entry point chính để chạy job LOAD dữ liệu từ MySQL (Transformed)
#           → Google BigQuery + ghi log + gửi email cảnh báo
# =============================================================================

import os
import sys
import logging
from datetime import datetime

# --------------------------------------------------------------------------- #
# Thêm thư mục gốc project vào sys.path để import được các module con
# --------------------------------------------------------------------------- #
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # Weather_airflow folder
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Cấu hình logging đẹp ngay từ đầu (sẽ hiển thị cả trong Airflow nếu cần)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("WEATHER_LOAD_MAIN")


def banner():
    """In banner đẹp khi khởi động"""
    print("\n" + "=" * 80)
    print(" " * 20 + "WEATHER DATA WAREHOUSE - LOAD TO BIGQUERY")
    print(" " * 25 + "Job khởi chạy lúc:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 80 + "\n")


def main() -> None:
    banner()

    try:
        # Import ở đây để tránh circular import + báo lỗi rõ ràng nếu thiếu thư viện
        from scripts.load.load_to_bigquery import WeatherLoadToBigQuery

        log.info("Khởi tạo job Load → BigQuery...")
        loader = WeatherLoadToBigQuery()
        loader.run()

        log.info("TOÀN BỘ QUY TRÌNH LOAD HOÀN TẤT THÀNH CÔNG!")
        print("\n" + "HOÀN TẤT!".center(80, "=") + "\n")

    except ImportError as ie:
        error_msg = f"Thiếu module hoặc cấu trúc thư mục sai: {ie}"
        log.error(error_msg)
        print(f"\nLỖI IMPORT: {error_msg}")
        print("Kiểm tra lại cấu trúc thư mục và file load_to_bigquery.py")
        sys.exit(1)

    except FileNotFoundError as fe:
        error_msg = str(fe)
        log.error(error_msg)
        print(f"\nLỖI FILE: {error_msg}")
        print("Không tìm thấy bigquery-key.json? Đặt nó cùng thư mục với load_to_bigquery.py")
        sys.exit(1)

    except Exception as e:
        error_msg = f"Lỗi không mong muốn trong job Load: {type(e).__name__}: {e}"
        log.exception(error_msg)
        print(f"\nCRITICAL ERROR: {error_msg}")
        sys.exit(1)


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    # Cách chạy thủ công:
    # python D:\DW-Weather\Weather_airflow_project\Weather_airflow\main.py
    main()