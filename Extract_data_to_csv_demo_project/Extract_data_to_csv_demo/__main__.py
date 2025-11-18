"""Entry point để chạy package Extract_data_to_csv_demo."""

import sys
from pathlib import Path

# Thêm thư mục parent vào Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from Extract_data_to_csv_demo.extractor import run


def main() -> None:
    """Chạy hàm run() mặc định."""
    run()


if __name__ == "__main__":
    main()