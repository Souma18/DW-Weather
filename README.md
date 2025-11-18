# DW-Weather

## Mục tiêu dự án
DW-Weather xây dựng một data pipeline tự động cho dữ liệu thời tiết:
- Thu thập từ nhiều nguồn (API, HTML, JSON…)
- Làm sạch, chuyển đổi với Python
- Tải vào Data Warehouse (BigQuery/Redshift/Snowflake/Postgres tuỳ cấu hình)
- Quản lý toàn bộ luồng công việc bằng Apache Airflow

## Cấu trúc thư mục chính
```
DW-Weather/
├─ README.md
└─ Weather_airflow_project/
   ├─ dev-requirements.txt     # Danh sách thư viện phục vụ phát triển/Airflow
   ├─ pyproject.toml           # Thông tin gói & cấu hình build (poetry)
   ├─ Weather_airflow/         # Source code chính
   │  ├─ dags/                 # Định nghĩa DAG Airflow
   │  └─ scripts/              # Module extract/clean/transform/load
   └─ tests/                   # Bộ kiểm thử
```

## Yêu cầu hệ thống trước khi cài Airflow
Kiểm tra phiên bản các công cụ ngay trên terminal/PowerShell:
- **Python** (<= 3.12):
  ```powershell
  python --version
  ```
  Nếu phiên bản lớn hơn 3.12, hãy cài thêm Python 3.12 hoặc thấp hơn.
- **pip** (đi kèm Python, cần để cài Airflow):
  ```powershell
  pip --version
  ```
  Đảm bảo pip hoạt động vì toàn bộ package Airflow cài qua pip.
- **MySQL** (≤ 8.x để tương thích connector):
  ```powershell
  mysql --version
  ```
  Nếu hệ thống dùng dịch vụ database khác, vẫn cần client MySQL tương thích với Airflow hook.

## Khởi tạo môi trường Airflow đầu tiên
1. **Chuẩn bị project Python trong VS Code**: mở thư mục `DW-Weather` trong VS Code hoặc IDE bạn dùng.
2. **Tạo virtualenv dành riêng Airflow** (Ví dụ dùng Python 3.12 đã cài với launcher `py`):
   ```powershell
   py -3.12 -m venv airflow_env
   ```
   Sau khi chạy sẽ có thư mục `airflow_env/` ngay trong project để chứa toàn bộ thư viện Airflow.
3. **Kích hoạt môi trường**:
   ```powershell
   airflow_env\Scripts\activate
   ```
   Terminal hiển thị dạng `(airflow_env) D:\project\...>` là đã kích hoạt thành công. Mọi lệnh tiếp theo (cài thư viện, chạy DAG) đều nên chạy trong môi trường này.

## Cài thư viện từ requirements
1. Đảm bảo môi trường ảo đã kích hoạt (`(airflow_env)`).
2. Cập nhật pip:
   ```powershell
   pip install --upgrade pip
   ```
3. Cài bộ thư viện chuẩn của dự án:
   ```powershell
   pip install -r Weather_airflow_project/dev-requirements.txt
   ```
   - Nếu bạn có file `requirements.txt` riêng, thay đường dẫn tương ứng.
   - Khi thêm thư viện mới, cập nhật file yêu cầu rồi chạy lại lệnh này để đồng bộ.
4. (Tuỳ chọn) Nếu dùng Poetry trong thư mục `Weather_airflow_project`, kích hoạt env rồi:
   ```powershell
   cd Weather_airflow_project
   poetry install
   ```

## Tránh đẩy môi trường Airflow/venv lên Git
Tạo `.gitignore` (nếu chưa có) với nội dung gợi ý:
```
.venv/
airflow_env/
Weather_airflow_project/.venv/
Weather_airflow_project/.airflow/
airflow_home/
logs/
*.db
.env
```
Trước khi `git push`, luôn chạy `git status` để chắc chắn chỉ commit DAG, scripts và cấu hình cần thiết.

## Việc tiếp theo
- Hoàn thiện tài liệu cài đặt Airflow chi tiết (biến môi trường, connection, scheduler).
- Tạo DAG thực tế cho luồng extract → clean → transform → load.
- Bổ sung test dưới `Weather_airflow_project/tests`.



