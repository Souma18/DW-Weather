-- init.sql – PHIÊN BẢN CUỐI (20/11/2025) – DimLocation dùng createdAt incremental

CREATE DATABASE IF NOT EXISTS db_etl_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS db_stage_clean CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS db_stage_transform CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE db_etl_metadata;

DROP TABLE IF EXISTS mapping_info;
CREATE TABLE mapping_info (
    id               INT AUTO_INCREMENT PRIMARY KEY               COMMENT 'Khóa chính tự tăng',
    source_table     VARCHAR(100) NOT NULL                        COMMENT 'Tên bảng nguồn trong db_stage_transform',
    target_table     VARCHAR(100) NOT NULL                        COMMENT 'Tên bảng đích trong BigQuery',
    timestamp_column VARCHAR(100) NULL                            COMMENT 'Cột timestamp để incremental (NULL = full load)',
    load_type        ENUM('full', 'incremental') DEFAULT 'incremental' COMMENT 'Kiểu load: full hoặc incremental',
    load_order       INT DEFAULT 99                               COMMENT 'Thứ tự chạy (số nhỏ chạy trước, Dim trước Fact)',
    is_active        BOOLEAN DEFAULT TRUE                         COMMENT 'Bật/tắt bảng này (TRUE = load, FALSE = bỏ qua)',
    note             VARCHAR(255) NULL                            COMMENT 'Ghi chú thêm (ví dụ lý do full load)'
) COMMENT = 'Cấu hình cách load từng bảng từ staging → BigQuery';

DROP TABLE IF EXISTS load_log;
CREATE TABLE load_log (
    id            INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Khóa chính log',
    status        VARCHAR(20)  NOT NULL               COMMENT 'Trạng thái bước load',
    record_count  INT          NULL                   COMMENT 'Số bản ghi đã load',
    source_name   VARCHAR(255) NULL                   COMMENT 'Tên bảng nguồn trong staging',
    table_name    TEXT         NULL                   COMMENT 'Tên bảng đích trong BigQuery',
    message       TEXT         NULL                   COMMENT 'Chi tiết log, cảnh báo hoặc lỗi',
    start_at      DATETIME     NULL                   COMMENT 'Thời gian bắt đầu load',
    end_at        DATETIME     NULL                   COMMENT 'Thời gian kết thúc load'
);

TRUNCATE TABLE mapping_info;
INSERT INTO mapping_info (source_table, target_table, timestamp_column, load_type, load_order, note) VALUES
('DimLocation',       'DimLocation',       'createdAt', 'incremental', 1,  'Dung createdAt de incremental load'),
('DimCyclone',        'DimCyclone',        'updatedAt', 'full',        2,  'Thường it thay doi → full load'),
('FactHeavyRain',     'FactHeavyRain',     'createdAt', 'incremental', 10, NULL),
('FactThunderstorm',  'FactThunderstorm',  'createdAt', 'incremental', 10, NULL),
('FactFog',           'FactFog',           'createdAt', 'incremental', 10, NULL),
('FactCycloneTrack',  'FactCycloneTrack',  'createdAt', 'incremental', 10, NULL),
('FactGale',          'FactGale',          'createdAt', 'incremental', 10, NULL);

CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_password';
GRANT ALL PRIVILEGES ON db_etl_metadata.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_clean.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_transform.* TO 'etl_user'@'%';
FLUSH PRIVILEGES;