-- ====================================================================
-- 1. CREATE DATABASES
-- ====================================================================

CREATE DATABASE IF NOT EXISTS db_etl_metadata CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS db_stage_clean CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS db_stage_transform CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE db_etl_metadata;

-- ====================================================================
-- 2. RECREATE TABLES
-- ====================================================================

DROP TABLE IF EXISTS load_log;
DROP TABLE IF EXISTS mapping_info;

CREATE TABLE mapping_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    timestamp_column VARCHAR(100),
    load_type ENUM('full', 'incremental') DEFAULT 'incremental',
    load_order INT DEFAULT 99,
    is_active BOOLEAN DEFAULT TRUE,
    note VARCHAR(255)
);

CREATE TABLE load_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    load_timestamp DATETIME NOT NULL,
    status ENUM('SUCCESS', 'FAILED') NOT NULL,
    rows_loaded INT DEFAULT 0,
    error_message TEXT,
    executed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ====================================================================
-- 3. INSERT MAPPING DATA
-- ====================================================================

INSERT INTO mapping_info (source_table, target_table, timestamp_column, load_type, load_order, note) VALUES
('DimLocation',       'DimLocation',       NULL,        'full',        1,  'Không có timestamp → full load'),
('DimCyclone',        'DimCyclone',        'updatedAt', 'full',        2,  'Thường ít thay đổi → full load'),

('FactHeavyRain',     'FactHeavyRain',     'createdAt', 'incremental', 10, NULL),
('FactThunderstorm',  'FactThunderstorm',  'createdAt', 'incremental', 10, NULL),
('FactFog',           'FactFog',           'createdAt', 'incremental', 10, NULL),
('FactCycloneTrack',  'FactCycloneTrack',  'createdAt', 'incremental', 10, NULL),
('FactGale',          'FactGale',          'createdAt', 'incremental', 10, 'Sửa từ FactGlide/FactWave thành FactGale');



CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_password';

GRANT ALL PRIVILEGES ON db_etl_metadata.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_clean.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_transform.* TO 'etl_user'@'%';

FLUSH PRIVILEGES;
