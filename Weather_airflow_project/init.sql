CREATE DATABASE IF NOT EXISTS db_stage_transform;
CREATE DATABASE IF NOT EXISTS db_etl_metadata;
CREATE DATABASE IF NOT EXISTS db_stage_clean;

-- Tạo database cho Airflow metadata
CREATE DATABASE IF NOT EXISTS airflow_db;

-- Gán quyền cho user etl_user trên các DB này
GRANT ALL PRIVILEGES ON db_stage_transform.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_etl_metadata.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_clean.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'etl_user'@'%';

-- Áp dụng thay đổi quyền
FLUSH PRIVILEGES;
