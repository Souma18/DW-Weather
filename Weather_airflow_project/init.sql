-- Tạo thêm các database
CREATE DATABASE IF NOT EXISTS db_stage_transform;
CREATE DATABASE IF NOT EXISTS db_etl_metadata;
CREATE DATABASE IF NOT EXISTS db_stage_clean;

-- Gán quyền cho user etl_user trên các DB này
GRANT ALL PRIVILEGES ON db_stage_transform.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_etl_metadata.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON db_stage_clean.* TO 'etl_user'@'%';

-- Áp dụng thay đổi quyền
FLUSH PRIVILEGES;
