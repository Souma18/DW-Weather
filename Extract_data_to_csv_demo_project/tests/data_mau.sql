-- ============================================================
-- Script dữ liệu mẫu cho hệ thống log extract
-- 
-- Bối cảnh:
--   - Job:   extract_weather_api
--   - Ngày:  2025-11-21  (thư mục raw/20251121/)
--   - Lần chạy: run1  → đặt run_number = 1
--   - Có 6 URL trong file links.txt nhưng tạo ra 7 file CSV:
--       + 1 file heavyrain_snow
--       + 1 file thunderstorms
--       + 1 file fog
--       + 1 file gale
--       + 1 file tc            (danh sách bão đang hoạt động)
--       + 1 file tc_track_2025204
--       + 1 file tc_forecast_2025204
--     => 1 URL bão chi tiết (tc_2025204) sinh ra 2 file (track + forecast),
--        nên tổng số file CSV = 7.
--

-- ============================================================
-- Bảng log lần chạy extract: log_extract_run
--
-- 1 dòng = 1 lần chạy job (1 lần gọi hàm run()).
--
-- Các cột:
--   - id           : khóa chính, định danh duy nhất cho mỗi lần chạy
--   - job_name     : tên job ETL (vd: 'extract_weather_api')
--   - run_date     : ngày chạy (theo giờ hệ thống, chỉ lấy phần ngày)
--   - run_number   : số lần chạy trong ngày (1 = run1, 2 = run2, ...)
--   - started_at   : thời điểm bắt đầu chạy job
--   - finished_at  : thời điểm kết thúc job
--   - status       : trạng thái tổng thể (SUCCESS / PARTIAL_SUCCESS / FAIL)
--   - total_links  : tổng số URL đọc được từ file links.txt
--   - success_count: số "event" thành công trong log_extract_event
--                    (thường tương ứng với số file/đầu ra thành công)
--   - fail_count   : số "event" thất bại trong log_extract_event
--   - created_at   : thời điểm tạo bản ghi log (thường = started_at)
--   - updated_at   : thời điểm cập nhật cuối (khi job kết thúc)
-- ============================================================

CREATE TABLE log_extract_run (
    id            BIGINT       PRIMARY KEY,
    job_name      VARCHAR(100) NOT NULL,
    run_date      DATE         NOT NULL,
    run_number    INT          NOT NULL,          -- run1, run2, ...
    started_at    TIMESTAMP    NOT NULL,
    finished_at   TIMESTAMP,
    status        VARCHAR(20)  NOT NULL,         -- SUCCESS / PARTIAL_SUCCESS / FAIL
    total_links   INT,
    success_count INT,
    fail_count    INT,
    created_at    TIMESTAMP    NOT NULL,
    updated_at    TIMESTAMP
);


-- ============================================================
-- Bảng log chi tiết từng URL / từng file: log_extract_event
--
-- 1 dòng = 1 event xử lý (thường là 1 URL → 1 hoặc nhiều file).
--
-- Các cột:
--   - id           : khóa chính, định danh duy nhất cho mỗi event
--   - run_id       : tham chiếu tới log_extract_run.id (biết event thuộc lần chạy nào)
--   - step         : bước xử lý, ví dụ: 'EXTRACT', 'FETCH_JSON', 'WRITE_CSV' (ở đây gộp là 'EXTRACT')
--   - status       : SUCCESS / FAIL / SKIP / WARN cho event đó
--   - url          : URL API đã gọi
--   - file_name    : tên file CSV tạo ra (nếu thành công), ví dụ: 'fog-20251121_005650-run1.csv'
--   - data_type    : loại dữ liệu (fog, gale, heavyrain_snow, tc, tc_track_2025204, tc_forecast_2025204, ...)
--   - sys_id       : mã hệ thống bão (2025204) cho tropical cyclone; NULL cho loại khác
--   - record_count : số dòng dữ liệu ghi ra file (không tính header)
--   - error_code   : mã lỗi ngắn (HTTP_ERROR, JSON_INVALID, WRITE_CSV_FAILED, ...), NULL nếu SUCCESS
--   - error_message: mô tả chi tiết lỗi (NULL hoặc rỗng nếu SUCCESS)
--   - started_at   : thời điểm bắt đầu xử lý URL/event
--   - finished_at  : thời điểm kết thúc xử lý URL/event
--   - created_at   : thời điểm ghi log (thường bằng finished_at)
-- ============================================================

CREATE TABLE log_extract_event (
    id            BIGINT       PRIMARY KEY,
    run_id        BIGINT       NOT NULL,        -- FK -> log_extract_run.id
    step          VARCHAR(50)  NOT NULL,        -- ví dụ: 'EXTRACT'
    status        VARCHAR(20)  NOT NULL,        -- SUCCESS / FAIL / SKIP / WARN
    url           TEXT,
    file_name     VARCHAR(255),
    data_type     VARCHAR(100),                 -- fog, gale, heavyrain_snow, tc, tc_track_2025204, ...
    sys_id        VARCHAR(50),                  -- cho tropical cyclone (2025204), NULL cho loại khác
    record_count  INT,
    error_code    VARCHAR(50),
    error_message TEXT,
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    created_at    TIMESTAMP    NOT NULL
);


-- ============================================================
-- DỮ LIỆU MẪU
-- ============================================================

-- 1) Log cho 1 lần chạy (run1) vào ngày 2025-11-21
--    - Có 6 URL trong links.txt
--    - Tạo ra 7 file CSV trong thư mục raw/20251121/
--      (do 1 URL bão chi tiết sinh ra 2 file: track + forecast)

INSERT INTO log_extract_run (
    id, job_name, run_date, run_number,
    started_at, finished_at, status,
    total_links, success_count, fail_count,
    created_at, updated_at
) VALUES (
    1,
    'extract_weather_api',
    DATE '2025-11-21',
    1,                                      -- run1
    TIMESTAMP '2025-11-21 00:56:50',
    TIMESTAMP '2025-11-21 00:56:59',
    'SUCCESS',
    6,                                      -- 6 URL trong links.txt
    7,                                      -- 7 event thành công (tương ứng 7 file CSV)
    0,
    TIMESTAMP '2025-11-21 00:56:50',
    TIMESTAMP '2025-11-21 00:56:59'
);


-- 2) Log chi tiết cho từng URL / file (7 bản ghi)

INSERT INTO log_extract_event (
    id, run_id, step, status,
    url, file_name, data_type, sys_id,
    record_count, error_code, error_message,
    started_at, finished_at, created_at
) VALUES

-- 1. Heavy rain / snow
(
    1, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/hvyrain.json?_=1763476735106',
    'heavyrain_snow-20251121_005650-run1.csv',
    'heavyrain_snow',
    NULL,
    80,                   -- ví dụ: khoảng 80 dòng dữ liệu (không tính header)
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:50',
    TIMESTAMP '2025-11-21 00:56:52',
    TIMESTAMP '2025-11-21 00:56:52'
),

-- 2. Thunderstorms
(
    2, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/thunderstorms.json?_=1763476735107',
    'thunderstorms-20251121_005650-run1.csv',
    'thunderstorms',
    NULL,
    60,                   -- ví dụ
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:50',
    TIMESTAMP '2025-11-21 00:56:53',
    TIMESTAMP '2025-11-21 00:56:53'
),

-- 3. Fog
(
    3, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/fog.json?_=1763476735108',
    'fog-20251121_005650-run1.csv',
    'fog',
    NULL,
    306,                  -- file có 307 dòng, trừ 1 dòng header = 306 bản ghi
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:50',
    TIMESTAMP '2025-11-21 00:56:54',
    TIMESTAMP '2025-11-21 00:56:54'
),

-- 4. Gale
(
    4, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/gale.json?_=1763476735109',
    'gale-20251121_005650-run1.csv',
    'gale',
    NULL,
    170,                  -- ví dụ
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:51',
    TIMESTAMP '2025-11-21 00:56:55',
    TIMESTAMP '2025-11-21 00:56:55'
),

-- 5. Tropical cyclone in force (danh sách bão đang hoạt động)
(
    5, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/tc_inforce.json?_=1763476735110',
    'tc-20251121_005650-run1.csv',
    'tc',                 -- viết tắt từ "tropical cyclone"
    NULL,
    5,                    -- ví dụ: 5 hệ thống bão đang hoạt động
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:51',
    TIMESTAMP '2025-11-21 00:56:56',
    TIMESTAMP '2025-11-21 00:56:56'
),

-- 6. Tropical cyclone 2025204 - track
(
    6, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/tc_2025204.json?_=1763476735111',
    'tc_track_2025204-20251121_005650-run1.csv',
    'tc_track_2025204',
    '2025204',
    10,                   -- ví dụ: 10 mốc track trong lịch sử di chuyển
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:52',
    TIMESTAMP '2025-11-21 00:56:57',
    TIMESTAMP '2025-11-21 00:56:57'
),

-- 7. Tropical cyclone 2025204 - forecast
(
    7, 1, 'EXTRACT', 'SUCCESS',
    'https://severeweather.wmo.int/json/tc_2025204.json?_=1763476735111',
    'tc_forecast_2025204-20251121_005650-run1.csv',
    'tc_forecast_2025204',
    '2025204',
    8,                    -- ví dụ: 8 mốc dự báo tiếp theo
    NULL, NULL,
    TIMESTAMP '2025-11-21 00:56:52',
    TIMESTAMP '2025-11-21 00:56:58',
    TIMESTAMP '2025-11-21 00:56:58'
);


