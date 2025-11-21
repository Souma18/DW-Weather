# test_log.py

from log_service import LogService
log = LogService()

# --- tạo 1 lần chạy ---
run = log.create_run("test_job", run_number=1, total_links=2)
print("RUN ID:", run.id)

# --- tạo event 1 ---
event1 = log.create_event(run.id, "http://example.com/api-1")
print("EVENT 1 ID:", event1.id)

log.finish_event_success(event1.id, "file1.json", record_count=120)

# --- tạo event 2 ---
event2 = log.create_event(run.id, "http://example.com/api-2")
print("EVENT 2 ID:", event2.id)

log.finish_event_fail(event2.id, "HTTP_500", "Internal Server Error")

# --- kết thúc run ---
log.finish_run(run.id, success_count=1, fail_count=1)

print("✔ TEST DONE")
