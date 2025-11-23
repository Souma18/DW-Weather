from service.email_service import send_email
from database.base import session_scope
def log_email_status(to_email: str, subject: str, content: str):
    send_email(to_email, subject, content)
def log_db_status(log_obj, SessionLocal):
    with session_scope(SessionLocal) as session:
        session.add(log_obj)

def log_dual_status(log_obj, SessionLocal, to_email: str, subject: str, content: str):
    # 1. Log to Database
    try:
        log_db_status(log_obj, SessionLocal)
    except Exception as e:
        content += f"\n\nXảy ra lỗi: {e}"

    # 2. Send Email
    log_email_status(to_email, subject, content)
