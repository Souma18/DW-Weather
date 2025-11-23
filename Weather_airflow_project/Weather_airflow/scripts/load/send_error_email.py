import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )


def send_error_email(
    error_message: str,
    table_name: str = "Unknown",
    recipients: Optional[list[str]] = None,
    subject_prefix: str = "[ETL ERROR]"
) -> bool:
    """
    Gửi email cảnh báo lỗi ETL (có thể dùng cho cả lỗi chi tiết và báo cáo tổng hợp).

    Args:
        error_message (str): Nội dung chi tiết lỗi
        table_name (str): Tên bảng bị lỗi (hoặc mô tả chung như "TỔNG HỢP LOAD")
        recipients (list[str], optional): Danh sách email nhận (nếu None thì lấy từ biến môi trường hoặc mặc định)
        subject_prefix (str): Prefix tiêu đề, ví dụ "[ETL ERROR]" hoặc "[ETL SUCCESS]"

    Returns:
        bool: True nếu gửi thành công, False nếu thất bại
    """
    # ====================== CẤU HÌNH ======================
    YOUR_EMAIL = "caominhhieunq@gmail.com"
    APP_PASSWORD = "dioefdvjhtgzihzq"        

    DEFAULT_RECIPIENTS = ["caominhhieunq@gmail.com"]  # Thêm email khác nếu cần
    TO_EMAILS = recipients or DEFAULT_RECIPIENTS

    current_time = datetime.now()
    subject = f"{subject_prefix} {table_name} – {current_time.strftime('%d/%m/%Y %H:%M')}"

    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
        <h2 style="color: #d32f2f;">CẢNH BÁO HỆ THỐNG ETL WEATHER</h2>
        <p><strong>Thời gian:</strong> {current_time.strftime('%d/%m/%Y %H:%M:%S')}</p>
        <p><strong>Sự kiện:</strong> <span style="font-size: 18px; font-weight: bold; color: #d32f2f;">{table_name}</span></p>
        <hr>
        <h3>Chi tiết:</h3>
        <div style="background:#fff; padding:15px; border-left:6px solid #d32f2f; font-family: Consolas, monospace; white-space: pre-wrap;">
{error_message.replace('\n', '<br>')}
        </div>
        <br>
        <p><em>Email được gửi tự động từ Weather ETL System</em></p>
        <p>Trân trọng,<br><b>Weather Data Warehouse Team</b></p>
    </body>
    </html>
    """

    msg = MIMEMultipart("alternative")
    msg["From"] = YOUR_EMAIL
    msg["To"] = ", ".join(TO_EMAILS)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(YOUR_EMAIL, APP_PASSWORD)
            server.send_message(msg)
        log.info(f"ĐÃ GỬI EMAIL THÀNH CÔNG → {', '.join(TO_EMAILS)} | {table_name}")
        return True

    except smtplib.SMTPAuthenticationError:
        log.error("LỖI: Sai email hoặc App Password! Kiểm tra lại cấu hình Gmail.")
        return False
    except smtplib.SMTPException as e:
        log.error(f"LỖI GỬI EMAIL: {e}")
        return False
    except Exception as e:
        log.error(f"LỖI KHÔNG XÁC ĐỊNH KHI GỬI EMAIL: {e}")
        return False


if __name__ == "__main__":
    send_error_email(
        error_message="Kiểm tra gửi email thành công!\nHệ thống gửi cảnh báo đã hoạt động bình thường.",
        table_name="TEST_CONNECTION",
        subject_prefix="[TEST OK]"
    )