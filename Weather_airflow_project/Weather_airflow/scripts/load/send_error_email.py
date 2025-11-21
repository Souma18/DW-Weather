import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging

log = logging.getLogger(__name__) 
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')

def send_error_email(error_message: str, table_name: str = "Unknown"):
    """
    Gửi email cảnh báo lỗi khi quá trình ETL load vào BigQuery thất bại.
    """
    # CẤU HÌNH GMAIL SMTP
    YOUR_EMAIL = "caominhhieunq@gmail.com"           # Thay bằng EMAIL GMAIL CỦA BẠN
    APP_PASSWORD = "dioefdvjhtgzihzq"               # Thay bằng App Password (16 ký tự)
    # ====================

    TO_EMAIL = YOUR_EMAIL 

    # Định dạng nội dung email
    current_time = datetime.now().strftime('%d/%m/%Y %H:%M')
    subject = f"[ETL ERROR] Bảng {table_name} load thất bại – {current_time}"

    body = f"""
    <h2 style="color:red;">CẢNH BÁO LỖI ETL LOAD TO BIGQUERY</h2>
    <p><strong>Thời gian:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
    <p><strong>Bảng bị lỗi:</strong> <span style="font-size:18px;font-weight:bold;color:red;">{table_name}</span></p>
    <hr>
    <h3>Chi tiết lỗi:</h3>
    <div style="background:#f8f8f8;padding:15px;border-left:6px solid red;font-family:consolas;">
{error_message.replace('\n', '<br>')}
    </div>
    <br>
    <p><i>Hệ thống tự động phát hiện lỗi và gửi cảnh báo.</i></p>
    <p>Trân trọng,<br><b>Weather ETL System</b></p>
    """

    msg = MIMEMultipart()
    msg["From"] = YOUR_EMAIL
    msg["To"] = TO_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))

    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(YOUR_EMAIL, APP_PASSWORD)
        server.send_message(msg)
        server.quit()
        log.info("ĐÃ GỬI EMAIL CẢNH BÁO THÀNH CÔNG!")
    except Exception as e:
        log.warning(f"Không gửi được email cảnh báo: {e}")

if __name__ == "__main__":
    send_error_email(
        table_name="TEST_TABLE_SCHEMA_CHECK",
        error_message="Kiểm tra gửi email thành công: Kết nối SMTP đã hoạt động."
    )