import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from datetime import datetime
from typing import List, Optional
import logging
from dotenv import load_dotenv
load_dotenv()

# Cấu hình logging
log = logging.getLogger(__name__)

# Cấu hình email gửi (giữ nguyên theo mẫu của bạn)
SENDER_EMAIL = "caominhhieunq@gmail.com" 
SENDER_PASSWORD = os.getenv("SENDER_PASSWORD") 
DEFAULT_RECIPIENTS = [SENDER_EMAIL]


def send_email(
    to_email: str,
    subject: str,
    content: str,
    html: bool = True,
    # Thêm các tham số tùy chọn cho email báo lỗi ETL
    error_message: Optional[str] = None,
    table_name: str = "Unknown",
    subject_prefix: str = "[ETL ERROR]"
):
    try:
        # Xử lý danh sách người nhận
        if isinstance(to_email, str):
            recipients = [to_email]
        else:
            recipients = list(to_email)
            
        # Nếu có lỗi, tạo nội dung HTML báo lỗi đặc thù
        if error_message:
            recipients = recipients or DEFAULT_RECIPIENTS
            now = datetime.now()
            final_subject = f"{subject_prefix} {table_name} - {now.strftime('%d/%m/%Y %H:%M')}"
            html = True # Báo lỗi luôn dùng HTML

            final_content = f"""
            <html>
                <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background: #f8f9fa;">
                    <div style="max-width: 700px; margin: auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                        
                        <div style="background: #b71c1c; color: white; padding: 20px; text-align: center;">
                            <h1 style="margin: 0; font-size: 22px;">CẢNH BÁO LỖI HỆ THỐNG ETL WEATHER</h1>
                        </div>

                        <div style="padding: 25px;">
                            <p><strong>Thời gian:</strong> {now.strftime('%d/%m/%Y %H:%M:%S')}</p>
                            <p><strong>Bảng:</strong> <span style="font-size: 20px; font-weight: bold; color: #b71c1c;">{table_name}</span></p>

                            <hr style="border: 1px solid #eee;">

                            <h3 style="color: #b71c1c;">Chi tiết lỗi:</h3>
                            <div style="background:#f5f5f5; padding:18px; border-left:6px solid #b71c1c;
                                         font-family: Consolas, monospace; white-space: pre-wrap; border-radius: 6px;
                                         overflow-x: auto;">
{error_message.replace('\n', '<br>')}
                            </div>

                            <br>
                            <p style="color: #666; font-size: 14px;">
                                <em>Email này được gửi tự động từ hệ thống Weather ETL.</em>
                            </p>

                            <p style="margin-top: 30px;">
                                Trân trọng,<br>
                                <strong>Weather Data Warehouse Team</strong>
                            </p>
                        </div>

                        <div style="background: #333; color: #aaa; text-align: center; padding: 15px; font-size: 12px;">
                            © 2025 Weather ETL System – Tự động giám sát 24/7
                        </div>
                    </div>
                </body>
            </html>
            """
        else:
            final_subject = subject
            final_content = content
        
        # Tạo message
        msg = MIMEMultipart("alternative")
        msg["From"] = SENDER_EMAIL
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = final_subject

        if html:
            msg.attach(MIMEText(final_content, "html"))
        else:
            msg.attach(MIMEText(final_content, "plain"))

        # Kết nối SMTP server và gửi email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())

        log.info(f"Gửi email thành công → {', '.join(recipients)}")
        return True

    except Exception as e:
        log.error(f"Lỗi gửi email: {e}")
        print("Error sending email:", e)
        return False

if __name__ == "__main__":
    send_email(
        to_email=DEFAULT_RECIPIENTS,
        subject="Chủ đề mặc định (sẽ bị ghi đè)",
        content="Nội dung này không được dùng khi gửi báo lỗi.",
        error_message="""KeyError: 'city_id' not found in the source data.
Please check the API response structure.
""",
        table_name="WEATHER_FACT",
        subject_prefix="[FATAL ERROR]"
    )