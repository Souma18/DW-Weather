
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Optional
import logging

log = logging.getLogger(__name__)

SENDER_EMAIL = "caominhhieunq@gmail.com"
SENDER_PASSWORD = "dioefdvjhtgzihzq"  

DEFAULT_RECIPIENTS = ["caominhhieunq@gmail.com"]


def send_email(to_email: str, subject: str, content: str, html: bool = True):
    if isinstance(to_email, str):
        recipients = [to_email]
    else:
        recipients = list(to_email)

    msg = MIMEMultipart("alternative")
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    if html:
        msg.attach(MIMEText(content, "html"))
    else:
        msg.attach(MIMEText(content, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
            
        log.info(f"Gửi email thành công → {', '.join(recipients)}")
        return True
    except Exception as e:
        log.error(f"Lỗi gửi email: {e}")
        print("Error sending email:", e)
        return False


def send_error_email(
    error_message: str,
    table_name: str = "Unknown",
    recipients: Optional[List[str]] = None,
    subject_prefix: str = "[ETL ERROR]"
) -> bool:
    recipients = recipients or DEFAULT_RECIPIENTS
    now = datetime.now()

    subject = f"{subject_prefix} {table_name} - {now.strftime('%d/%m/%Y %H:%M')}"

    html_content = f"""
    <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; margin: 0; padding: 20px; background: #f8f9fa;">
            <div style="max-width: 700px; margin: auto; background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.1);">
                <div style="background: #d32f2f; color: white; padding: 20px; text-align: center;">
                    <h1 style="margin: 0; font-size: 24px;">CẢNH BÁO HỆ THỐNG ETL WEATHER</h1>
                </div>
                <div style="padding: 25px;">
                    <p><strong>Thời gian:</strong> {now.strftime('%d/%m/%Y %H:%M:%S')}</p>
                    <p><strong>Sự kiện:</strong> <span style="font-size: 20px; font-weight: bold; color: #d32f2f;">{table_name}</span></p>
                    <hr style="border: 1px solid #eee;">
                    <h3 style="color: #d32f2f;">Chi tiết:</h3>
                    <div style="background:#f5f5f5; padding:18px; border-left:6px solid #d32f2f; 
                                font-family: Consolas, monospace; white-space: pre-wrap; border-radius: 6px; 
                                overflow-x: auto;">
{error_message.replace('\n', '<br>')}
                    </div>
                    <br>
                    <p style="color: #666; font-size: 14px;">
                        <em>Email được gửi tự động từ Weather ETL System</em>
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

    return send_email(recipients, subject, html_content, html=True)


if __name__ == "__main__":
    send_error_email(
        error_message="Test template sạch sẽ không icon thành công.",
        table_name="TEST FINAL",
        subject_prefix="[TEST OK]"
    )