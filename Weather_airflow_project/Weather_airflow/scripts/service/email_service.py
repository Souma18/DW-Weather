from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import smtplib
# Cấu hình email gửi
SENDER_EMAIL = "22130080@st.hcmuaf.edu.vn"   # email của bạn
SENDER_PASSWORD = os.getenv("EMAIL_PASSWORD")  # lấy mật khẩu từ biến môi trường
def send_email(to_email: str, subject: str, content: str, html: bool = True):
    try:
        # Tạo message
        msg = MIMEMultipart("alternative")
        msg["From"] = SENDER_EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject
        if html:
            msg.attach(MIMEText(content, "html"))
        else:
            msg.attach(MIMEText(content, "plain"))
        # Kết nối SMTP server và gửi email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())
        return True

    except Exception as e:
        print("Error sending email:", e)
        return False

