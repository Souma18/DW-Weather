import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Lấy API KEY từ biến môi trường
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")


def send_email(to_email: str, subject: str, content: str):
    message = Mail(
        from_email="youremail@domain.com",    # Email của bạn
        to_emails=to_email,
        subject=subject,
        html_content=content                   # Có thể là plain text hoặc HTML
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)

        # Success
        return response.status_code in [200, 202]

    except Exception as e:
        print("Error sending email:", e)
        return False
