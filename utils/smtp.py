import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List

class EmailSender:
    def __init__(self, smtp_server: str, smtp_port: int, sender_email: str, sender_password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password

    def send_email(self, subject: str, body: str, recipients: List[str]):
        # Tạo MIME message
        msg = MIMEMultipart()
        msg['From'] = self.sender_email
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        try:
            # Thiết lập kết nối SMTP và gửi email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # Bắt đầu TLS cho bảo mật
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipients, msg.as_string())
            print("Email sent successfully!")
        except Exception as e:
            print(f"Failed to send email: {e}")

# Cấu hình EmailSender
email_sender = EmailSender(
    smtp_server='smtp.gmail.com',
    smtp_port=587,
    sender_email='briannguyen1192@gmail.com',
    sender_password='Thanh@2495' 
)
