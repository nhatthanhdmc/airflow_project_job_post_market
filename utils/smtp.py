import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import config as cfg

# SMTP settings    
class EmailSender:
    def __init__(self, smtp_server: str, smtp_port: int, sender_email: str, sender_password: str):
        self.smtp_server = smtp_server or cfg.smtp["gmail"]["smtp_server"]
        self.smtp_port = smtp_port or cfg.smtp["gmail"]["smtp_port"]
        self.sender_email = sender_email or cfg.smtp["gmail"]["sender_email"]
        self.sender_password = sender_password or cfg.smtp["gmail"]["sender_password"]

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