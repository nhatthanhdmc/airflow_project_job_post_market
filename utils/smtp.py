import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import config as cfg
import pendulum
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# SMTP settings    
class EmailSender:
    def __init__(self, smtp_server: str, smtp_port: int, sender_email: str, sender_password: str):
        self.smtp_server = smtp_server or cfg.smtp["gmail"]["smtp_server"]
        self.smtp_port = smtp_port or cfg.smtp["gmail"]["smtp_port"]
        self.sender_email = sender_email or cfg.smtp["gmail"]["sender_email"]
        self.sender_password = sender_password or cfg.smtp["gmail"]["sender_password"]

    def send_email(self, subject: str, body: str, recipients: List[str], context: dict, is_success: int):
        # Task info
        task_instance = context.get('task_instance')
        dag_id = context.get('dag').dag_id
        task_id = task_instance.task_id
        execution_date=str(local_tz.convert(context.get('execution_date'))),
        log_url = task_instance.log_url    
        exception = context.get('exception')
        
        # Contain
        recipients = ['nguyentuancong.hcm@gmail.com']
        if is_success == 1:            
            subject = f"Task {task_id} (DAG {dag_id}) was SUCCESS"
        else:
            subject = f"Task {task_id} (DAG {dag_id}) was FAIL"
        
        if exception:
            body = f"Dear you,\nExecution time: {execution_date} \nLog URL: {log_url}\nBest regards\n"
        else:
            body = f"Dear you,\nExecution time: {execution_date} \nException: {exception}\nLog URL: {log_url}\nBest regards\n"
        
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