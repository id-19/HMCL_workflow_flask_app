import smtplib
from email.mime.text import MIMEText
from airflow.sdk import dag, task, Param
from datetime import timedelta, datetime
from typing import Dict

@dag(
    dag_id="dynamic_email_sender",
    schedule=None,
    # start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["email"],
    params={
        "sender_email": Param("Sender email address", type=str),
        "receiver_email": Param("Receiver email address", type=str),
        "email_subject": Param("Email subject line", type=str),
        "email_content": Param("Email body content", type=str),
        "app_password": Param("Authentication password", type=str),
        "smtp_server": Param("smtp.gmail.com", type=str),
        "smtp_port": Param(465, type=int)
    }
)
def email_sender_dag():
    
    @task(retries=3, retry_delay=timedelta(minutes=2))
    def send_email(**context) -> None:
        # Access execution context correctly
        dag_run = context["dag_run"]
        params: Dict = dag_run.conf if dag_run else {}
        
        # Validate required parameters
        required_params = {
            "sender_email": "Sender email address",
            "receiver_email": "Receiver email address",
            "email_subject": "Email subject line",
            "email_content": "Email body content",
            "app_password": "Authentication password"
        }
        
        missing = [name for name in required_params if name not in params]
        if missing:
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")
        
        # Extract parameters
        sender = params["sender_email"]
        recipient = params["receiver_email"]
        subject = params["email_subject"]
        content = params["email_content"]
        app_password = params["app_password"]
        
        # Extract optional parameters with defaults
        smtp_server = params.get("smtp_server", "smtp.gmail.com")
        smtp_port = params.get("smtp_port", 465)
        
        # Create and send email
        msg = MIMEText(content)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = recipient
        
        try:
            with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
                server.login(sender, app_password)
                server.sendmail(sender, recipient, msg.as_string())
                print(f"Email successfully sent to {recipient}")
        except Exception as e:
            print(f"Email sending failed: {e}")
            raise
    
    send_email()

email_sender_dag()
