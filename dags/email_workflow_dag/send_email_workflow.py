from airflow.sdk import dag, task, Param
from datetime import datetime
from typing import Dict
import os
from send_email_script import send_plain_text_email

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
        "smtp_server": Param("smtp-relay.gmail.com", type=str),
        "smtp_port": Param(25, type=int)
    }
)
def email_sender_dag():
    
    @task(retries=3, retry_delay=300)  # 5 minutes in seconds
    def send_email(**context) -> None:
        # Access execution context
        dag_run = context["dag_run"]
        params: Dict = dag_run.conf if dag_run else {}
        
        # Validate required parameters
        required_params = [
            "sender_email",
            "receiver_email",
            "email_subject",
            "email_content",
            "app_password"
        ]
        
        missing = [name for name in required_params if name not in params]
        if missing:
            raise ValueError(f"Missing required parameters: {', '.join(missing)}")
        
        # Extract SMTP parameters with defaults from environment or use function defaults
        smtp_server = params.get("smtp_server", os.getenv("SMTP_SERVER", "smtp-relay.gmail.com"))
        smtp_port = int(params.get("smtp_port", os.getenv("SMTP_PORT", "25")))
        
        # Call the imported function
        send_plain_text_email(
            sender_email=params["sender_email"],
            receiver_email=params["receiver_email"],
            email_subject=params["email_subject"],
            email_content=params["email_content"],
            app_password=params["app_password"],
            smtp_server=smtp_server,
            smtp_port=smtp_port
        )
    
    send_email()

email_sender_dag()
