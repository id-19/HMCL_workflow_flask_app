from typing import Any
from pydantic import BaseModel
from datetime import datetime, timedelta

class ExecuteDagRequest(BaseModel):
    dag_id: str
    wait_for_termination: bool = False
    conf: Any
    start_date: datetime = datetime.now()
    end_date: datetime = datetime.now()
    schedule_interval: timedelta = timedelta(days=1)

class ExecuteDatabaseWorkflowRequest(BaseModel):
    sap_db_name: str
    sap_db_user: str
    sap_db_password: str
    sap_db_host: str
    sap_db_port: int
    sap_db_schema: Any

class ExecuteMailWorkflowRequest(BaseModel):
    sender_email: str
    receiver_email: str
    email_subject: str
    email_content: str
    auth_done: bool = True
    app_password: str | None = None
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 465

class AuthenticationRequest(BaseModel):
    email: str
    user_flow: bool # Whether we should open an OAuth login window or not
    direct_auth: bool # Whether we should use the app password provided directly
    