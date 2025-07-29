import smtplib
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv
load_dotenv()

def send_plain_text_email(sender_email: str, receiver_email: str, email_subject: str, email_content: str, app_password: str, smtp_server = os.getenv("SMTP_SERVER") or "smtp-relay.gmail.com", smtp_port = int(os.getenv("SMTP_PORT") or 25)):
    """
    Sends a plain text email using the provided parameters.

    Args:
        sender_email (str): The email address of the sender.
        receiver_email (str): The email address of the receiver.
        email_subject (str): The subject of the email.
        email_content (str): The body content of the email.
        app_password (str): The application-specific password for the sender's email account.
        smtp_server (str, optional): The SMTP server address. Defaults to "smtp.gmail.com".
        smtp_port (int, optional): The SMTP port. Defaults to 465.
    """
    # Create the email message
    msg = MIMEText(email_content)
    msg['Subject'] = email_subject
    msg['From'] = sender_email
    msg['To'] = receiver_email

    try:
        # Connect to the SMTP server and send the email
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print(f"Email successfully sent to {receiver_email}")
    except Exception as e:
        print(f"Email sending failed: {e}")
        raise

if __name__ == "__main__":
    # --- IMPORTANT --- #
    # Replace the placeholder values below with your actual credentials and email content.
    # For security reasons, it's recommended to use environment variables or a config file
    # to store sensitive information like passwords, rather than hardcoding them in the script.

    SENDER_EMAIL = "ishan.datta.interns@heromotocorp.com"
    APP_PASSWORD = os.getenv("APP_PASSWORD")  # Use an app-specific password if using Gmail
    assert APP_PASSWORD is not None, "APP_PASSWORD environment variable is not set"
    RECEIVER_EMAIL = "ishan.datta.interns@heromotocorp.com"

    EMAIL_SUBJECT = "Test Email from Python Script"
    EMAIL_CONTENT = "This is a test email sent from a standalone Python script."

    send_plain_text_email(
        sender_email=SENDER_EMAIL,
        receiver_email=RECEIVER_EMAIL,
        email_subject=EMAIL_SUBJECT,
        email_content=EMAIL_CONTENT,
        app_password=APP_PASSWORD
    )
