import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

def send_status_email(subject: str, body: str, to_email: str = "capstoneproject196@gmail.com") -> Dict[str, Any]:
    """
    Sends an email containing pipeline health and anomaly prediction status.
    """
    try:
        # We will set these in app.yaml
        sender_email = os.getenv("SMTP_USER", "capstoneproject196@gmail.com") 
        sender_password = os.getenv("SMTP_PASSWORD") 

        if not sender_password:
            raise ValueError("SMTP_PASSWORD environment variable is missing. Cannot send email.")

        # Construct the email
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = to_email
        msg['Subject'] = subject

        # Attach the body (allowing HTML so the AI agent can make it look nice)
        msg.attach(MIMEText(body, 'html'))

        # Connect to Gmail's SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Secure the connection
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()

        return {
            "status": "success", 
            "message": f"Email successfully sent to {to_email} with subject: '{subject}'"
        }
        
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return {"status": "failed", "error": str(e)}