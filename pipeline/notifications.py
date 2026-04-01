import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, UTC


def send_email_alert(subject, message, error_details=None):
    """
    Send pipeline alerts via Gmail.
    
    Args:
        subject: Email subject line
        message: Main alert message
        error_details: Additional error context (dict)
    """
    sender_email = os.getenv('GMAIL_ADDRESS')
    sender_password = os.getenv('GMAIL_APP_PASSWORD')
    recipient_email = os.getenv('ALERT_EMAIL', sender_email)  # Default to self
    
    if not sender_email or not sender_password:
        print(f"Email not configured - skipping notification: {message}")
        return False
    
    # Create message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = f"[Pipeline Alert] {subject}"
    
    # Build email body
    body = f"""
    Pipeline Alert
    ==============
    Time: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}
    
    {message}
    """
    
    if error_details:
        body += "\n\nError Details:\n"
        for key, value in error_details.items():
            body += f"  {key}: {value}\n"
    
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        # Connect to Gmail
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        
        # Send email
        server.send_message(msg)
        server.quit()
        
        return True
    except Exception as e:
        print(f"Failed to send email alert: {e}")
        return False


def send_pipeline_summary(api_name, records_loaded, errors, duration):
    """Send summary email after pipeline completes."""
    
    status = "✅ SUCCESS" if errors == 0 else "⚠️ COMPLETED WITH ERRORS"
    subject = f"{api_name} Pipeline: {status}"
    
    message = f"""
    Pipeline: {api_name}
    Status: {status}
    Records Loaded: {records_loaded}
    Errors: {errors}
    Duration: {duration:.2f} seconds
    """
    
    send_email_alert(subject, message)