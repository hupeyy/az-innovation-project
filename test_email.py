from dotenv import load_dotenv
load_dotenv()

from pipeline.notifications import send_email_alert

send_email_alert(
    "Test Alert",
    "This is a test of the pipeline notification system",
    {"test": "data", "status": "working"}
)
