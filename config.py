import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------
# API Keys
# -----------------------------------------------------------------
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
ALPHAVANTAGE_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")

# -----------------------------------------------------------------
# GCP / BigQuery
# -----------------------------------------------------------------
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID", "az-data-pipeline")
GCP_PROJECT_ID = BIGQUERY_PROJECT_ID  # backwards compatibility
DATASET_ID = os.getenv("DATASET_ID", "az_innovation_dataset")
GOOGLE_APPLICATION_CREDENTIALS = (
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
)

# -----------------------------------------------------------------
# Google Sheets Logging
# -----------------------------------------------------------------
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")

# -----------------------------------------------------------------
# Gmail Notifications
# -----------------------------------------------------------------
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")

# -----------------------------------------------------------------
# Optional: OpenClaw / LLM
# -----------------------------------------------------------------
OPENCLAW_API_KEY = os.getenv("OPENCLAW_API_KEY")
OPENCLAW_MODEL = os.getenv("OPENCLAW_MODEL")

# -----------------------------------------------------------------
# Optional: Runtime / Workflow Config
# -----------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEFAULT_CITY = os.getenv("DEFAULT_CITY", "Phoenix")
DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "US")
BRAND_KEYWORDS = [
    kw.strip() for kw in os.getenv("BRAND_KEYWORDS", "").split(",") if kw.strip()
]
GMAIL_LABEL = os.getenv("GMAIL_LABEL", "INBOX")
CALENDAR_ID = os.getenv("CALENDAR_ID", "primary")
RECEIPT_UPLOAD_DIR = os.getenv("RECEIPT_UPLOAD_DIR", "./receipts")
EXPENSE_OUTPUT_TABLE = os.getenv("EXPENSE_OUTPUT_TABLE", "expenses_data")


def validate_env():
    required = [
        "OPENWEATHER_API_KEY",
        "NEWS_API_KEY",
        "ALPHAVANTAGE_API_KEY",
        "BIGQUERY_PROJECT_ID",
    ]

    missing = [var for var in required if not os.getenv(var)]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}"
        )

    # Optional warning for credentials file path
    creds_path = GOOGLE_APPLICATION_CREDENTIALS
    if creds_path and not os.path.exists(creds_path):
        raise EnvironmentError(
            f"Google credentials file not found at: {creds_path}"
        )