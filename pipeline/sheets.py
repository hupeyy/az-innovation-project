import os
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, UTC


SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


def get_sheets_client():
    """Authenticate and return a gspread client."""
    credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')

    if not credentials_json:
        raise EnvironmentError("GOOGLE_CREDENTIALS_JSON not set in environment")

    # Handle both file path and raw JSON string
    try:
        # Try parsing as raw JSON first (for GitHub Actions)
        credentials_dict = json.loads(credentials_json)
    except json.JSONDecodeError:
        # Fall back to file path (for local development)
        with open(credentials_json, 'r') as f:
            credentials_dict = json.load(f)

    creds = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    return gspread.authorize(creds)


def log_to_sheets(api_name, records_fetched, records_loaded, errors, status):
    """
    Append a pipeline run row to the Google Sheet activity log.

    Args:
        api_name:        e.g. 'weather' or 'news'
        records_fetched: number of records returned by API
        records_loaded:  number of records inserted into BigQuery
        errors:          error count for this run
        status:          'SUCCESS' or 'FAILED'
    """
    sheet_id = os.getenv('GOOGLE_SHEETS_ID')

    if not sheet_id:
        print("GOOGLE_SHEETS_ID not set - skipping Sheets logging")
        return False

    try:
        client = get_sheets_client()
        sheet = client.open_by_key(sheet_id).sheet1

        row = [
            datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC'),
            api_name.capitalize(),
            records_fetched,
            records_loaded,
            errors,
            status
        ]

        sheet.append_row(row)
        return True

    except Exception as e:
        print(f"Failed to log to Google Sheets: {e}")
        return False