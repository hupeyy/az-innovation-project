# AZ Data Pipeline

An automated data pipeline that fetches weather, news, and stock market data from public APIs, loads it into Google BigQuery, logs activity to Google Sheets, and sends email alerts on failure.

---

## Architecture

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   OpenWeather   │     │     NewsAPI     │     │  Alpha Vantage  │ 
│       API       │     │       API       │     │       API       │
└────────┬────────┘     └────────┬────────┘     └─────────┬───────┘
         │                       │                        │
         └──────────────┬─────────────────────────────────┘
                        │
                 ┌──────▼──────┐
                 │  runner.py  │
                 │ (pipeline   │
                 │orchestrator)|
                 └──────┬──────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
 ┌────────▼─────┐  ┌────▼───────┐  ┌─▼──────────────┐
 │   BigQuery   │  │ Google     │  │ Gmail Alerts   │
 │              │  │ Sheets     │  │ (errors only)  │
 │ api_requests │  │ Activity   │  └────────────────┘
 │ raw_data     │  │ Log        │
 │ weather_data │  └────────────┘
 │ news_data    │
 │ stock_data   │
 │ api_errors   │
 │ extracted_   │
 │ entities     │
 └──────────────┘
         ▲
         │
┌────────┴────────┐
│ GitHub Actions  │
│ (every 60 min)  │
└─────────────────┘


az-innovation-project/
├── .github/
│   └── workflows/
│       └── pipeline.yml
├── apis/
│   ├── weather/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── fetch.py
│   │   ├── parse.py
│   │   └── pipeline.py
│   ├── news/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── fetch.py
│   │   ├── parse.py
│   │   └── pipeline.py
│   └── alpha_vantage/
│       ├── __init__.py
│       ├── config.py
│       ├── fetch.py
│       ├── parse.py
│       └── pipeline.py
├── bq/
│   ├── __init__.py
│   ├── client.py
│   ├── schema.sql
│   └── setup_bq.py
├── pipeline/
│   ├── __init__.py
│   ├── logger.py
│   ├── notifications.py
│   ├── runner.py
│   └── sheets.py
├── tests/
│   ├── __init__.py
│   ├── test_alpha_vantage.py
│   ├── test_news.py
│   ├── test_notifications.py
│   ├── test_runner.py
│   ├── test_setup_bq.py
│   └── test_weather.py
├── config.py
├── main.py
├── .env.example
├── requirements.txt
└── README.md

## Features
- Fetches weather data from OpenWeather API
- Fetches article data from NewsAPI
- Loads structured and raw records into BigQuery
- Logs pipeline errors to a dedicated api_errors table
- Sends Gmail notifications when pipeline runs fail
- Logs pipeline activity to a public Google Sheet
- Runs automatically on a schedule with GitHub Actions
- Includes pytest coverage for APIs, notifications, setup, and orchestrationFetches current weather data from the OpenWeather API
- Fetches top technology news articles from NewsAPI
- Fetches real-time stock quotes from Alpha Vantage API
- Loads structured and raw records into Google BigQuery
- Logs pipeline errors to a dedicated api_errors table
- Stores extracted entities (sources, symbols) in a shared extracted_entities table
- Sends Gmail notifications when pipeline runs fail
- Logs pipeline activity to a public Google Sheet
- Runs automatically on a schedule with GitHub Actions
- Includes full pytest coverage for all APIs, notifications, setup, and orchestration


## BigQuery Schema
The pipeline writes to the following tables:

api_requests — request metadata including endpoint, status code, and response time
raw_data — full raw API payloads for debugging and replay
weather_data — parsed weather records
news_data — parsed news article records
stock_data	Parsed stock quote records from Alpha Vantage
extracted_entities	Generic entity table for sources, symbols, and categories across all APIs
api_errors — errors by pipeline stage (fetch, parse, insert, etc.)

## Free Tier Limits
- BigQuery: 10GB storage, 1TB queries/month
- NewsAPI: 100 requests/day
- OpenWeather: 1,000 requests/day
- Alpha Vantage: 25 requests/day, 5 requests/minute
- GitHub Actions: 2,000 minutes/month
- Gmail SMTP: ~500 emails/day
- Google Sheets API: generous free tier for this project's usage

Note: Alpha Vantage returns HTTP 200 OK even when rate limits are reached. The pipeline handles this by checking the response body for Note or Error Message keys and logging them to api_errors instead of attempting to insert invalid data.

# Setup Instructions
## Prerequisites
- Python 3.11+
- Google Cloud project with BigQuery enabled
- OpenWeather API key
- NewsAPI key
- Alpha Vantage API key
- Gmail account with 2-Step Verification enabled

1. Clone the repository
```bash
git clone https://github.com/your-username/az-innovation-project.git
cd az-innovation-project
```

1. Create and activate a virtual environment

### Windows

```bash
python -m venv venv
venv\Scripts\activate
```

### Mac/Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```
4. Create environment file
Create a .env file in the root of the project and add:

```bash
OPENWEATHER_API_KEY=your_openweather_key
NEWS_API_KEY=your_newsapi_key

BIGQUERY_PROJECT_ID=your_gcp_project_id
BIGQUERY_DATASET_ID=your_bigquery_dataset

GOOGLE_CREDENTIALS_JSON=path/to/your/service_account_key.json
GOOGLE_SHEETS_ID=your_google_sheet_id

GMAIL_ADDRESS=your_email@gmail.com
GMAIL_APP_PASSWORD=your_16_character_app_password
ALERT_EMAIL=your_email@gmail.com
```

5. Set up BigQuery tables
```bash
python bq/setup_bq.py
``` 

6. Run the pipeline locally
```bash
python main.py
```

7. Run tests
```bash
pytest tests/ -v
```

## GitHub Actions Setup
The pipeline is automated using GitHub Actions. Add the following repository secrets:

- OPENWEATHER_API_KEY
- NEWS_API_KEY
- ALPHAVANTAGE_API_KEY	
- BIGQUERY_PROJECT_ID
- BIGQUERY_DATASET_ID
- GOOGLE_CREDENTIALS_JSON
- GOOGLE_SHEETS_ID
- GMAIL_ADDRESS