import os
from dotenv import load_dotenv

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
NEWS_API_KEY        = os.getenv('NEWS_API_KEY')
GCP_PROJECT_ID      = os.getenv('GCP_PROJECT_ID')
DATASET_ID          = 'az_innovation_data'

def validate_env():
    required = [
        'OPENWEATHER_API_KEY',
        'NEWS_API_KEY',
        'GCP_PROJECT_ID',
    ]
    missing = [var for var in required if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f'Missing required environment variables: {missing}')