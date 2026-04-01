from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import requests
import json
from google.cloud import bigquery

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = 'az_innovation_data'
UNITS = 'imperial'

client = bigquery.Client(project=GCP_PROJECT_ID)

def insert_rows(table_name, rows):
    table_ref = f'{GCP_PROJECT_ID}.{DATASET_ID}.{table_name}'
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f'Error inserting into {table_name}: {errors}')
    else:
        print(f'Successfully inserted into {table_name}')

# --- Fetch weather data ---
response = requests.get(
    'https://api.openweathermap.org/data/2.5/weather',
    params={
        'lat': 28.59,
        'lon': -81.38,
        'appid': OPENWEATHER_API_KEY,
        'units': UNITS
    }
)

if response.status_code == 200:
    data = response.json()

    # 1. Insert api_requests first to get request_id
    api_request = {
        'id': data['dt'],  # using Unix timestamp as unique id
        'source_id': 1,
        'endpoint': '/data/2.5/weather',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'http_status': response.status_code,
        'response_time_ms': int(response.elapsed.total_seconds() * 1000)
    }
    insert_rows('api_requests', [api_request])
    request_id = api_request['id']

    # 2. Insert weather_data
    weather_data = {
        'id': data['id'],  # OpenWeather city id
        'request_id': request_id,
        'city_name': data['name'],
        'country': data['sys']['country'],
        'units': UNITS,
        'latitude': data['coord']['lat'],
        'longitude': data['coord']['lon'],
        'temp_min': data['main']['temp_min'],
        'temp_max': data['main']['temp_max'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'sunrise': datetime.fromtimestamp(data['sys']['sunrise'], tz=timezone.utc).isoformat(),
        'sunset': datetime.fromtimestamp(data['sys']['sunset'], tz=timezone.utc).isoformat(),
    }
    insert_rows('weather_data', [weather_data])

    # 3. Insert raw_data
    raw_data = {
        'id': data['dt'],
        'request_id': request_id,
        'raw_data': json.dumps(data)  # BigQuery stores as STRING
    }
    insert_rows('raw_data', [raw_data])

else:
    # Insert api_errors
    error = {
        'id': int(datetime.now(timezone.utc).timestamp()),
        'request_id': 0,
        'error_message': f'HTTP {response.status_code}: {response.text}',
        'timestamp': datetime.now(timezone.utc).isoformat()
    }
    insert_rows('api_errors', [error])