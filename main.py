from dotenv import load_dotenv
from datetime import datetime, timezone
import os
import requests
import json

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
UNITS = 'imperial'

response = requests.get(
    'https://api.openweathermap.org/data/2.5/weather',
    params={
        'lat': 44.34,
        'lon': 10.99,
        'appid': OPENWEATHER_API_KEY,
        'units': UNITS
    }
)

if response.status_code == 200:
    data = response.json()

    # Matches weather_data table schema
    weather_data = {
        'city_name': data['name'],
        'country': data['sys']['country'],
        'units': UNITS,
        'latitude': data['coord']['lat'],
        'longitude': data['coord']['lon'],
        'temp_min': data['main']['temp_min'],
        'temp_max': data['main']['temp_max'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed'],
        'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).isoformat(),
        'sunset': datetime.fromtimestamp(data['sys']['sunset']).isoformat(),
    }

    # Matches api_requests table schema
    api_request = {
        'source_id': 1,  # assumes openweather is id=1 in api_sources
        'endpoint': '/data/2.5/weather',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'http_status': response.status_code,
        'response_time_ms': int(response.elapsed.total_seconds() * 1000)
    }

    # Matches raw_data table schema
    raw_data = {
        'raw_data': data
    }

    print(json.dumps(weather_data, indent=2))
    print(json.dumps(api_request, indent=2))
    print(json.dumps(raw_data, indent=2))

else:
    # Matches api_errors table schema
    error = {
        'error_message': f'HTTP {response.status_code}: {response.text}',
        'timestamp': datetime.now(datetime.UTC).isoformat()
    }
    print(json.dumps(error, indent=2))