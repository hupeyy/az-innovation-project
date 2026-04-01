import json
from apis.weather import config as weather_config
from apis.weather.fetch import fetch_weather
from apis.weather.parse import parse_weather

def get_api_meta():
    return {
        'api_name':  'weather',
        'source_id': weather_config.SOURCE_ID,
        'endpoint':  weather_config.ENDPOINT,
        'table':     weather_config.TABLE
    }

def fetch(api_key, logger):
    return fetch_weather(api_key, logger)

def parse(data, request_id, logger):
    return parse_weather(data, request_id, logger)

def get_raw_row(data, request_id):
    return {
        'id':         data['dt'],
        'request_id': request_id,
        'raw_data':   json.dumps(data)
    }