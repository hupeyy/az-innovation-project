import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
import apis.weather.config as config
from apis.weather import config as weather_config

def fetch_weather(api_key, logger):
    try:
        logger.info(
            f'Fetching weather data for '
            f'lat={config.ORLANDO_LAT}, lon={config.ORLANDO_LON}'
        )
        response = requests.get(
            f'https://api.openweathermap.org{weather_config.ENDPOINT}',
            params={
                'lat':   config.ORLANDO_LAT,
                'lon':   config.ORLANDO_LON,
                'appid': api_key,
                'units': config.UNITS
            },
            timeout=10
        )
        return response, None
    except ConnectionError as e:
        return None, f'Connection error: {e}'
    except Timeout:
        return None, 'Request timed out after 10 seconds'
    except RequestException as e:
        return None, f'Unexpected request error: {e}'