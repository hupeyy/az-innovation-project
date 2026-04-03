# fetch.py
import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from apis.alpha_vantage import config as av_config

def fetch_stock(api_key, logger):
    try:
        logger.info(f'Fetching stock data for {av_config.SYMBOL}')
        response = requests.get(
            f'{av_config.BASE_URL}{av_config.ENDPOINT}',
            params={
                'function': av_config.FUNCTION,
                'symbol':   av_config.SYMBOL,
                'apikey':   api_key,
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