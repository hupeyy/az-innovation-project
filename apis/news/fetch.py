import requests
from requests.exceptions import ConnectionError, Timeout, RequestException
from apis.news import config as news_config

def fetch_news(api_key, logger):
    try:
        logger.info(
            f'Fetching news | '
            f'country={news_config.COUNTRY} '
            f'category={news_config.CATEGORY}'
        )
        response = requests.get(
            f'{news_config.BASE_URL}{news_config.ENDPOINT}',
            params={
                'country':  news_config.COUNTRY,
                'category': news_config.CATEGORY,
                'pageSize': news_config.PAGE_SIZE,
                'apiKey':   api_key,
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