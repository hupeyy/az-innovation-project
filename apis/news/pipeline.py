import json
from apis.news import config as news_config
from apis.news.fetch import fetch_news
from apis.news.parse import parse_news 


def get_api_meta():
    return {
        'api_name':  'news',
        'source_id': news_config.SOURCE_ID,
        'endpoint':  news_config.ENDPOINT,
        'table':     news_config.TABLE
    }

def fetch(api_key, logger):
    return fetch_news(api_key, logger)

def parse(data, request_id, logger):
    """
    Delegates to the actual news parsing logic and returns its 3-value tuple.
    """
    return parse_news(data, request_id, logger)

def get_raw_row(data, request_id):
    return {
        'id':         request_id,
        'request_id': request_id,
        'raw_data':   json.dumps(data)
    }