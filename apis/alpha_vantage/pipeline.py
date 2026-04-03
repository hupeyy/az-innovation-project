import json
from apis.alpha_vantage import config as av_config
from apis.alpha_vantage.fetch import fetch_stock
from apis.alpha_vantage.parse import parse_stock

def get_api_meta():
    return {
        'api_name':  'alpha_vantage',
        'source_id': av_config.SOURCE_ID,
        'endpoint':  av_config.ENDPOINT,
        'table':     av_config.TABLE
    }

def fetch(api_key, logger):
    return fetch_stock(api_key, logger)

def parse(data, request_id, logger):
    return parse_stock(data, request_id, logger)

def get_raw_row(data, request_id):
    return {
        'id':         request_id,
        'request_id': request_id,
        'raw_data':   json.dumps(data)
    }