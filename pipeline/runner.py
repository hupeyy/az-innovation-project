import json
from datetime import datetime, timezone

from config import validate_env, OPENWEATHER_API_KEY, NEWS_API_KEY
from bq.client import get_bq_client, insert_rows
from pipeline.logger import setup_logger, log_pipeline_error

logger = setup_logger('az_pipeline')

# Map each api module to its key
API_KEYS = {
    'weather': OPENWEATHER_API_KEY,
    'news':    NEWS_API_KEY,
}

def run_pipeline(api):
    # 1. Validate environment
    try:
        validate_env()
    except EnvironmentError as e:
        logger.critical(str(e))
        return

    # 2. Initialize BigQuery client
    try:
        bq = get_bq_client(logger)
    except Exception:
        return

    # 3. Resolve the correct API key from meta
    meta    = api.get_api_meta()
    api_key = API_KEYS.get(meta['api_name'])

    if not api_key:
        logger.critical(f'No API key found for api_name={meta["api_name"]}')
        return

    # 4. Fetch
    response, fetch_error = api.fetch(api_key, logger)

    if response is None:
        request_id = int(datetime.now(timezone.utc).timestamp())
        log_pipeline_error(bq, logger, fetch_error, request_id, stage='fetch')
        return

    # 5. Log the API request
    request_id  = int(datetime.now(timezone.utc).timestamp())
    api_request = {
        'id':               request_id,
        'source_id':        meta['source_id'],
        'endpoint':         meta['endpoint'],
        'timestamp':        datetime.now(timezone.utc).isoformat(),
        'http_status':      response.status_code,
        'response_time_ms': int(response.elapsed.total_seconds() * 1000)
    }

    if not insert_rows(bq, 'api_requests', [api_request], logger):
        logger.critical(
            f'Failed to insert api_request id={request_id} — aborting pipeline'
        )
        return

    # 5a. Non-200 HTTP response
    if response.status_code != 200:
        log_pipeline_error(
            bq, logger,
            f'HTTP {response.status_code}: {response.text}',
            request_id,
            stage='fetch'
        )
        return

    # 6. Decode JSON
    try:
        data = response.json()
    except ValueError as e:
        log_pipeline_error(
            bq, logger,
            f'Failed to decode JSON response: {e}',
            request_id,
            stage='parse'
        )
        return

    # 7. Insert raw data
    raw_row = api.get_raw_row(data, request_id)
    if not insert_rows(bq, 'raw_data', [raw_row], logger):
        log_pipeline_error(
            bq, logger,
            'Failed to insert raw_data',
            request_id,
            stage='insert'
        )

    # 8. Parse — handles both single row and list of rows
    parsed, parse_error = api.parse(data, request_id, logger)

    if parsed is None:
        log_pipeline_error(bq, logger, parse_error, request_id, stage='parse')
        return

    rows = parsed if isinstance(parsed, list) else [parsed]

    if not insert_rows(bq, meta['table'], rows, logger):
        log_pipeline_error(
            bq, logger,
            f'Failed to insert into {meta["table"]}',
            request_id,
            stage='insert'
        )
        return

    logger.info(
        f'Pipeline completed successfully | '
        f'api={meta["api_name"]} | '
        f'request_id={request_id} | '
        f'rows={len(rows)}'
    )