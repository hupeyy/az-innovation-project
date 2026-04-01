# pipeline/runner.py
import json
from datetime import datetime, timezone

from config import validate_env, OPENWEATHER_API_KEY   # root config
from bq.client import get_bq_client, insert_rows
from pipeline.logger import setup_logger, log_pipeline_error

logger = setup_logger('weather_pipeline')

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

    # 3. Fetch
    response, fetch_error = api.fetch(OPENWEATHER_API_KEY, logger)

    # 3a. Request failed entirely
    if response is None:
        request_id = int(datetime.now(timezone.utc).timestamp())
        log_pipeline_error(bq, logger, fetch_error, request_id, stage='fetch')
        return

    # 4. Log the API request — meta comes from the api module
    meta       = api.get_api_meta()
    request_id = int(datetime.now(timezone.utc).timestamp())
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

    # 4a. Non-200 HTTP response
    if response.status_code != 200:
        log_pipeline_error(
            bq, logger,
            f'HTTP {response.status_code}: {response.text}',
            request_id,
            stage='fetch'
        )
        return

    # 5. Parse JSON
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

    # 6. Insert raw data — delegated to api module
    raw_row = api.get_raw_row(data, request_id)
    if not insert_rows(bq, 'raw_data', [raw_row], logger):
        log_pipeline_error(
            bq, logger,
            'Failed to insert raw_data',
            request_id,
            stage='insert'
        )

    # 7. Parse and insert — delegated to api module
    parsed_row, parse_error = api.parse(data, request_id, logger)

    if parsed_row is None:
        log_pipeline_error(bq, logger, parse_error, request_id, stage='parse')
        return

    # table name comes from meta so the runner doesn't hardcode 'weather_data'
    if not insert_rows(bq, meta['table'], [parsed_row], logger):
        log_pipeline_error(
            bq, logger,
            f'Failed to insert into {meta["table"]}',
            request_id,
            stage='insert'
        )

    logger.info(f'Pipeline completed successfully | request_id={request_id}')