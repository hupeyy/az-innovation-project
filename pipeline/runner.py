import json
import time
from datetime import datetime, timezone

from config import validate_env, OPENWEATHER_API_KEY, NEWS_API_KEY
from bq.client import get_bq_client, insert_rows
from pipeline.logger import setup_logger, log_pipeline_error
from pipeline.notifications import send_email_alert, send_pipeline_summary

logger = setup_logger('az_pipeline')

# Map each api module to its key
API_KEYS = {
    'weather': OPENWEATHER_API_KEY,
    'news':    NEWS_API_KEY,
}

def run_pipeline(api):
    start_time = time.time()
    error_count = 0
    records_loaded = 0
    api_name = None

    try:
        # 1. Validate environment
        try:
            validate_env()
        except EnvironmentError as e:
            logger.critical(str(e))
            send_email_alert("Configuration Error", str(e))
            return

        # 2. Initialize BigQuery client
        try:
            bq = get_bq_client(logger)
        except Exception as e:
            send_email_alert("BigQuery Connection Failed", str(e))
            return

        # 3. Resolve the correct API key from meta
        meta     = api.get_api_meta()
        api_name = meta['api_name']
        api_key  = API_KEYS.get(api_name)

        if not api_key:
            error_msg = f'No API key found for api_name={api_name}'
            logger.critical(error_msg)
            send_email_alert(f"Missing API Key - {api_name}", error_msg)
            return

        # 4. Fetch
        response, fetch_error = api.fetch(api_key, logger)

        if response is None:
            request_id = int(datetime.now(timezone.utc).timestamp())
            log_pipeline_error(bq, logger, fetch_error, request_id, stage='fetch')
            send_email_alert(
                f"{api_name.upper()} Fetch Failed",
                fetch_error,
                {"request_id": request_id, "stage": "fetch"}
            )
            error_count += 1
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
            error_msg = f'Failed to insert api_request id={request_id} — aborting pipeline'
            logger.critical(error_msg)
            send_email_alert(f"BigQuery Insert Failed - {api_name}", error_msg)
            error_count += 1
            return

        # 5a. Non-200 HTTP response
        if response.status_code != 200:
            error_msg = f'HTTP {response.status_code}: {response.text}'
            log_pipeline_error(bq, logger, error_msg, request_id, stage='fetch')
            send_email_alert(
                f"{api_name.upper()} API Error: HTTP {response.status_code}",
                response.text[:500],
                {"request_id": request_id, "status_code": response.status_code}
            )
            error_count += 1
            return

        # 6. Decode JSON
        try:
            data = response.json()
        except ValueError as e:
            error_msg = f'Failed to decode JSON response: {e}'
            log_pipeline_error(bq, logger, error_msg, request_id, stage='parse')
            send_email_alert(
                f"{api_name.upper()} Invalid JSON Response",
                error_msg,
                {"request_id": request_id}
            )
            error_count += 1
            return

        # 7. Insert raw data
        raw_row = api.get_raw_row(data, request_id)
        if not insert_rows(bq, 'raw_data', [raw_row], logger):
            log_pipeline_error(bq, logger, 'Failed to insert raw_data', request_id, stage='insert')
            # Don't return — raw data failure shouldn't stop pipeline

        # 8. Parse — handles both single row and list of rows
        parsed, parse_error = api.parse(data, request_id, logger)

        if parsed is None:
            log_pipeline_error(bq, logger, parse_error, request_id, stage='parse')
            send_email_alert(
                f"{api_name.upper()} Parse Failed",
                parse_error,
                {"request_id": request_id, "hint": "API structure may have changed"}
            )
            error_count += 1
            return

        rows = parsed if isinstance(parsed, list) else [parsed]

        if not insert_rows(bq, meta['table'], rows, logger):
            error_msg = f'Failed to insert into {meta["table"]}'
            log_pipeline_error(bq, logger, error_msg, request_id, stage='insert')
            send_email_alert(
                f"BigQuery Insert Failed - {api_name}",
                error_msg,
                {"request_id": request_id, "table": meta['table'], "rows": len(rows)}
            )
            error_count += 1
            return

        # Success!
        records_loaded = len(rows)
        logger.info(
            f'Pipeline completed successfully | '
            f'api={api_name} | '
            f'request_id={request_id} | '
            f'rows={len(rows)}'
        )

    except Exception as e:
        logger.critical(f"Pipeline crashed: {str(e)}", exc_info=True)
        send_email_alert(
            f"CRITICAL: Pipeline Crashed - {api_name or 'unknown'}",
            str(e),
            {"error_type": type(e).__name__}
        )
        error_count += 1
        raise


    finally:
        elapsed = time.time() - start_time
        if api_name and error_count > 0:
            # Only send summary if we got far enough to identify the API and had at least one error
            send_pipeline_summary(api_name, records_loaded, error_count, elapsed)