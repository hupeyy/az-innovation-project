# pipeline/logger.py

import logging
from datetime import datetime, timezone

def setup_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log', mode='a')
        ]
    )
    return logging.getLogger(name)

def log_pipeline_error(bq, logger, error_message, request_id, stage=None):
    from bq.client import insert_rows
    logger.error(
        f'Pipeline error | stage={stage} | '
        f'request_id={request_id} | {error_message}'
    )
    error_row = {
        'id':            int(datetime.now(timezone.utc).timestamp()),
        'request_id':    request_id,
        'error_message': f'[{stage.upper()}] {error_message}' if stage else error_message,
        'timestamp':     datetime.now(timezone.utc).isoformat()
    }
    insert_rows(bq, 'api_errors', [error_row], logger)  # ← logger was missing hereimport logging
from datetime import datetime, timezone

def setup_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log', mode='a')
        ]
    )
    return logging.getLogger(name)

def log_pipeline_error(bq, logger, error_message, request_id, stage=None):
    from bq.client import insert_rows
    logger.error(
        f'Pipeline error | stage={stage} | '
        f'request_id={request_id} | {error_message}'
    )
    error_row = {
        'id':            int(datetime.now(timezone.utc).timestamp()),
        'request_id':    request_id,
        'error_message': f'[{stage.upper()}] {error_message}' if stage else error_message,
        'timestamp':     datetime.now(timezone.utc).isoformat()
    }
    insert_rows(bq, 'api_errors', [error_row])# pipeline/logger.py

import logging
from datetime import datetime, timezone

def setup_logger(name):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log', mode='a')
        ]
    )
    return logging.getLogger(name)

def log_pipeline_error(bq, logger, error_message, request_id, stage=None):
    from bq.client import insert_rows
    logger.error(
        f'Pipeline error | stage={stage} | '
        f'request_id={request_id} | {error_message}'
    )
    error_row = {
        'id':            int(datetime.now(timezone.utc).timestamp()),
        'request_id':    request_id,
        'error_message': f'[{stage.upper()}] {error_message}' if stage else error_message,
        'timestamp':     datetime.now(timezone.utc).isoformat()
    }
    insert_rows(bq, 'api_errors', [error_row], logger)