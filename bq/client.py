# bq/client.py
from config import GCP_PROJECT_ID, DATASET_ID

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def get_bq_client(logger):
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        logger.info('BigQuery client initialized successfully')
        return client
    except Exception as e:
        logger.critical(f'Failed to initialize BigQuery client: {e}')
        raise

def insert_rows(client, table_name, rows, logger):
    table_ref = f'{GCP_PROJECT_ID}.{DATASET_ID}.{table_name}'
    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            for error in errors:
                logger.error(
                    f'BigQuery row insert error | table={table_name} '
                    f'index={error.get("index")} '
                    f'reason={error.get("errors")}'
                )
            return False
        logger.info(f'Inserted {len(rows)} row(s) into {table_name}')
        return True
    except GoogleAPIError as e:
        logger.error(f'BigQuery API error on table={table_name}: {e}')
        return False
    except Exception as e:
        logger.error(f'Unexpected error inserting into table={table_name}: {e}')
        return False