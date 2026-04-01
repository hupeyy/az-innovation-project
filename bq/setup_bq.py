from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('DATA_SET_ID')

client = bigquery.Client(project=PROJECT_ID)

# Create dataset if it doesn't exist
dataset_ref = bigquery.Dataset(f'{PROJECT_ID}.{DATASET_ID}')
dataset_ref.location = 'US'
client.create_dataset(dataset_ref, exists_ok=True)
print(f'Dataset {DATASET_ID} ready.')

# Table definitions
tables = {
    'api_sources': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('name', 'STRING'),
        bigquery.SchemaField('base_url', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
    ],
    'api_requests': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('source_id', 'INT64'),
        bigquery.SchemaField('endpoint', 'STRING'),
        bigquery.SchemaField('timestamp', 'TIMESTAMP'),
        bigquery.SchemaField('http_status', 'INT64'),
        bigquery.SchemaField('response_time_ms', 'INT64'),
    ],
    'weather_data': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('request_id', 'INT64'),
        bigquery.SchemaField('city_name', 'STRING'),
        bigquery.SchemaField('country', 'STRING'),
        bigquery.SchemaField('units', 'STRING'),
        bigquery.SchemaField('latitude', 'FLOAT64'),
        bigquery.SchemaField('longitude', 'FLOAT64'),
        bigquery.SchemaField('temp_min', 'FLOAT64'),
        bigquery.SchemaField('temp_max', 'FLOAT64'),
        bigquery.SchemaField('humidity', 'INT64'),
        bigquery.SchemaField('wind_speed', 'FLOAT64'),
        bigquery.SchemaField('sunrise', 'TIMESTAMP'),
        bigquery.SchemaField('sunset', 'TIMESTAMP'),
    ],
    'extracted_entities': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('request_id', 'INT64'),
        bigquery.SchemaField('entity_type', 'STRING'),
        bigquery.SchemaField('entity_value', 'STRING'),
    ],
    'raw_data': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('request_id', 'INT64'),
        bigquery.SchemaField('raw_data', 'STRING'),  # store as JSON string
    ],
    'api_errors': [
        bigquery.SchemaField('id', 'INT64'),
        bigquery.SchemaField('request_id', 'INT64'),
        bigquery.SchemaField('error_message', 'STRING'),
        bigquery.SchemaField('timestamp', 'TIMESTAMP'),
    ],
}

# Create each table
for table_name, schema in tables.items():
    table_ref = bigquery.Table(f'{PROJECT_ID}.{DATASET_ID}.{table_name}', schema=schema)
    client.create_table(table_ref, exists_ok=True)
    print(f'Table {table_name} ready.')