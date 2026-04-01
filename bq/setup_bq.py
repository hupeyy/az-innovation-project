from google.cloud import bigquery
from dotenv import load_dotenv
import os
import re

load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('DATA_SET_ID')

client = bigquery.Client(project=PROJECT_ID)

# -------------------------------------------------------------------
# SQL -> BigQuery type mapping
# -------------------------------------------------------------------
TYPE_MAP = {
    'SERIAL':       'INT64',
    'INTEGER':      'INT64',
    'INT':          'INT64',
    'BIGINT':       'INT64',
    'SMALLINT':     'INT64',
    'FLOAT':        'FLOAT64',
    'DOUBLE':       'FLOAT64',
    'REAL':         'FLOAT64',
    'VARCHAR':      'STRING',
    'TEXT':         'STRING',
    'CHAR':         'STRING',
    'BOOLEAN':      'BOOL',
    'BOOL':         'BOOL',
    'TIMESTAMP':    'TIMESTAMP',
    'DATE':         'DATE',
    'TIME':         'TIME',
    'JSONB':        'STRING',   # BigQuery has no native JSONB — store as STRING
    'JSON':         'STRING',
    'NUMERIC':      'NUMERIC',
    'DECIMAL':      'NUMERIC',
}

# -------------------------------------------------------------------
# SQL Parser
# -------------------------------------------------------------------
def parse_sql(filepath):
    """
    Parse a SQL schema file and extract table definitions.

    Returns:
        dict: { table_name: [ SchemaField, ... ] }
    """
    with open(filepath, 'r') as f:
        sql = f.read()

    # Strip comments
    sql = re.sub(r'--.*',          '',  sql)   # single line comments
    sql = re.sub(r'/\*.*?\*/',     '',  sql, flags=re.DOTALL)  # block comments

    # Find all CREATE TABLE blocks
    table_pattern = re.compile(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)\s*;',
        re.IGNORECASE | re.DOTALL
    )

    tables = {}
    for match in table_pattern.finditer(sql):
        table_name  = match.group(1).strip()
        columns_raw = match.group(2).strip()
        schema      = parse_columns(columns_raw)

        if schema:
            tables[table_name] = schema
            print(f'Parsed table: {table_name} ({len(schema)} fields)')

    return tables

def parse_columns(columns_raw):
    """
    Parse column definitions from inside a CREATE TABLE block.

    Returns:
        list: [ SchemaField, ... ]
    """
    schema  = []
    columns = [col.strip() for col in columns_raw.split(',')]

    for column in columns:
        column = column.strip()

        # Skip constraints
        if re.match(r'(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK|CONSTRAINT)', column, re.IGNORECASE):
            continue

        # Match column name and type
        # e.g. "id SERIAL NOT NULL" or "name VARCHAR(255) NOT NULL"
        col_match = re.match(r'(\w+)\s+(\w+)', column)
        if not col_match:
            continue

        col_name    = col_match.group(1)
        col_sql_type = col_match.group(2).upper()
        bq_type     = TYPE_MAP.get(col_sql_type)

        if not bq_type:
            print(f'  Warning: unknown type "{col_sql_type}" for column "{col_name}" — defaulting to STRING')
            bq_type = 'STRING'

        # Determine mode
        is_nullable = 'NOT NULL' not in column.upper()
        mode        = 'NULLABLE' if is_nullable else 'REQUIRED'

        # SERIAL is always a generated primary key — treat as required
        if col_sql_type == 'SERIAL':
            mode = 'REQUIRED'

        schema.append(bigquery.SchemaField(col_name, bq_type, mode=mode))

    return schema

# -------------------------------------------------------------------
# Dataset Setup
# -------------------------------------------------------------------
def setup_dataset():
    dataset_ref          = bigquery.Dataset(f'{PROJECT_ID}.{DATASET_ID}')
    dataset_ref.location = 'US'
    client.create_dataset(dataset_ref, exists_ok=True)
    print(f'Dataset {DATASET_ID} ready.')

# -------------------------------------------------------------------
# Table Setup
# -------------------------------------------------------------------
def setup_tables(tables):
    for table_name, schema in tables.items():
        table_ref = bigquery.Table(
            f'{PROJECT_ID}.{DATASET_ID}.{table_name}',
            schema=schema
        )
        client.create_table(table_ref, exists_ok=True)
        print(f'Table {table_name} ready.')

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
if __name__ == '__main__':
    sql_file = os.path.join(os.path.dirname(__file__), 'schema.sql')

    if not os.path.exists(sql_file):
        print(f'Error: schema.sql not found at {sql_file}')
        exit(1)

    setup_dataset()
    tables = parse_sql(sql_file)
    setup_tables(tables)
    print(f'\nSetup complete. {len(tables)} table(s) processed.')