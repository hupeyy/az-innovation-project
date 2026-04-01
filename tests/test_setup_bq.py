import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['GCP_PROJECT_ID'] = 'test_project'
os.environ['DATA_SET_ID']    = 'test_dataset'

import pytest
from unittest.mock import MagicMock, patch
from google.cloud import bigquery

from bq.setup_bq import parse_sql, parse_columns, setup_dataset, setup_tables, TYPE_MAP

# -------------------------------------------------------------------
# Sample SQL Fixtures
# -------------------------------------------------------------------

SIMPLE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS simple_table (
    id      SERIAL      NOT NULL,
    name    VARCHAR(255) NOT NULL,
    value   FLOAT,
    note    TEXT
);
"""

FULL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS api_sources (
    id       SERIAL       NOT NULL,
    name     VARCHAR(255) NOT NULL,
    base_url VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS api_requests (
    id               SERIAL    NOT NULL,
    source_id        INTEGER   NOT NULL,
    endpoint         VARCHAR(255) NOT NULL,
    timestamp        TIMESTAMP NOT NULL,
    http_status      INTEGER,
    response_time_ms INTEGER
);

CREATE TABLE IF NOT EXISTS weather_data (
    id         SERIAL    NOT NULL,
    request_id INTEGER   NOT NULL,
    city_name  VARCHAR(255) NOT NULL,
    country    VARCHAR(10)  NOT NULL,
    units      VARCHAR(20)  NOT NULL,
    latitude   FLOAT     NOT NULL,
    longitude  FLOAT     NOT NULL,
    temp_min   FLOAT     NOT NULL,
    temp_max   FLOAT     NOT NULL,
    humidity   INTEGER   NOT NULL,
    wind_speed FLOAT     NOT NULL,
    sunrise    TIMESTAMP NOT NULL,
    sunset     TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS news_data (
    id           SERIAL       NOT NULL,
    request_id   INTEGER      NOT NULL,
    source_id    VARCHAR(255),
    source_name  VARCHAR(255) NOT NULL,
    author       VARCHAR(255),
    title        VARCHAR(255) NOT NULL,
    description  TEXT,
    url          TEXT         NOT NULL,
    url_to_image TEXT,
    published_at TIMESTAMP    NOT NULL,
    content      TEXT
);

CREATE TABLE IF NOT EXISTS raw_data (
    id         SERIAL   NOT NULL,
    request_id INTEGER  NOT NULL,
    raw_data   JSONB    NOT NULL
);

CREATE TABLE IF NOT EXISTS api_errors (
    id            SERIAL    NOT NULL,
    request_id    INTEGER   NOT NULL,
    error_message TEXT      NOT NULL,
    timestamp     TIMESTAMP NOT NULL
);
"""

COMMENTED_SQL = """
-- This is a comment
CREATE TABLE IF NOT EXISTS commented_table (
    id   SERIAL  NOT NULL,  -- inline comment
    name VARCHAR(255) NOT NULL
    /* block
       comment */
);
"""

CONSTRAINT_SQL = """
CREATE TABLE IF NOT EXISTS constraint_table (
    id        SERIAL  NOT NULL,
    parent_id INTEGER NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (parent_id) REFERENCES other_table(id),
    UNIQUE (parent_id)
);
"""

UNKNOWN_TYPE_SQL = """
CREATE TABLE IF NOT EXISTS unknown_type_table (
    id      SERIAL NOT NULL,
    data    BYTEA  NOT NULL
);
"""

def write_temp_sql(tmp_path, content):
    """Write SQL content to a temp file and return the path."""
    sql_file = tmp_path / 'schema.sql'
    sql_file.write_text(content)
    return str(sql_file)


# ===================================================================
# 1. Type Mapping Tests
# ===================================================================
class TestTypeMap:
    """Ensure all expected SQL types map to correct BigQuery types."""

    def test_serial_maps_to_int64(self):
        assert TYPE_MAP['SERIAL'] == 'INT64'

    def test_integer_maps_to_int64(self):
        assert TYPE_MAP['INTEGER'] == 'INT64'

    def test_varchar_maps_to_string(self):
        assert TYPE_MAP['VARCHAR'] == 'STRING'

    def test_text_maps_to_string(self):
        assert TYPE_MAP['TEXT'] == 'STRING'

    def test_float_maps_to_float64(self):
        assert TYPE_MAP['FLOAT'] == 'FLOAT64'

    def test_timestamp_maps_to_timestamp(self):
        assert TYPE_MAP['TIMESTAMP'] == 'TIMESTAMP'

    def test_jsonb_maps_to_string(self):
        """JSONB has no BigQuery equivalent — must be stored as STRING."""
        assert TYPE_MAP['JSONB'] == 'STRING'

    def test_json_maps_to_string(self):
        assert TYPE_MAP['JSON'] == 'STRING'

    def test_boolean_maps_to_bool(self):
        assert TYPE_MAP['BOOLEAN'] == 'BOOL'


# ===================================================================
# 2. Column Parser Tests
# ===================================================================
class TestParseColumns:
    """Tests for parse_columns() in isolation."""

    def test_not_null_column_is_required(self):
        schema = parse_columns('id SERIAL NOT NULL')
        assert schema[0].mode == 'REQUIRED'

    def test_nullable_column_is_nullable(self):
        schema = parse_columns('note TEXT')
        assert schema[0].mode == 'NULLABLE'

    def test_serial_is_always_required(self):
        """SERIAL implies a primary key — always required."""
        schema = parse_columns('id SERIAL')
        assert schema[0].mode == 'REQUIRED'

    def test_column_name_is_parsed(self):
        schema = parse_columns('city_name VARCHAR(255) NOT NULL')
        assert schema[0].name == 'city_name'

    def test_varchar_type_is_string(self):
        schema = parse_columns('city_name VARCHAR(255) NOT NULL')
        assert schema[0].field_type == 'STRING'

    def test_integer_type_is_int64(self):
        schema = parse_columns('humidity INTEGER NOT NULL')
        assert schema[0].field_type == 'INT64'

    def test_float_type_is_float64(self):
        schema = parse_columns('latitude FLOAT NOT NULL')
        assert schema[0].field_type == 'FLOAT64'

    def test_timestamp_type_is_timestamp(self):
        schema = parse_columns('created_at TIMESTAMP NOT NULL')
        assert schema[0].field_type == 'TIMESTAMP'

    def test_jsonb_type_is_string(self):
        schema = parse_columns('raw_data JSONB NOT NULL')
        assert schema[0].field_type == 'STRING'

    def test_primary_key_constraint_is_skipped(self):
        schema = parse_columns(
            'id SERIAL NOT NULL,\n'
            'PRIMARY KEY (id)'
        )
        assert len(schema) == 1
        assert schema[0].name == 'id'

    def test_foreign_key_constraint_is_skipped(self):
        schema = parse_columns(
            'parent_id INTEGER NOT NULL,\n'
            'FOREIGN KEY (parent_id) REFERENCES other_table(id)'
        )
        assert len(schema) == 1
        assert schema[0].name == 'parent_id'

    def test_unique_constraint_is_skipped(self):
        schema = parse_columns(
            'name VARCHAR(255) NOT NULL,\n'
            'UNIQUE (name)'
        )
        assert len(schema) == 1

    def test_multiple_columns_parsed(self):
        schema = parse_columns(
            'id SERIAL NOT NULL,\n'
            'name VARCHAR(255) NOT NULL,\n'
            'value FLOAT'
        )
        assert len(schema) == 3

    def test_empty_string_returns_empty_list(self):
        schema = parse_columns('')
        assert schema == []


# ===================================================================
# 3. SQL File Parser Tests
# ===================================================================
class TestParseSql:
    """Tests for parse_sql() reading from a file."""

    def test_parses_correct_number_of_tables(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        assert len(tables) == 6

    def test_all_expected_tables_present(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        expected = {
            'api_sources', 'api_requests', 'weather_data',
            'news_data', 'raw_data', 'api_errors'
        }
        assert set(tables.keys()) == expected

    def test_each_table_has_schema_fields(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        for table_name, schema in tables.items():
            assert len(schema) > 0, f'{table_name} has no fields'

    def test_strips_single_line_comments(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, COMMENTED_SQL)
        tables   = parse_sql(sql_file)
        assert 'commented_table' in tables

    def test_strips_block_comments(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, COMMENTED_SQL)
        tables   = parse_sql(sql_file)
        schema   = tables['commented_table']
        # Block comment should not produce a field
        assert len(schema) == 2

    def test_constraints_are_not_parsed_as_fields(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, CONSTRAINT_SQL)
        tables   = parse_sql(sql_file)
        schema   = tables['constraint_table']
        names    = [field.name for field in schema]
        assert 'PRIMARY'  not in names
        assert 'FOREIGN'  not in names
        assert 'UNIQUE'   not in names

    def test_unknown_type_defaults_to_string(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, UNKNOWN_TYPE_SQL)
        tables   = parse_sql(sql_file)
        schema   = tables['unknown_type_table']
        data_field = next(f for f in schema if f.name == 'data')
        assert data_field.field_type == 'STRING'

    def test_missing_file_raises_error(self):
        with pytest.raises(FileNotFoundError):
            parse_sql('/nonexistent/path/schema.sql')

    # --- weather_data specific ---

    def test_weather_data_has_correct_field_count(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        assert len(tables['weather_data']) == 13

    def test_weather_latitude_is_float64(self, tmp_path):
        sql_file   = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables     = parse_sql(sql_file)
        lat_field  = next(f for f in tables['weather_data'] if f.name == 'latitude')
        assert lat_field.field_type == 'FLOAT64'

    def test_weather_sunrise_is_timestamp(self, tmp_path):
        sql_file      = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables        = parse_sql(sql_file)
        sunrise_field = next(f for f in tables['weather_data'] if f.name == 'sunrise')
        assert sunrise_field.field_type == 'TIMESTAMP'

    def test_weather_all_fields_required(self, tmp_path):
        """All weather_data fields are NOT NULL in the schema."""
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        for field in tables['weather_data']:
            assert field.mode == 'REQUIRED', f'{field.name} should be REQUIRED'

    # --- news_data specific ---

    def test_news_data_has_correct_field_count(self, tmp_path):
        sql_file = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables   = parse_sql(sql_file)
        assert len(tables['news_data']) == 11

    def test_news_nullable_fields_are_nullable(self, tmp_path):
        """source_id, author, description, url_to_image, content are nullable."""
        sql_file       = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables         = parse_sql(sql_file)
        nullable_fields = ['source_id', 'author', 'description', 'url_to_image', 'content']
        for field in tables['news_data']:
            if field.name in nullable_fields:
                assert field.mode == 'NULLABLE', f'{field.name} should be NULLABLE'

    def test_news_required_fields_are_required(self, tmp_path):
        """id, request_id, source_name, title, url, published_at must be required."""
        sql_file        = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables          = parse_sql(sql_file)
        required_fields = ['id', 'request_id', 'source_name', 'title', 'url', 'published_at']
        for field in tables['news_data']:
            if field.name in required_fields:
                assert field.mode == 'REQUIRED', f'{field.name} should be REQUIRED'

    # --- raw_data specific ---

    def test_raw_data_is_string_type(self, tmp_path):
        """JSONB must be converted to STRING for BigQuery."""
        sql_file   = write_temp_sql(tmp_path, FULL_SCHEMA_SQL)
        tables     = parse_sql(sql_file)
        raw_field  = next(f for f in tables['raw_data'] if f.name == 'raw_data')
        assert raw_field.field_type == 'STRING'


# ===================================================================
# 4. BigQuery Setup Tests (mocked — never touches real BQ)
# ===================================================================
class TestSetupDataset:

    @patch('bq.setup_bq.client')
    def test_create_dataset_is_called(self, mock_client):
        setup_dataset()
        mock_client.create_dataset.assert_called_once()

    @patch('bq.setup_bq.client')
    def test_dataset_exists_ok_is_true(self, mock_client):
        """exists_ok=True means it won't fail if dataset already exists."""
        setup_dataset()
        _, kwargs = mock_client.create_dataset.call_args
        assert kwargs.get('exists_ok') is True


class TestSetupTables:

    @patch('bq.setup_bq.client')
    def test_creates_correct_number_of_tables(self, mock_client):
        mock_client.create_table.return_value = MagicMock()
        tables = {
            'table_one': [bigquery.SchemaField('id', 'INT64', mode='REQUIRED')],
            'table_two': [bigquery.SchemaField('id', 'INT64', mode='REQUIRED')],
        }
        setup_tables(tables)
        assert mock_client.create_table.call_count == 2

    @patch('bq.setup_bq.client')
    def test_exists_ok_is_true(self, mock_client):
        """exists_ok=True means already-existing tables won't raise an error."""
        mock_client.create_table.return_value = MagicMock()
        tables = {
            'table_one': [bigquery.SchemaField('id', 'INT64', mode='REQUIRED')]
        }
        setup_tables(tables)
        _, kwargs = mock_client.create_table.call_args
        assert kwargs.get('exists_ok') is True

    @patch('bq.setup_bq.client')
    def test_empty_tables_dict_creates_nothing(self, mock_client):
        setup_tables({})
        mock_client.create_table.assert_not_called()