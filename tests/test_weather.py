import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['OPENWEATHER_API_KEY'] = 'test_api_key'
os.environ['GCP_PROJECT_ID']      = 'test_project'

import pytest
import json
import logging
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from requests.exceptions import ConnectionError, Timeout, RequestException
from google.api_core.exceptions import GoogleAPIError

from apis.weather.fetch    import fetch_weather
from apis.weather.parse    import parse_weather
from apis.weather.pipeline import get_api_meta, get_raw_row
import apis.weather.pipeline as weather_api
from pipeline.runner       import run_pipeline
from bq.client             import insert_rows, get_bq_client

# -------------------------------------------------------------------
# Shared Fixtures & Helpers
# -------------------------------------------------------------------

VALID_WEATHER_RESPONSE = {
    'dt': 1717000000,
    'id': 4167147,
    'name': 'Orlando',
    'sys': {
        'country': 'US',
        'sunrise': 1716979200,
        'sunset':  1717027200
    },
    'coord': {
        'lat': 28.59,
        'lon': -81.38
    },
    'main': {
        'temp_min': 75.0,
        'temp_max': 92.0,
        'humidity': 80
    },
    'wind': {
        'speed': 10.5
    }
}

EXPECTED_WEATHER_ROW = {
    'id':         4167147,
    'city_name':  'Orlando',
    'country':    'US',
    'units':      'imperial',
    'latitude':   28.59,
    'longitude':  -81.38,
    'temp_min':   75.0,
    'temp_max':   92.0,
    'humidity':   80,
    'wind_speed': 10.5,
    'sunrise':    datetime.fromtimestamp(1716979200, tz=timezone.utc).isoformat(),
    'sunset':     datetime.fromtimestamp(1717027200, tz=timezone.utc).isoformat(),
}

def make_mock_response(status_code=200, json_data=None, text='', elapsed_ms=120):
    mock_resp                       = MagicMock()
    mock_resp.status_code           = status_code
    mock_resp.text                  = text
    mock_resp.json.return_value     = json_data if json_data is not None else VALID_WEATHER_RESPONSE
    mock_elapsed                    = MagicMock()
    mock_elapsed.total_seconds.return_value = elapsed_ms / 1000
    mock_resp.elapsed               = mock_elapsed
    return mock_resp

def make_bq_client(insert_return=None):
    client                          = MagicMock()
    client.insert_rows_json.return_value = insert_return or []
    return client

def make_mock_logger():
    return MagicMock()

def remove_key(d, key):
    return {k: v for k, v in d.items() if k != key}

def nested_remove_key(data, path):
    import copy
    d   = copy.deepcopy(data)
    ref = d
    for key in path[:-1]:
        ref = ref[key]
    del ref[path[-1]]
    return d

def get_inserted_tables(client):
    """Extract table names from all BQ insert calls."""
    return [
        call_args.args[0]
        for call_args in client.insert_rows_json.call_args_list
    ]


# ===================================================================
# 1. API Meta Tests
# ===================================================================
class TestWeatherApiMeta:
    """
    Tests for get_api_meta().
    Every API module must return the correct meta for the runner to use.
    """

    def test_meta_contains_source_id(self):
        meta = get_api_meta()
        assert 'source_id' in meta

    def test_meta_contains_endpoint(self):
        meta = get_api_meta()
        assert 'endpoint' in meta

    def test_meta_contains_table(self):
        meta = get_api_meta()
        assert 'table' in meta

    def test_source_id_is_correct(self):
        meta = get_api_meta()
        assert meta['source_id'] == 1

    def test_endpoint_is_correct(self):
        meta = get_api_meta()
        assert meta['endpoint'] == '/data/2.5/weather'

    def test_table_is_correct(self):
        meta = get_api_meta()
        assert meta['table'] == 'weather_data'


# ===================================================================
# 2. Raw Row Tests
# ===================================================================
class TestWeatherRawRow:
    """
    Tests for get_raw_row().
    Ensures the raw response is always stored correctly.
    """

    def test_raw_row_contains_request_id(self):
        raw_row = get_raw_row(VALID_WEATHER_RESPONSE, request_id=999)
        assert raw_row['request_id'] == 999

    def test_raw_row_id_uses_dt(self):
        raw_row = get_raw_row(VALID_WEATHER_RESPONSE, request_id=999)
        assert raw_row['id'] == VALID_WEATHER_RESPONSE['dt']

    def test_raw_data_is_valid_json_string(self):
        raw_row = get_raw_row(VALID_WEATHER_RESPONSE, request_id=999)
        # Should be a string that can be parsed back to a dict
        parsed  = json.loads(raw_row['raw_data'])
        assert parsed == VALID_WEATHER_RESPONSE

    def test_raw_data_preserves_all_fields(self):
        raw_row = get_raw_row(VALID_WEATHER_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        for key in VALID_WEATHER_RESPONSE:
            assert key in parsed


# ===================================================================
# 3. Fetch Tests
# ===================================================================
class TestFetchWeather:
    """Tests for fetch_weather() in isolation."""

    @patch('apis.weather.fetch.requests.get')
    def test_returns_response_on_success(self, mock_get):
        mock_get.return_value   = make_mock_response(200)
        response, error         = fetch_weather('test_key', make_mock_logger())
        assert response         is not None
        assert error            is None

    @patch('apis.weather.fetch.requests.get')
    def test_returns_response_on_401(self, mock_get):
        """fetch() itself succeeds — HTTP errors are handled by the runner."""
        mock_get.return_value   = make_mock_response(401, text='Unauthorized')
        response, error         = fetch_weather('bad_key', make_mock_logger())
        assert response         is not None
        assert response.status_code == 401

    @patch('apis.weather.fetch.requests.get')
    def test_returns_response_on_429(self, mock_get):
        mock_get.return_value   = make_mock_response(429, text='Too Many Requests')
        response, error         = fetch_weather('test_key', make_mock_logger())
        assert response         is not None
        assert response.status_code == 429

    @patch('apis.weather.fetch.requests.get')
    def test_returns_response_on_500(self, mock_get):
        mock_get.return_value   = make_mock_response(500, text='Server Error')
        response, error         = fetch_weather('test_key', make_mock_logger())
        assert response         is not None
        assert response.status_code == 500

    @patch('apis.weather.fetch.requests.get', side_effect=ConnectionError('No network'))
    def test_connection_error_returns_none(self, mock_get):
        response, error = fetch_weather('test_key', make_mock_logger())
        assert response is None
        assert 'Connection error' in error

    @patch('apis.weather.fetch.requests.get', side_effect=Timeout())
    def test_timeout_returns_none(self, mock_get):
        response, error = fetch_weather('test_key', make_mock_logger())
        assert response is None
        assert 'timed out' in error

    @patch('apis.weather.fetch.requests.get', side_effect=RequestException('Failure'))
    def test_request_exception_returns_none(self, mock_get):
        response, error = fetch_weather('test_key', make_mock_logger())
        assert response is None
        assert 'Unexpected request error' in error

    @patch('apis.weather.fetch.requests.get')
    def test_correct_coordinates_sent(self, mock_get):
        import apis.weather.config as config
        mock_get.return_value   = make_mock_response(200)
        fetch_weather('test_key', make_mock_logger())
        sent_params             = mock_get.call_args.kwargs['params']
        assert sent_params['lat'] == config.ORLANDO_LAT
        assert sent_params['lon'] == config.ORLANDO_LON

    @patch('apis.weather.fetch.requests.get')
    def test_correct_units_sent(self, mock_get):
        import apis.weather.config as config
        mock_get.return_value   = make_mock_response(200)
        fetch_weather('test_key', make_mock_logger())
        sent_params             = mock_get.call_args.kwargs['params']
        assert sent_params['units'] == config.UNITS

    @patch('apis.weather.fetch.requests.get')
    def test_api_key_sent(self, mock_get):
        mock_get.return_value   = make_mock_response(200)
        fetch_weather('test_key', make_mock_logger())
        sent_params             = mock_get.call_args.kwargs['params']
        assert sent_params['appid'] == 'test_key'

    @patch('apis.weather.fetch.requests.get')
    def test_timeout_is_enforced(self, mock_get):
        mock_get.return_value   = make_mock_response(200)
        fetch_weather('test_key', make_mock_logger())
        sent_kwargs             = mock_get.call_args.kwargs
        assert 'timeout' in sent_kwargs
        assert sent_kwargs['timeout'] > 0


# ===================================================================
# 4. Parse Tests
# ===================================================================
class TestParseWeather:
    """Tests for parse_weather() in isolation."""

    def test_valid_response_returns_correct_row(self):
        weather_row, error = parse_weather(VALID_WEATHER_RESPONSE, request_id=1, logger=make_mock_logger())
        assert error is None
        for field, expected in EXPECTED_WEATHER_ROW.items():
            assert weather_row[field] == expected, f'Mismatch on field: {field}'

    def test_request_id_is_attached(self):
        weather_row, _ = parse_weather(VALID_WEATHER_RESPONSE, request_id=999, logger=make_mock_logger())
        assert weather_row['request_id'] == 999

    def test_sunrise_is_utc_isoformat(self):
        weather_row, _ = parse_weather(VALID_WEATHER_RESPONSE, request_id=1, logger=make_mock_logger())
        assert weather_row['sunrise'].endswith('+00:00')

    def test_sunset_is_utc_isoformat(self):
        weather_row, _ = parse_weather(VALID_WEATHER_RESPONSE, request_id=1, logger=make_mock_logger())
        assert weather_row['sunset'].endswith('+00:00')

    # --- Missing top-level fields ---

    def test_missing_city_name_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'name')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_id_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'id')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_sys_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'sys')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_coord_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'coord')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_main_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'main')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_wind_returns_error(self):
        bad_data           = remove_key(VALID_WEATHER_RESPONSE, 'wind')
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    # --- Missing nested fields ---

    def test_missing_country_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['sys', 'country'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_sunrise_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['sys', 'sunrise'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_sunset_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['sys', 'sunset'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_temp_min_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['main', 'temp_min'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_temp_max_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['main', 'temp_max'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_humidity_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['main', 'humidity'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_missing_wind_speed_returns_error(self):
        bad_data           = nested_remove_key(VALID_WEATHER_RESPONSE, ['wind', 'speed'])
        weather_row, error = parse_weather(bad_data, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    # --- Bad types ---

    def test_none_response_returns_error(self):
        weather_row, error = parse_weather(None, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None

    def test_empty_response_returns_error(self):
        weather_row, error = parse_weather({}, request_id=1, logger=make_mock_logger())
        assert weather_row is None
        assert error       is not None


# ===================================================================
# 5. End-to-End Pipeline Tests
# ===================================================================
class TestWeatherPipeline:
    """
    Full run_pipeline() tests with weather_api module passed in.
    All BigQuery and HTTP calls are mocked.
    """

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_successful_run_inserts_all_three_tables(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_requests' in t for t in tables)
        assert any('raw_data'     in t for t in tables)
        assert any('weather_data' in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_table_used_from_meta(self, mock_get_client, mock_get):
        """Runner should use table name from meta, not hardcode 'weather_data'."""
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        expected_table = get_api_meta()['table']
        tables         = get_inserted_tables(client)
        assert any(expected_table in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_endpoint_logged_in_api_request(self, mock_get_client, mock_get):
        """Runner should use endpoint from meta, not hardcode the weather endpoint."""
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        # Find the api_requests insert call and inspect the row
        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'api_requests' in table:
                assert rows[0]['endpoint']  == get_api_meta()['endpoint']
                assert rows[0]['source_id'] == get_api_meta()['source_id']

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(401, text='Unauthorized')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_does_not_insert_weather_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(500, text='Server Error')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert not any('weather_data' in t for t in tables)

    @patch('apis.weather.fetch.requests.get', side_effect=Timeout())
    @patch('pipeline.runner.get_bq_client')
    def test_timeout_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.weather.fetch.requests.get', side_effect=ConnectionError())
    @patch('pipeline.runner.get_bq_client')
    def test_connection_error_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_malformed_json_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_resp                    = make_mock_response(200)
        mock_resp.json.side_effect   = ValueError('No JSON')
        mock_get.return_value        = mock_resp
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_incomplete_response_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200, json_data={'dt': 123})
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_inserted_before_weather_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables        = get_inserted_tables(client)
        raw_index     = next(i for i, t in enumerate(tables) if 'raw_data'     in t)
        weather_index = next(i for i, t in enumerate(tables) if 'weather_data' in t)
        assert raw_index < weather_index

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_api_requests_is_first_insert(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        assert 'api_requests' in client.insert_rows_json.call_args_list[0].args[0]

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_pipeline_aborts_if_api_request_insert_fails(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],  # api_requests fails
            [],
            [],
        ]
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        assert client.insert_rows_json.call_count == 1

    @patch('apis.weather.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_failure_does_not_stop_weather_insert(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [],                                                   # api_requests
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],   # raw_data fails
            [],                                                   # weather_data
        ]
        mock_get_client.return_value = client

        run_pipeline(weather_api)

        tables = get_inserted_tables(client)
        assert any('weather_data' in t for t in tables)