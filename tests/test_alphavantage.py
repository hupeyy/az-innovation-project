# tests/test_alpha_vantage.py

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['ALPHAVANTAGE_API_KEY'] = 'test_api_key'
os.environ['GCP_PROJECT_ID']       = 'test_project'

import pytest
import json
import copy
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from requests.exceptions import ConnectionError, Timeout, RequestException

from apis.alpha_vantage.fetch    import fetch_stock
from apis.alpha_vantage.parse    import parse_stock
from apis.alpha_vantage.pipeline import get_api_meta, get_raw_row
import apis.alpha_vantage.pipeline as av_api
from pipeline.runner             import run_pipeline


# -------------------------------------------------------------------
# Shared Fixtures & Helpers
# -------------------------------------------------------------------

VALID_AV_RESPONSE = {
    "Global Quote": {
        "01. symbol":            "IBM",
        "02. open":              "190.0000",
        "03. high":              "192.0000",
        "04. low":               "189.0000",
        "05. price":             "191.5000",
        "06. volume":            "3000000",
        "07. latest trading day": "2024-06-15",
        "08. previous close":    "189.5000",
        "09. change":            "2.0000",
        "10. change percent":    "1.0554%"
    }
}

EXPECTED_STOCK_ROW = {
    'symbol':     'IBM',
    'price':      191.5,
    'volume':     3000000,
    'latest_day': '2024-06-15',
}

AV_RATE_LIMIT_RESPONSE = {
    "Note": (
        "Thank you for using Alpha Vantage! Our standard API call "
        "frequency is 5 calls per minute and 100 calls per day."
    )
}

AV_ERROR_RESPONSE = {
    "Error Message": (
        "Invalid API call. Please retry or visit documentation."
    )
}

AV_EMPTY_QUOTE_RESPONSE = {
    "Global Quote": {}
}


def make_mock_response(status_code=200, json_data=None, text='', elapsed_ms=120):
    mock_resp                               = MagicMock()
    mock_resp.status_code                   = status_code
    mock_resp.text                          = text
    mock_resp.json.return_value             = json_data if json_data is not None else VALID_AV_RESPONSE
    mock_elapsed                            = MagicMock()
    mock_elapsed.total_seconds.return_value = elapsed_ms / 1000
    mock_resp.elapsed                       = mock_elapsed
    return mock_resp


def make_bq_client(insert_return=None):
    client                               = MagicMock()
    client.insert_rows_json.return_value = insert_return or []
    return client


def make_mock_logger():
    return MagicMock()


def remove_key(d, key):
    """Remove a top-level key from a dict copy."""
    return {k: v for k, v in d.items() if k != key}


def remove_quote_key(data, key):
    """Remove a key from inside the 'Global Quote' dict."""
    d = copy.deepcopy(data)
    del d['Global Quote'][key]
    return d


def get_inserted_tables(client):
    return [
        call_args.args[0]
        for call_args in client.insert_rows_json.call_args_list
    ]


# ===================================================================
# 1. API Meta Tests
# ===================================================================
class TestAlphaVantageApiMeta:
    """
    Tests for get_api_meta().
    Every API module must return the correct meta for the runner to use.
    """

    def test_meta_contains_api_name(self):
        meta = get_api_meta()
        assert 'api_name' in meta

    def test_meta_contains_source_id(self):
        meta = get_api_meta()
        assert 'source_id' in meta

    def test_meta_contains_endpoint(self):
        meta = get_api_meta()
        assert 'endpoint' in meta

    def test_meta_contains_table(self):
        meta = get_api_meta()
        assert 'table' in meta

    def test_api_name_is_correct(self):
        meta = get_api_meta()
        assert meta['api_name'] == 'alpha_vantage'

    def test_source_id_is_correct(self):
        meta = get_api_meta()
        assert meta['source_id'] == 3

    def test_endpoint_is_correct(self):
        meta = get_api_meta()
        assert meta['endpoint'] == '/query'

    def test_table_is_correct(self):
        meta = get_api_meta()
        assert meta['table'] == 'stock_data'


# ===================================================================
# 2. Raw Row Tests
# ===================================================================
class TestAlphaVantageRawRow:
    """
    Tests for get_raw_row().
    Ensures the raw response is always stored correctly.
    """

    def test_raw_row_contains_request_id(self):
        raw_row = get_raw_row(VALID_AV_RESPONSE, request_id=999)
        assert raw_row['request_id'] == 999

    def test_raw_row_id_matches_request_id(self):
        raw_row = get_raw_row(VALID_AV_RESPONSE, request_id=999)
        assert raw_row['id'] == 999

    def test_raw_data_is_valid_json_string(self):
        raw_row = get_raw_row(VALID_AV_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        assert parsed == VALID_AV_RESPONSE

    def test_raw_data_preserves_all_fields(self):
        raw_row = get_raw_row(VALID_AV_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        for key in VALID_AV_RESPONSE:
            assert key in parsed

    def test_raw_data_preserves_global_quote_fields(self):
        raw_row = get_raw_row(VALID_AV_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        for key in VALID_AV_RESPONSE['Global Quote']:
            assert key in parsed['Global Quote']

    def test_raw_data_stores_error_response(self):
        """Raw data should store even API-level error responses."""
        raw_row = get_raw_row(AV_RATE_LIMIT_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        assert 'Note' in parsed

    def test_raw_data_stores_empty_quote_response(self):
        raw_row = get_raw_row(AV_EMPTY_QUOTE_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        assert 'Global Quote' in parsed


# ===================================================================
# 3. Fetch Tests
# ===================================================================
class TestFetchStockData:
    """Tests for fetch_stock() in isolation."""

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_returns_response_on_success(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        response, error       = fetch_stock('test_key', make_mock_logger())
        assert response       is not None
        assert error          is None

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_returns_no_error_on_success(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        response, error       = fetch_stock('test_key', make_mock_logger())
        assert error          is None

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_returns_response_on_401(self, mock_get):
        mock_get.return_value = make_mock_response(401, text='Unauthorized')
        response, error       = fetch_stock('bad_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 401

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_returns_response_on_429(self, mock_get):
        mock_get.return_value = make_mock_response(429, text='Too Many Requests')
        response, error       = fetch_stock('test_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 429

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_returns_response_on_500(self, mock_get):
        mock_get.return_value = make_mock_response(500, text='Server Error')
        response, error       = fetch_stock('test_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 500

    @patch('apis.alpha_vantage.fetch.requests.get', side_effect=ConnectionError('No network'))
    def test_connection_error_returns_none(self, mock_get):
        response, error = fetch_stock('test_key', make_mock_logger())
        assert response is None
        assert error    is not None

    @patch('apis.alpha_vantage.fetch.requests.get', side_effect=Timeout())
    def test_timeout_returns_none(self, mock_get):
        response, error = fetch_stock('test_key', make_mock_logger())
        assert response is None
        assert 'timed out' in error

    @patch('apis.alpha_vantage.fetch.requests.get', side_effect=RequestException('Failure'))
    def test_request_exception_returns_none(self, mock_get):
        response, error = fetch_stock('test_key', make_mock_logger())
        assert response is None
        assert error    is not None

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_api_key_sent_in_params(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_stock('test_key', make_mock_logger())
        sent_params           = mock_get.call_args.kwargs['params']
        assert sent_params['apikey'] == 'test_key'

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_function_param_sent(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_stock('test_key', make_mock_logger())
        sent_params           = mock_get.call_args.kwargs['params']
        assert 'function' in sent_params

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_symbol_param_sent(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_stock('test_key', make_mock_logger())
        sent_params           = mock_get.call_args.kwargs['params']
        assert 'symbol' in sent_params

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_timeout_is_enforced(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_stock('test_key', make_mock_logger())
        sent_kwargs           = mock_get.call_args.kwargs
        assert 'timeout' in sent_kwargs
        assert sent_kwargs['timeout'] > 0

    @patch('apis.alpha_vantage.fetch.requests.get')
    def test_correct_base_url_used(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_stock('test_key', make_mock_logger())
        called_url            = mock_get.call_args.args[0]
        assert 'alphavantage.co' in called_url


# ===================================================================
# 4. Parse Tests
# ===================================================================
class TestParseStock:
    """Tests for parse_stock() in isolation."""

    # --- Happy path ---

    def test_valid_response_returns_one_row(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err  is None
        assert len(rows) == 1

    def test_valid_response_returns_correct_symbol(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert rows[0]['symbol'] == EXPECTED_STOCK_ROW['symbol']

    def test_valid_response_returns_correct_price(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert rows[0]['price'] == EXPECTED_STOCK_ROW['price']

    def test_valid_response_returns_correct_volume(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert rows[0]['volume'] == EXPECTED_STOCK_ROW['volume']

    def test_valid_response_returns_correct_latest_day(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert rows[0]['latest_day'] == EXPECTED_STOCK_ROW['latest_day']

    def test_request_id_is_attached_to_row(self):
        rows, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=999, logger=make_mock_logger())
        assert err is None
        assert rows[0]['request_id'] == 999

    def test_price_is_cast_to_float(self):
        rows, _, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert isinstance(rows[0]['price'], float)

    def test_volume_is_cast_to_int(self):
        rows, _, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert isinstance(rows[0]['volume'], int)

    # --- Entities ---

    def test_returns_one_entity(self):
        _, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert len(entities) == 1

    def test_entity_type_is_stock_symbol(self):
        _, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert entities[0]['entity_type'] == 'stock_symbol'

    def test_entity_value_matches_symbol(self):
        _, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert entities[0]['entity_value'] == 'IBM'

    def test_entity_has_request_id(self):
        _, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=999, logger=make_mock_logger())
        assert err is None
        assert entities[0]['request_id'] == 999

    def test_entity_has_id_field(self):
        _, entities, err = parse_stock(VALID_AV_RESPONSE, request_id=1, logger=make_mock_logger())
        assert err is None
        assert 'id' in entities[0]

    # --- API-level pseudo errors (Alpha Vantage returns 200 OK for these) ---

    def test_rate_limit_note_returns_error(self):
        rows, entities, err = parse_stock(AV_RATE_LIMIT_RESPONSE, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert entities is None
        assert 'API Limit' in err

    def test_error_message_key_returns_error(self):
        rows, entities, err = parse_stock(AV_ERROR_RESPONSE, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert entities is None
        assert 'API Error' in err

    def test_empty_global_quote_returns_error(self):
        rows, entities, err = parse_stock(AV_EMPTY_QUOTE_RESPONSE, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert entities is None
        assert err      is not None

    # --- Required fields ---

    def test_missing_global_quote_key_returns_error(self):
        bad_data            = remove_key(VALID_AV_RESPONSE, 'Global Quote')
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_missing_symbol_returns_error(self):
        bad_data            = remove_quote_key(VALID_AV_RESPONSE, '01. symbol')
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_missing_price_returns_error(self):
        bad_data            = remove_quote_key(VALID_AV_RESPONSE, '05. price')
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_missing_volume_returns_error(self):
        bad_data            = remove_quote_key(VALID_AV_RESPONSE, '06. volume')
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_missing_latest_day_returns_error(self):
        bad_data            = remove_quote_key(VALID_AV_RESPONSE, '07. latest trading day')
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    # --- Bad types ---

    def test_none_response_returns_error(self):
        rows, entities, err = parse_stock(None, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_empty_dict_returns_error(self):
        rows, entities, err = parse_stock({}, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_non_numeric_price_returns_error(self):
        bad_data = copy.deepcopy(VALID_AV_RESPONSE)
        bad_data['Global Quote']['05. price'] = 'not_a_number'
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None

    def test_non_numeric_volume_returns_error(self):
        bad_data = copy.deepcopy(VALID_AV_RESPONSE)
        bad_data['Global Quote']['06. volume'] = 'not_a_number'
        rows, entities, err = parse_stock(bad_data, request_id=1, logger=make_mock_logger())
        assert rows     is None
        assert err      is not None


# ===================================================================
# 5. End-to-End Pipeline Tests
# ===================================================================
class TestAlphaVantagePipeline:
    """
    Full run_pipeline() tests with av_api module passed in.
    All BigQuery and HTTP calls are mocked.
    """

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_successful_run_inserts_all_tables(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_requests' in t for t in tables)
        assert any('raw_data'     in t for t in tables)
        assert any('stock_data'   in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_table_used_from_meta(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        expected_table = get_api_meta()['table']
        tables         = get_inserted_tables(client)
        assert any(expected_table in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_endpoint_logged_in_api_request(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'api_requests' in table:
                assert rows[0]['endpoint']  == get_api_meta()['endpoint']
                assert rows[0]['source_id'] == get_api_meta()['source_id']

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(401, text='Unauthorized')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_does_not_insert_stock_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(500, text='Server Error')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert not any('stock_data' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get', side_effect=Timeout())
    @patch('pipeline.runner.get_bq_client')
    def test_timeout_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get', side_effect=ConnectionError())
    @patch('pipeline.runner.get_bq_client')
    def test_connection_error_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_malformed_json_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_resp                    = make_mock_response(200)
        mock_resp.json.side_effect   = ValueError('No JSON')
        mock_get.return_value        = mock_resp
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_rate_limit_response_logs_to_api_errors(self, mock_get_client, mock_get):
        """Alpha Vantage rate limit is a 200 OK with a Note key — must still log error."""
        mock_get.return_value        = make_mock_response(200, json_data=AV_RATE_LIMIT_RESPONSE)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors'  in t for t in tables)
        assert not any('stock_data' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_error_message_response_logs_to_api_errors(self, mock_get_client, mock_get):
        """Alpha Vantage invalid call is a 200 OK with Error Message key — must still log error."""
        mock_get.return_value        = make_mock_response(200, json_data=AV_ERROR_RESPONSE)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('api_errors'     in t for t in tables)
        assert not any('stock_data' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_inserted_before_stock_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables      = get_inserted_tables(client)
        raw_index   = next(i for i, t in enumerate(tables) if 'raw_data'   in t)
        stock_index = next(i for i, t in enumerate(tables) if 'stock_data' in t)
        assert raw_index < stock_index

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_api_requests_is_first_insert(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        assert 'api_requests' in client.insert_rows_json.call_args_list[0].args[0]

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_pipeline_aborts_if_api_request_insert_fails(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],  # api_requests fails
            [],                                                   # raw_data
            [],                                                   # stock_data
        ]
        mock_get_client.return_value = client

        run_pipeline(av_api)

        assert client.insert_rows_json.call_count == 1

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_failure_does_not_stop_stock_insert(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [],                                                   # api_requests
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],   # raw_data fails
            [],                                                    # stock_data
        ]
        mock_get_client.return_value = client

        run_pipeline(av_api)

        tables = get_inserted_tables(client)
        assert any('stock_data' in t for t in tables)

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_stock_data_inserted_as_single_row(self, mock_get_client, mock_get):
        """Alpha Vantage GLOBAL_QUOTE always returns exactly one stock row."""
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'stock_data' in table:
                assert len(rows) == 1

    @patch('apis.alpha_vantage.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_entity_inserted_with_stock_symbol(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(av_api)

        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'extracted_entities' in table:
                assert rows[0]['entity_type']  == 'stock_symbol'
                assert rows[0]['entity_value'] == 'IBM'