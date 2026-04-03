# tests/test_news.py

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['NEWS_API_KEY']    = 'test_api_key'
os.environ['GCP_PROJECT_ID']  = 'test_project'

import pytest
import json
import copy
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from requests.exceptions import ConnectionError, Timeout, RequestException

from apis.news.fetch    import fetch_news
from apis.news.parse    import parse_news
from apis.news.pipeline import get_api_meta, get_raw_row
import apis.news.pipeline as news_api
from pipeline.runner    import run_pipeline


# -------------------------------------------------------------------
# Shared Fixtures & Helpers
# -------------------------------------------------------------------

VALID_NEWS_RESPONSE = {
    'status': 'ok',
    'totalResults': 2,
    'articles': [
        {
            'source': {'id': 'bbc-news', 'name': 'BBC News'},
            'author': 'Jane Smith',
            'title': 'Breaking: Major Event Unfolds',
            'description': 'A major event is currently unfolding downtown.',
            'url': 'https://bbc.com/news/article-1',
            'urlToImage': 'https://bbc.com/images/article-1.jpg',
            'publishedAt': '2024-06-15T14:30:00Z',
            'content': 'Full content of the article goes here...'
        },
        {
            'source': {'id': 'cnn', 'name': 'CNN'},
            'author': 'John Doe',
            'title': 'Tech Company Announces New Product',
            'description': 'A leading tech company revealed its latest product.',
            'url': 'https://cnn.com/tech/article-2',
            'urlToImage': 'https://cnn.com/images/article-2.jpg',
            'publishedAt': '2024-06-15T12:00:00Z',
            'content': 'The company said in a statement...'
        }
    ]
}

EXPECTED_NEWS_ROWS = [
    {
        'source_name':  'BBC News',
        'author':       'Jane Smith',
        'title':        'Breaking: Major Event Unfolds',
        'description':  'A major event is currently unfolding downtown.',
        'url':          'https://bbc.com/news/article-1',
        'image_url':    'https://bbc.com/images/article-1.jpg',
        'published_at': '2024-06-15T14:30:00Z',
        'content':      'Full content of the article goes here...'
    },
    {
        'source_name':  'CNN',
        'author':       'John Doe',
        'title':        'Tech Company Announces New Product',
        'description':  'A leading tech company revealed its latest product.',
        'url':          'https://cnn.com/tech/article-2',
        'image_url':    'https://cnn.com/images/article-2.jpg',
        'published_at': '2024-06-15T12:00:00Z',
        'content':      'The company said in a statement...'
    }
]


def make_mock_response(status_code=200, json_data=None, text='', elapsed_ms=120):
    mock_resp                               = MagicMock()
    mock_resp.status_code                   = status_code
    mock_resp.text                          = text
    mock_resp.json.return_value             = json_data if json_data is not None else VALID_NEWS_RESPONSE
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
    return {k: v for k, v in d.items() if k != key}


def remove_article_key(data, key):
    """Remove a top-level key from every article in the response."""
    d = copy.deepcopy(data)
    for article in d['articles']:
        del article[key]
    return d


def remove_nested_article_key(data, path):
    """Remove a nested key from every article. e.g. path=['source', 'name']"""
    d = copy.deepcopy(data)
    for article in d['articles']:
        ref = article
        for p in path[:-1]:
            ref = ref[p]
        del ref[path[-1]]
    return d


def get_inserted_tables(client):
    return [
        call_args.args[0]
        for call_args in client.insert_rows_json.call_args_list
    ]


# ===================================================================
# 1. API Meta Tests
# ===================================================================
class TestNewsApiMeta:
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

    def test_meta_contains_api_name(self):
        meta = get_api_meta()
        assert 'api_name' in meta

    def test_source_id_is_correct(self):
        meta = get_api_meta()
        assert meta['source_id'] == 2

    def test_endpoint_is_correct(self):
        meta = get_api_meta()
        assert meta['endpoint'] == '/v2/top-headlines'

    def test_table_is_correct(self):
        meta = get_api_meta()
        assert meta['table'] == 'news_data'

    def test_api_name_is_correct(self):
        meta = get_api_meta()
        assert meta['api_name'] == 'news'


# ===================================================================
# 2. Raw Row Tests
# ===================================================================
class TestNewsRawRow:
    """
    Tests for get_raw_row().
    Ensures the raw response is always stored correctly.
    """

    def test_raw_row_contains_request_id(self):
        raw_row = get_raw_row(VALID_NEWS_RESPONSE, request_id=999)
        assert raw_row['request_id'] == 999

    def test_raw_row_id_matches_request_id(self):
        raw_row = get_raw_row(VALID_NEWS_RESPONSE, request_id=999)
        assert raw_row['id'] == 999

    def test_raw_data_is_valid_json_string(self):
        raw_row = get_raw_row(VALID_NEWS_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        assert parsed == VALID_NEWS_RESPONSE

    def test_raw_data_preserves_all_fields(self):
        raw_row = get_raw_row(VALID_NEWS_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        for key in VALID_NEWS_RESPONSE:
            assert key in parsed

    def test_raw_data_preserves_all_articles(self):
        raw_row = get_raw_row(VALID_NEWS_RESPONSE, request_id=999)
        parsed  = json.loads(raw_row['raw_data'])
        assert len(parsed['articles']) == len(VALID_NEWS_RESPONSE['articles'])


# ===================================================================
# 3. Fetch Tests
# ===================================================================
class TestFetchNews:
    """Tests for fetch_news() in isolation."""

    @patch('apis.news.fetch.requests.get')
    def test_returns_response_on_success(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        response, error       = fetch_news('test_key', make_mock_logger())
        assert response       is not None
        assert error          is None

    @patch('apis.news.fetch.requests.get')
    def test_returns_response_on_401(self, mock_get):
        mock_get.return_value = make_mock_response(401, text='Unauthorized')
        response, error       = fetch_news('bad_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 401

    @patch('apis.news.fetch.requests.get')
    def test_returns_response_on_429(self, mock_get):
        mock_get.return_value = make_mock_response(429, text='Too Many Requests')
        response, error       = fetch_news('test_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 429

    @patch('apis.news.fetch.requests.get')
    def test_returns_response_on_500(self, mock_get):
        mock_get.return_value = make_mock_response(500, text='Server Error')
        response, error       = fetch_news('test_key', make_mock_logger())
        assert response       is not None
        assert response.status_code == 500

    @patch('apis.news.fetch.requests.get', side_effect=ConnectionError('No network'))
    def test_connection_error_returns_none(self, mock_get):
        response, error = fetch_news('test_key', make_mock_logger())
        assert response is None
        assert 'Connection error' in error

    @patch('apis.news.fetch.requests.get', side_effect=Timeout())
    def test_timeout_returns_none(self, mock_get):
        response, error = fetch_news('test_key', make_mock_logger())
        assert response is None
        assert 'timed out' in error

    @patch('apis.news.fetch.requests.get', side_effect=RequestException('Failure'))
    def test_request_exception_returns_none(self, mock_get):
        response, error = fetch_news('test_key', make_mock_logger())
        assert response is None
        assert 'Unexpected request error' in error

    @patch('apis.news.fetch.requests.get')
    def test_api_key_sent(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_news('test_key', make_mock_logger())
        sent_params           = mock_get.call_args.kwargs['params']
        assert sent_params['apiKey'] == 'test_key'

    @patch('apis.news.fetch.requests.get')
    def test_country_param_sent(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_news('test_key', make_mock_logger())
        sent_params           = mock_get.call_args.kwargs['params']
        assert 'country' in sent_params

    @patch('apis.news.fetch.requests.get')
    def test_timeout_is_enforced(self, mock_get):
        mock_get.return_value = make_mock_response(200)
        fetch_news('test_key', make_mock_logger())
        sent_kwargs           = mock_get.call_args.kwargs
        assert 'timeout' in sent_kwargs
        assert sent_kwargs['timeout'] > 0


# ===================================================================
# 4. Parse Tests
# ===================================================================
class TestParseNews:
    """Tests for parse_news() in isolation."""

    # --- Happy path ---

    def test_valid_response_returns_correct_row_count(self):
        rows, entities, error = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        assert error is None
        assert len(rows) == 2

    def test_valid_response_returns_correct_fields(self):
        rows, entities, error = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        assert error is None
        for i, row in enumerate(rows):
            for field, expected in EXPECTED_NEWS_ROWS[i].items():
                assert row[field] == expected, f'Mismatch on article {i}, field: {field}'

    def test_request_id_is_attached_to_every_row(self):
        rows, _, error = parse_news(VALID_NEWS_RESPONSE, request_id=999, logger=make_mock_logger())
        assert error is None
        for row in rows:
            assert row['request_id'] == 999

    def test_source_name_extracted_from_nested_object(self):
        rows, _, _ = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        assert rows[0]['source_name'] == 'BBC News'
        assert rows[1]['source_name'] == 'CNN'

    def test_image_url_maps_from_urlToImage(self):
        rows, _, _ = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        assert rows[0]['image_url'] == VALID_NEWS_RESPONSE['articles'][0]['urlToImage']

    # --- Entities ---

    def test_returns_entities_for_each_article(self):
        _, entities, error = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        assert error    is None
        assert len(entities) == 2

    def test_entity_type_is_source(self):
        _, entities, _ = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        for entity in entities:
            assert entity['entity_type'] == 'source'

    def test_entity_values_match_source_names(self):
        _, entities, _ = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        values = [e['entity_value'] for e in entities]
        assert 'BBC News' in values
        assert 'CNN'      in values

    def test_entity_has_request_id(self):
        _, entities, _ = parse_news(VALID_NEWS_RESPONSE, request_id=999, logger=make_mock_logger())
        for entity in entities:
            assert entity['request_id'] == 999

    def test_entity_has_id_field(self):
        _, entities, _ = parse_news(VALID_NEWS_RESPONSE, request_id=1, logger=make_mock_logger())
        for entity in entities:
            assert 'id' in entity

    # --- Nullable fields ---

    def test_missing_author_returns_none(self):
        data               = remove_article_key(VALID_NEWS_RESPONSE, 'author')
        rows, _, error     = parse_news(data, request_id=1, logger=make_mock_logger())
        assert error is None
        for row in rows:
            assert row['author'] is None

    def test_missing_description_returns_none(self):
        data               = remove_article_key(VALID_NEWS_RESPONSE, 'description')
        rows, _, error     = parse_news(data, request_id=1, logger=make_mock_logger())
        assert error is None
        for row in rows:
            assert row['description'] is None

    def test_missing_image_url_returns_none(self):
        data               = remove_article_key(VALID_NEWS_RESPONSE, 'urlToImage')
        rows, _, error     = parse_news(data, request_id=1, logger=make_mock_logger())
        assert error is None
        for row in rows:
            assert row['image_url'] is None

    def test_missing_content_returns_none(self):
        data               = remove_article_key(VALID_NEWS_RESPONSE, 'content')
        rows, _, error     = parse_news(data, request_id=1, logger=make_mock_logger())
        assert error is None
        for row in rows:
            assert row['content'] is None

    # --- Required fields ---

    def test_missing_articles_key_returns_error(self):
        bad_data              = remove_key(VALID_NEWS_RESPONSE, 'articles')
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_empty_articles_returns_error(self):
        data                  = copy.deepcopy(VALID_NEWS_RESPONSE)
        data['articles']      = []
        rows, entities, error = parse_news(data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_missing_source_returns_error(self):
        bad_data              = remove_article_key(VALID_NEWS_RESPONSE, 'source')
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_missing_source_name_returns_error(self):
        bad_data              = remove_nested_article_key(VALID_NEWS_RESPONSE, ['source', 'name'])
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_missing_title_returns_error(self):
        bad_data              = remove_article_key(VALID_NEWS_RESPONSE, 'title')
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_missing_url_returns_error(self):
        bad_data              = remove_article_key(VALID_NEWS_RESPONSE, 'url')
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_missing_published_at_returns_error(self):
        bad_data              = remove_article_key(VALID_NEWS_RESPONSE, 'publishedAt')
        rows, entities, error = parse_news(bad_data, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    # --- Bad types ---

    def test_none_response_returns_error(self):
        rows, entities, error = parse_news(None, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    def test_empty_response_returns_error(self):
        rows, entities, error = parse_news({}, request_id=1, logger=make_mock_logger())
        assert rows   is None
        assert error  is not None

    # --- Single article ---

    def test_single_article_returns_one_row(self):
        data             = copy.deepcopy(VALID_NEWS_RESPONSE)
        data['articles'] = [data['articles'][0]]
        rows, _, error   = parse_news(data, request_id=1, logger=make_mock_logger())
        assert error is None
        assert len(rows) == 1

    def test_single_article_returns_one_entity(self):
        data             = copy.deepcopy(VALID_NEWS_RESPONSE)
        data['articles'] = [data['articles'][0]]
        _, entities, _   = parse_news(data, request_id=1, logger=make_mock_logger())
        assert len(entities) == 1


# ===================================================================
# 5. End-to-End Pipeline Tests
# ===================================================================
class TestNewsPipeline:
    """
    Full run_pipeline() tests with news_api module passed in.
    All BigQuery and HTTP calls are mocked.
    """

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_successful_run_inserts_all_three_tables(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_requests' in t for t in tables)
        assert any('raw_data'     in t for t in tables)
        assert any('news_data'    in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_table_used_from_meta(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        expected_table = get_api_meta()['table']
        tables         = get_inserted_tables(client)
        assert any(expected_table in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_correct_endpoint_logged_in_api_request(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'api_requests' in table:
                assert rows[0]['endpoint']  == get_api_meta()['endpoint']
                assert rows[0]['source_id'] == get_api_meta()['source_id']

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(401, text='Unauthorized')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_non_200_does_not_insert_news_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(500, text='Server Error')
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert not any('news_data' in t for t in tables)

    @patch('apis.news.fetch.requests.get', side_effect=Timeout())
    @patch('pipeline.runner.get_bq_client')
    def test_timeout_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.news.fetch.requests.get', side_effect=ConnectionError())
    @patch('pipeline.runner.get_bq_client')
    def test_connection_error_logs_to_api_errors(self, mock_get_client, mock_get):
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_malformed_json_logs_to_api_errors(self, mock_get_client, mock_get):
        mock_resp                    = make_mock_response(200)
        mock_resp.json.side_effect   = ValueError('No JSON')
        mock_get.return_value        = mock_resp
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_empty_articles_logs_to_api_errors(self, mock_get_client, mock_get):
        empty_response               = {'status': 'ok', 'totalResults': 0, 'articles': []}
        mock_get.return_value        = make_mock_response(200, json_data=empty_response)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('api_errors' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_inserted_before_news_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables     = get_inserted_tables(client)
        raw_index  = next(i for i, t in enumerate(tables) if 'raw_data'  in t)
        news_index = next(i for i, t in enumerate(tables) if 'news_data' in t)
        assert raw_index < news_index

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_api_requests_is_first_insert(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        assert 'api_requests' in client.insert_rows_json.call_args_list[0].args[0]

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_pipeline_aborts_if_api_request_insert_fails(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],
            [],
            [],
        ]
        mock_get_client.return_value = client

        run_pipeline(news_api)

        assert client.insert_rows_json.call_count == 1

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_raw_data_failure_does_not_stop_news_insert(self, mock_get_client, mock_get):
        mock_get.return_value = make_mock_response(200)
        client                = MagicMock()
        client.insert_rows_json.side_effect = [
            [],                                                   # api_requests
            [{'index': 0, 'errors': [{'reason': 'invalid'}]}],   # raw_data fails
            [],                                                   # news_data
        ]
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('news_data' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_multiple_articles_inserted_as_multiple_rows(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        for call_args in client.insert_rows_json.call_args_list:
            table, rows = call_args.args
            if 'news_data' in table:
                assert len(rows) == 2

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_entities_inserted_for_each_article(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables = get_inserted_tables(client)
        assert any('extracted_entities' in t for t in tables)

    @patch('apis.news.fetch.requests.get')
    @patch('pipeline.runner.get_bq_client')
    def test_entities_inserted_after_news_data(self, mock_get_client, mock_get):
        mock_get.return_value        = make_mock_response(200)
        client                       = make_bq_client()
        mock_get_client.return_value = client

        run_pipeline(news_api)

        tables       = get_inserted_tables(client)
        news_index   = next(i for i, t in enumerate(tables) if 'news_data'          in t)
        entity_index = next(i for i, t in enumerate(tables) if 'extracted_entities' in t)
        assert news_index < entity_index