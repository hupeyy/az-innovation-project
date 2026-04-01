# tests/test_runner.py

import pytest
from unittest.mock import patch, MagicMock
from pipeline.runner import run_pipeline


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def mock_api():
    """A fully working mock API module."""
    api = MagicMock()
    api.get_api_meta.return_value = {
        'api_name': 'weather',
        'source_id': 1,
        'endpoint': 'https://api.openweathermap.org/test',
        'table': 'weather_data'
    }
    
    # Successful fetch response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.elapsed.total_seconds.return_value = 0.25
    mock_response.json.return_value = {'temp': 72}
    api.fetch.return_value = (mock_response, None)
    
    # Successful parse
    api.parse.return_value = ([{'id': 1, 'temp': 72}], None)
    api.get_raw_row.return_value = {'id': 1, 'raw': '{}'}
    
    return api


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv('GMAIL_ADDRESS', 'test@gmail.com')
    monkeypatch.setenv('GMAIL_APP_PASSWORD', 'fakepassword1234')
    monkeypatch.setenv('ALERT_EMAIL', 'alert@gmail.com')


# ─────────────────────────────────────────────
# Success path — no notifications expected
# ─────────────────────────────────────────────

class TestRunnerSuccessNotifications:

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_pipeline_summary')
    def test_summary_not_sent_on_success(
        self, mock_summary, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """Summary email should NOT fire when pipeline succeeds."""
        run_pipeline(mock_api)
        mock_summary.assert_not_called()

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_email_alert')
    def test_alert_not_sent_on_success(
        self, mock_alert, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """No alert emails should fire on a clean successful run."""
        run_pipeline(mock_api)
        mock_alert.assert_not_called()


# ─────────────────────────────────────────────
# Error paths — notifications expected
# ─────────────────────────────────────────────

class TestRunnerErrorNotifications:

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_pipeline_summary')
    def test_summary_sent_on_fetch_failure(
        self, mock_summary, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """Summary email SHOULD fire when fetch fails."""
        mock_api.fetch.return_value = (None, "Connection timeout")

        run_pipeline(mock_api)

        mock_summary.assert_called_once()

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_pipeline_summary')
    def test_summary_sent_on_parse_failure(
        self, mock_summary, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """Summary email SHOULD fire when parse fails."""
        mock_api.parse.return_value = (None, "Unexpected API structure")

        run_pipeline(mock_api)

        mock_summary.assert_called_once()

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_email_alert')
    def test_alert_sent_on_fetch_failure(
        self, mock_alert, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """Alert email SHOULD fire immediately when fetch fails."""
        mock_api.fetch.return_value = (None, "Connection timeout")

        run_pipeline(mock_api)

        mock_alert.assert_called()
        # Verify the subject mentions the failure
        subject = mock_alert.call_args[0][0]
        assert "Fetch Failed" in subject

    @patch('pipeline.runner.insert_rows', return_value=True)
    @patch('pipeline.runner.get_bq_client')
    @patch('pipeline.runner.validate_env')
    @patch('pipeline.runner.send_email_alert')
    def test_alert_sent_on_http_error(
        self, mock_alert, mock_validate, mock_bq, mock_insert, mock_api, mock_env_vars
    ):
        """Alert email SHOULD fire on non-200 HTTP response."""
        mock_api.fetch.return_value[0].status_code = 429
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.elapsed.total_seconds.return_value = 0.1
        mock_response.text = "Rate limit exceeded"
        mock_api.fetch.return_value = (mock_response, None)

        run_pipeline(mock_api)

        mock_alert.assert_called()
        subject = mock_alert.call_args[0][0]
        assert "429" in subject


# ─────────────────────────────────────────────
# api_name not resolved — no summary expected
# ─────────────────────────────────────────────

class TestRunnerEarlyFailureNotifications:

    @patch('pipeline.runner.send_pipeline_summary')
    @patch('pipeline.runner.validate_env', side_effect=EnvironmentError("Missing API key"))
    def test_summary_not_sent_if_api_name_not_resolved(
        self, mock_validate, mock_summary
    ):
        """Summary should not fire if pipeline fails before api_name is set."""
        api = MagicMock()

        run_pipeline(api)

        mock_summary.assert_not_called()

    @patch('pipeline.runner.send_pipeline_summary')
    @patch('pipeline.runner.get_bq_client', side_effect=Exception("BQ unavailable"))
    @patch('pipeline.runner.validate_env')
    def test_summary_not_sent_if_bq_fails_before_api_name(
        self, mock_validate, mock_bq, mock_summary
    ):
        """Summary should not fire if BigQuery fails before api_name is resolved."""
        api = MagicMock()

        run_pipeline(api)

        mock_summary.assert_not_called()