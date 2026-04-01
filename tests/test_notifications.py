# tests/test_notifications.py

import pytest
from unittest.mock import patch, MagicMock, call
import os
from pipeline.notifications import send_email_alert, send_pipeline_summary


# ─────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set fake email credentials for all tests."""
    monkeypatch.setenv('GMAIL_ADDRESS', 'test@gmail.com')
    monkeypatch.setenv('GMAIL_APP_PASSWORD', 'fakepassword1234')
    monkeypatch.setenv('ALERT_EMAIL', 'alert@gmail.com')


@pytest.fixture
def missing_env_vars(monkeypatch):
    """Simulate unconfigured email environment."""
    monkeypatch.delenv('GMAIL_ADDRESS', raising=False)
    monkeypatch.delenv('GMAIL_APP_PASSWORD', raising=False)
    monkeypatch.delenv('ALERT_EMAIL', raising=False)


# ─────────────────────────────────────────────
# send_email_alert() Tests
# ─────────────────────────────────────────────

class TestSendEmailAlert:

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_returns_true_on_success(self, mock_smtp, mock_env_vars):
        """Email sends successfully and returns True."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        result = send_email_alert("Test Subject", "Test message body")

        assert result is True

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_smtp_called_with_correct_server(self, mock_smtp, mock_env_vars):
        """Verify Gmail SMTP server and port are used."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert("Test", "Message")

        mock_smtp.assert_called_once_with('smtp.gmail.com', 587)

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_smtp_login_uses_env_credentials(self, mock_smtp, mock_env_vars):
        """Verify login uses credentials from environment variables."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert("Test", "Message")

        mock_server.login.assert_called_once_with(
            'test@gmail.com',
            'fakepassword1234'
        )

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_starttls_is_called(self, mock_smtp, mock_env_vars):
        """Verify TLS encryption is initiated."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert("Test", "Message")

        mock_server.starttls.assert_called_once()

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_subject_contains_pipeline_alert_prefix(self, mock_smtp, mock_env_vars):
        """Email subject should be prefixed with [Pipeline Alert]."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert("API Down", "Weather API not responding")

        # Grab the message that was sent
        sent_message = mock_server.send_message.call_args[0][0]
        assert sent_message['Subject'] == '[Pipeline Alert] API Down'

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_email_body_contains_message(self, mock_smtp, mock_env_vars):
        """The message content should appear in the email body."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert("Test", "BigQuery load failed")

        sent_message = mock_server.send_message.call_args[0][0]
        # Decode the email payload to check body content
        body = sent_message.get_payload(0).get_payload()
        assert "BigQuery load failed" in body

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_error_details_included_in_body(self, mock_smtp, mock_env_vars):
        """Error details dict should be appended to the email body."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        send_email_alert(
            "API Error",
            "NewsAPI failed",
            error_details={"status_code": 429, "api": "NewsAPI"}
        )

        sent_message = mock_server.send_message.call_args[0][0]
        body = sent_message.get_payload(0).get_payload()
        assert "status_code" in body
        assert "429" in body
        assert "NewsAPI" in body

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_no_error_details_still_sends(self, mock_smtp, mock_env_vars):
        """Email sends fine when error_details is None (default)."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server

        result = send_email_alert("Test", "Message with no error details")

        assert result is True
        mock_server.send_message.assert_called_once()

    def test_returns_false_when_credentials_missing(self, missing_env_vars):
        """Should return False and skip sending when env vars not set."""
        result = send_email_alert("Test", "Message")
        assert result is False

    def test_no_smtp_call_when_credentials_missing(self, missing_env_vars):
        """SMTP should never be called when credentials are missing."""
        with patch('pipeline.notifications.smtplib.SMTP') as mock_smtp:
            send_email_alert("Test", "Message")
            mock_smtp.assert_not_called()

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_returns_false_on_smtp_exception(self, mock_smtp, mock_env_vars):
        """Should catch SMTP exceptions and return False instead of crashing."""
        mock_smtp.side_effect = Exception("Connection refused")

        result = send_email_alert("Test", "Message")

        assert result is False

    @patch('pipeline.notifications.smtplib.SMTP')
    def test_returns_false_on_login_failure(self, mock_smtp, mock_env_vars):
        """Should handle bad password gracefully."""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        mock_server.login.side_effect = Exception("Invalid credentials")

        result = send_email_alert("Test", "Message")

        assert result is False


# ─────────────────────────────────────────────
# send_pipeline_summary() Tests
# ─────────────────────────────────────────────

class TestSendPipelineSummary:

    @patch('pipeline.notifications.send_email_alert')
    def test_calls_send_email_alert(self, mock_alert, mock_env_vars):
        """Summary function should delegate to send_email_alert."""
        send_pipeline_summary("Weather", records_loaded=10, errors=0, duration=3.5)
        mock_alert.assert_called_once()

    @patch('pipeline.notifications.send_email_alert')
    def test_success_status_when_no_errors(self, mock_alert, mock_env_vars):
        """Subject should contain SUCCESS when errors=0."""
        send_pipeline_summary("Weather", records_loaded=10, errors=0, duration=3.5)

        subject = mock_alert.call_args[0][0]
        assert "SUCCESS" in subject

    @patch('pipeline.notifications.send_email_alert')
    def test_error_status_when_errors_present(self, mock_alert, mock_env_vars):
        """Subject should indicate errors when errors > 0."""
        send_pipeline_summary("News", records_loaded=5, errors=2, duration=1.2)

        subject = mock_alert.call_args[0][0]
        assert "ERRORS" in subject or "ERROR" in subject

    @patch('pipeline.notifications.send_email_alert')
    def test_api_name_in_subject(self, mock_alert, mock_env_vars):
        """API name should appear in the email subject."""
        send_pipeline_summary("Weather", records_loaded=1, errors=0, duration=2.0)

        subject = mock_alert.call_args[0][0]
        assert "Weather" in subject

    @patch('pipeline.notifications.send_email_alert')
    def test_records_loaded_in_message(self, mock_alert, mock_env_vars):
        """Records loaded count should appear in the message body."""
        send_pipeline_summary("News", records_loaded=42, errors=0, duration=1.5)

        message = mock_alert.call_args[0][1]
        assert "42" in message

    @patch('pipeline.notifications.send_email_alert')
    def test_duration_in_message(self, mock_alert, mock_env_vars):
        """Duration should appear formatted in the message body."""
        send_pipeline_summary("Weather", records_loaded=1, errors=0, duration=4.567)

        message = mock_alert.call_args[0][1]
        assert "4.57" in message  # Matches {duration:.2f} formatting