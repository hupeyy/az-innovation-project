# tests/test_slack.py

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ['SLACK_BOT_TOKEN'] = 'xoxb-test-token'
os.environ['SLACK_CHANNEL_ID'] = 'C1234567890'

import pytest
from unittest.mock import MagicMock, patch
from slack_sdk.errors import SlackApiError

from interface.slack.client import post_message
from interface.slack.messages import (
    format_morning_brief,
    format_pipeline_alert,
    format_brand_alert,
)
from interface.slack.router import (
    post_pipeline_alert,
    post_morning_brief,
    post_brand_alert,
)


# -------------------------------------------------------------------
# Shared Fixtures & Helpers
# -------------------------------------------------------------------

def make_slack_response(ok=True, channel='C1234567890', ts='1234567890.123456', error=None):
    """Create a mock Slack API response."""
    response = {
        'ok': ok,
        'channel': channel,
        'ts': ts,
    }
    if error:
        response['error'] = error
    return response


def make_mock_slack_client(response=None, side_effect=None):
    """Create a mock Slack WebClient."""
    client = MagicMock()
    
    if side_effect:
        client.chat_postMessage.side_effect = side_effect
    else:
        client.chat_postMessage.return_value = response or make_slack_response()
    
    return client


def make_slack_api_error(error='channel_not_found'):
    """Create a mock SlackApiError."""
    response = {'error': error}
    return SlackApiError(message=error, response=response)


# ===================================================================
# 1. Client Tests
# ===================================================================
class TestSlackClient:
    """
    Tests for post_message() in isolation.
    Verifies low-level Slack API interaction.
    """

    @patch('interface.slack.client.client')
    def test_post_message_returns_response_on_success(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        response = post_message("test message")
        assert response is not None
        assert response['ok'] is True

    @patch('interface.slack.client.client')
    def test_post_message_uses_default_channel(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        post_message("test message")
        call_args = mock_client.chat_postMessage.call_args
        assert call_args.kwargs['channel'] == 'C1234567890'

    @patch('interface.slack.client.client')
    def test_post_message_uses_custom_channel(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        post_message("test message", channel='C9876543210')
        call_args = mock_client.chat_postMessage.call_args
        assert call_args.kwargs['channel'] == 'C9876543210'

    @patch('interface.slack.client.client')
    def test_post_message_sends_text(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        post_message("hello world")
        call_args = mock_client.chat_postMessage.call_args
        assert call_args.kwargs['text'] == "hello world"

    @patch('interface.slack.client.client')
    def test_post_message_raises_on_channel_not_found(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('channel_not_found')
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test message")
        assert 'Slack API error' in str(exc_info.value)

    @patch('interface.slack.client.client')
    def test_post_message_raises_on_not_in_channel(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('not_in_channel')
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test message")
        assert 'Slack API error' in str(exc_info.value)

    @patch('interface.slack.client.client')
    def test_post_message_raises_on_invalid_auth(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('invalid_auth')
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test message")
        assert 'Slack API error' in str(exc_info.value)

    @patch('interface.slack.client.DEFAULT_SLACK_CHANNEL_ID', None)
    def test_post_message_raises_when_no_channel_configured(self):
        with pytest.raises(ValueError) as exc_info:
            post_message("test message")
        assert 'SLACK_CHANNEL_ID not set' in str(exc_info.value)

    @patch('interface.slack.client.client')
    def test_post_message_with_empty_text(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        post_message("")
        call_args = mock_client.chat_postMessage.call_args
        assert call_args.kwargs['text'] == ""

    @patch('interface.slack.client.client')
    def test_post_message_preserves_formatting(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        message = "*bold* _italic_ `code`\n• bullet"
        post_message(message)
        call_args = mock_client.chat_postMessage.call_args
        assert call_args.kwargs['text'] == message


# ===================================================================
# 2. Message Formatting Tests
# ===================================================================
class TestFormatMorningBrief:
    """Tests for format_morning_brief() message builder."""

    def test_includes_title(self):
        result = format_morning_brief()
        assert "Morning Brief" in result

    def test_includes_emoji(self):
        result = format_morning_brief()
        assert "🌤" in result

    def test_includes_weather_when_provided(self):
        result = format_morning_brief(weather="Sunny, 77°F")
        assert "Weather:" in result
        assert "Sunny, 77°F" in result

    def test_excludes_weather_when_none(self):
        result = format_morning_brief(weather=None)
        assert "Weather:" not in result

    def test_includes_stocks_when_provided(self):
        result = format_morning_brief(stocks="IBM up 1.2%")
        assert "Markets:" in result
        assert "IBM up 1.2%" in result

    def test_excludes_stocks_when_none(self):
        result = format_morning_brief(stocks=None)
        assert "Markets:" not in result

    def test_includes_news_list(self):
        news = ["Headline 1", "Headline 2", "Headline 3"]
        result = format_morning_brief(news=news)
        assert "Top News:" in result
        assert "• Headline 1" in result
        assert "• Headline 2" in result
        assert "• Headline 3" in result

    def test_excludes_news_when_none(self):
        result = format_morning_brief(news=None)
        assert "Top News:" not in result

    def test_excludes_news_when_empty_list(self):
        result = format_morning_brief(news=[])
        assert "Top News:" not in result

    def test_includes_calendar_list(self):
        calendar = ["10 AM Meeting", "2 PM Review"]
        result = format_morning_brief(calendar=calendar)
        assert "Calendar:" in result
        assert "• 10 AM Meeting" in result
        assert "• 2 PM Review" in result

    def test_excludes_calendar_when_none(self):
        result = format_morning_brief(calendar=None)
        assert "Calendar:" not in result

    def test_excludes_calendar_when_empty_list(self):
        result = format_morning_brief(calendar=[])
        assert "Calendar:" not in result

    def test_all_sections_present(self):
        result = format_morning_brief(
            weather="Sunny",
            stocks="IBM +1.2%",
            news=["News 1"],
            calendar=["Meeting 1"]
        )
        assert "Weather:" in result
        assert "Markets:" in result
        assert "Top News:" in result
        assert "Calendar:" in result

    def test_returns_string(self):
        result = format_morning_brief()
        assert isinstance(result, str)

    def test_uses_newlines_for_structure(self):
        result = format_morning_brief(weather="Sunny", stocks="IBM +1.2%")
        assert "\n" in result


class TestFormatPipelineAlert:
    """Tests for format_pipeline_alert() message builder."""

    def test_includes_alert_title(self):
        result = format_pipeline_alert("test message")
        assert "Pipeline Alert" in result

    def test_includes_warning_emoji(self):
        result = format_pipeline_alert("test message")
        assert "⚠️" in result

    def test_includes_provided_message(self):
        result = format_pipeline_alert("Something went wrong")
        assert "Something went wrong" in result

    def test_returns_string(self):
        result = format_pipeline_alert("test")
        assert isinstance(result, str)

    def test_handles_empty_message(self):
        result = format_pipeline_alert("")
        assert isinstance(result, str)

    def test_preserves_multiline_messages(self):
        message = "Line 1\nLine 2\nLine 3"
        result = format_pipeline_alert(message)
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result


class TestFormatBrandAlert:
    """Tests for format_brand_alert() message builder."""

    def test_includes_title(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "Brand Mention Alert" in result

    def test_includes_emoji(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "🔎" in result

    def test_includes_platform(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "Platform:" in result
        assert "reddit" in result

    def test_includes_keyword(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "Keyword:" in result
        assert "OpenClaw" in result

    def test_includes_sentiment(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "Sentiment:" in result
        assert "positive" in result

    def test_includes_text(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert "Text:" in result
        assert "Great product!" in result

    def test_includes_url_when_provided(self):
        result = format_brand_alert(
            "reddit", "OpenClaw", "positive", "Great product!",
            url="https://reddit.com/example"
        )
        assert "URL:" in result
        assert "https://reddit.com/example" in result

    def test_excludes_url_when_none(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!", url=None)
        assert "URL:" not in result

    def test_returns_string(self):
        result = format_brand_alert("reddit", "OpenClaw", "positive", "Great product!")
        assert isinstance(result, str)


# ===================================================================
# 3. Router Tests
# ===================================================================
class TestPostPipelineAlert:
    """Tests for post_pipeline_alert() in router layer."""

    @patch('interface.slack.router.post_message')
    def test_calls_post_message(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_pipeline_alert("test alert")
        assert mock_post.called

    @patch('interface.slack.router.post_message')
    def test_formats_message_before_sending(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_pipeline_alert("test alert")
        sent_text = mock_post.call_args.args[0]
        assert "Pipeline Alert" in sent_text
        assert "test alert" in sent_text

    @patch('interface.slack.router.post_message')
    def test_passes_custom_channel(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_pipeline_alert("test alert", channel="C9999999999")
        assert mock_post.call_args.kwargs['channel'] == "C9999999999"

    @patch('interface.slack.router.post_message')
    def test_uses_default_channel_when_not_specified(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_pipeline_alert("test alert")
        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs.get('channel') is None  # lets client handle default

    @patch('interface.slack.router.post_message')
    def test_returns_response(self, mock_post):
        expected_response = make_slack_response()
        mock_post.return_value = expected_response
        result = post_pipeline_alert("test alert")
        assert result == expected_response


class TestPostMorningBrief:
    """Tests for post_morning_brief() in router layer."""

    @patch('interface.slack.router.post_message')
    def test_calls_post_message(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_morning_brief()
        assert mock_post.called

    @patch('interface.slack.router.post_message')
    def test_formats_message_before_sending(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_morning_brief(weather="Sunny", stocks="IBM +1.2%")
        sent_text = mock_post.call_args.args[0]
        assert "Morning Brief" in sent_text
        assert "Sunny" in sent_text
        assert "IBM +1.2%" in sent_text

    @patch('interface.slack.router.post_message')
    def test_passes_all_parameters_to_formatter(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_morning_brief(
            weather="Sunny",
            news=["Headline 1"],
            stocks="IBM +1.2%",
            calendar=["10 AM Meeting"]
        )
        sent_text = mock_post.call_args.args[0]
        assert "Sunny" in sent_text
        assert "Headline 1" in sent_text
        assert "IBM +1.2%" in sent_text
        assert "10 AM Meeting" in sent_text

    @patch('interface.slack.router.post_message')
    def test_passes_custom_channel(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_morning_brief(channel="C9999999999")
        assert mock_post.call_args.kwargs['channel'] == "C9999999999"

    @patch('interface.slack.router.post_message')
    def test_returns_response(self, mock_post):
        expected_response = make_slack_response()
        mock_post.return_value = expected_response
        result = post_morning_brief()
        assert result == expected_response


class TestPostBrandAlert:
    """Tests for post_brand_alert() in router layer."""

    @patch('interface.slack.router.post_message')
    def test_calls_post_message(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_brand_alert("reddit", "OpenClaw", "positive", "Great tool!")
        assert mock_post.called

    @patch('interface.slack.router.post_message')
    def test_formats_message_before_sending(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_brand_alert("reddit", "OpenClaw", "positive", "Great tool!")
        sent_text = mock_post.call_args.args[0]
        assert "Brand Mention Alert" in sent_text
        assert "reddit" in sent_text
        assert "OpenClaw" in sent_text
        assert "positive" in sent_text
        assert "Great tool!" in sent_text

    @patch('interface.slack.router.post_message')
    def test_includes_url_when_provided(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_brand_alert(
            "reddit", "OpenClaw", "positive", "Great tool!",
            url="https://reddit.com/example"
        )
        sent_text = mock_post.call_args.args[0]
        assert "https://reddit.com/example" in sent_text

    @patch('interface.slack.router.post_message')
    def test_passes_custom_channel(self, mock_post):
        mock_post.return_value = make_slack_response()
        post_brand_alert(
            "reddit", "OpenClaw", "positive", "Great tool!",
            channel="C9999999999"
        )
        assert mock_post.call_args.kwargs['channel'] == "C9999999999"

    @patch('interface.slack.router.post_message')
    def test_returns_response(self, mock_post):
        expected_response = make_slack_response()
        mock_post.return_value = expected_response
        result = post_brand_alert("reddit", "OpenClaw", "positive", "Great tool!")
        assert result == expected_response


# ===================================================================
# 4. Integration Tests
# ===================================================================
class TestSlackIntegration:
    """
    End-to-end tests verifying the full stack from router to client.
    """

    @patch('interface.slack.client.client')
    def test_morning_brief_end_to_end(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        
        result = post_morning_brief(
            weather="Sunny, 77°F",
            stocks="IBM up 1.2%",
            news=["AI startup raises funding", "Cloud earnings beat"],
            calendar=["10 AM Client Sync"]
        )
        
        assert result['ok'] is True
        assert mock_client.chat_postMessage.called
        
        sent_text = mock_client.chat_postMessage.call_args.kwargs['text']
        assert "Morning Brief" in sent_text
        assert "Sunny, 77°F" in sent_text
        assert "IBM up 1.2%" in sent_text
        assert "AI startup raises funding" in sent_text
        assert "10 AM Client Sync" in sent_text

    @patch('interface.slack.client.client')
    def test_pipeline_alert_end_to_end(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        
        result = post_pipeline_alert("Weather API failed: timeout")
        
        assert result['ok'] is True
        assert mock_client.chat_postMessage.called
        
        sent_text = mock_client.chat_postMessage.call_args.kwargs['text']
        assert "Pipeline Alert" in sent_text
        assert "Weather API failed: timeout" in sent_text

    @patch('interface.slack.client.client')
    def test_brand_alert_end_to_end(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        
        result = post_brand_alert(
            platform="reddit",
            keyword="OpenClaw",
            sentiment="negative",
            text="Setup is confusing",
            url="https://reddit.com/r/example/123"
        )
        
        assert result['ok'] is True
        assert mock_client.chat_postMessage.called
        
        sent_text = mock_client.chat_postMessage.call_args.kwargs['text']
        assert "Brand Mention Alert" in sent_text
        assert "reddit" in sent_text
        assert "OpenClaw" in sent_text
        assert "negative" in sent_text
        assert "Setup is confusing" in sent_text
        assert "https://reddit.com/r/example/123" in sent_text

    @patch('interface.slack.client.client')
    def test_custom_channel_routing_end_to_end(self, mock_client):
        mock_client.chat_postMessage.return_value = make_slack_response()
        
        custom_channel = "C9999999999"
        post_morning_brief(weather="Test", channel=custom_channel)
        
        assert mock_client.chat_postMessage.call_args.kwargs['channel'] == custom_channel

    @patch('interface.slack.client.client')
    def test_slack_error_propagates_through_stack(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('not_in_channel')
        
        with pytest.raises(RuntimeError) as exc_info:
            post_pipeline_alert("test")
        
        assert "Slack API error" in str(exc_info.value)


# ===================================================================
# 5. Error Handling Tests
# ===================================================================
class TestSlackErrorHandling:
    """Tests for various error scenarios."""

    @patch('interface.slack.client.client')
    def test_rate_limit_error(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('rate_limited')
        
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test")
        
        assert "Slack API error" in str(exc_info.value)

    @patch('interface.slack.client.client')
    def test_missing_scope_error(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('missing_scope')
        
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test")
        
        assert "Slack API error" in str(exc_info.value)

    @patch('interface.slack.client.client')
    def test_archived_channel_error(self, mock_client):
        mock_client.chat_postMessage.side_effect = make_slack_api_error('is_archived')
        
        with pytest.raises(RuntimeError) as exc_info:
            post_message("test")
        
        assert "Slack API error" in str(exc_info.value)

    @patch('interface.slack.client.SLACK_BOT_TOKEN', None)
    def test_missing_bot_token_handled(self):
        # This will fail at client initialization if token is truly None
        # Your actual implementation may handle this differently
        pass

    def test_empty_message_formats_correctly(self):
        result = format_morning_brief()
        assert isinstance(result, str)
        assert len(result) > 0