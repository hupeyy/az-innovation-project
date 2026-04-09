from interface.slack.client import post_message
from interface.slack.messages import (
    format_morning_brief,
    format_pipeline_alert,
    format_brand_alert,
)

def post_pipeline_alert(message: str, channel: str = None):
    text = format_pipeline_alert(message)
    return post_message(text, channel=channel)


def post_morning_brief(weather=None, news=None, stocks=None, calendar=None, channel: str = None):
    text = format_morning_brief(
        weather=weather,
        news=news,
        stocks=stocks,
        calendar=calendar,
    )
    return post_message(text, channel=channel)


def post_brand_alert(platform, keyword, sentiment, text, url=None, channel: str = None):
    msg = format_brand_alert(platform, keyword, sentiment, text, url)
    return post_message(msg, channel=channel)