import os
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

load_dotenv()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
DEFAULT_SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

client = WebClient(token=SLACK_BOT_TOKEN)

def post_message(text: str, channel: str = None):
    target_channel = channel or DEFAULT_SLACK_CHANNEL_ID

    if not target_channel:
        raise ValueError("SLACK_CHANNEL_ID not set")

    try:
        return client.chat_postMessage(channel=target_channel, text=text)
    except SlackApiError as e:
        raise RuntimeError(f"Slack API error: {e.response['error']}")