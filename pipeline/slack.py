from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
from dotenv import load_dotenv

load_dotenv()

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")

client = WebClient(token=SLACK_BOT_TOKEN)

def send_slack_message(text, channel=None):
    target_channel = channel or SLACK_CHANNEL_ID
    try:
        response = client.chat_postMessage(
            channel=target_channel,
            text=text
        )
        return response
    except SlackApiError as e:
        print(f"Failed to send Slack message: {e.response['error']}")
        return None