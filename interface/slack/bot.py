import os
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

slack_app = App(token=os.environ["SLACK_BOT_TOKEN"])

@slack_app.event("message")
def handle_message_events(body, say, logger):
    event = body.get("event", {})
    logger.info(f"Incoming event: {event}")

    if event.get("bot_id"):
        return

    if event.get("subtype") is not None:
        return

    text = event.get("text", "").strip()
    channel_type = event.get("channel_type")

    if channel_type == "im" and text:
        say(f"You said: {text}")

if __name__ == "__main__":
    handler = SocketModeHandler(slack_app, os.environ["SLACK_APP_TOKEN"])
    handler.start()