import os
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

bot_token = os.environ["SLACK_BOT_TOKEN"]
app_token = os.environ["SLACK_APP_TOKEN"]

app = App(token=bot_token)

@app.event("message")
def handle_message_events(body, say, logger):
    event = body.get("event", {})
    logger.info(f"Incoming event: {event}")

    # Ignore bot messages
    if event.get("bot_id"):
        return

    # Ignore message subtypes like edits, bot_message, etc.
    if event.get("subtype") is not None:
        return

    text = event.get("text", "").strip()
    channel_type = event.get("channel_type")

    # Only reply in direct messages
    if channel_type == "im" and text:
        say(f"You said: {text}")

if __name__ == "__main__":
    SocketModeHandler(app, app_token).start()