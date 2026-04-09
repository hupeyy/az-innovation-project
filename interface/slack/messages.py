def format_morning_brief(weather=None, news=None, stocks=None, calendar=None):
    parts = ["🌤 *Morning Brief*"]

    if weather:
        parts.append(f"*Weather:* {weather}")

    if stocks:
        parts.append(f"*Markets:* {stocks}")

    if news:
        parts.append("*Top News:*")
        for item in news:
            parts.append(f"• {item}")

    if calendar:
        parts.append("*Calendar:*")
        for event in calendar:
            parts.append(f"• {event}")

    return "\n".join(parts)


def format_pipeline_alert(message: str):
    return f"⚠️ *Pipeline Alert*\n{message}"


def format_brand_alert(platform: str, keyword: str, sentiment: str, text: str, url: str = None):
    msg = (
        f"🔎 *Brand Mention Alert*\n"
        f"*Platform:* {platform}\n"
        f"*Keyword:* {keyword}\n"
        f"*Sentiment:* {sentiment}\n"
        f"*Text:* {text}"
    )
    if url:
        msg += f"\n*URL:* {url}"
    return msg