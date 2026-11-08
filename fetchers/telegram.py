"""Send a message to Telegram through your bot.

Config (Lambda environment variables -> SSM parameter names):
  TELEGRAM_TOKEN_PARAM    bot token from @BotFather (SecureString)
  TELEGRAM_CHAT_ID_PARAM  your chat ID
"""

import html
import json
import os

import boto3

from common import http_request, log

MAX_LENGTH = 4096  # Telegram's limit per message


def esc(value):
    """Escape text for Telegram's HTML parse mode."""
    return html.escape(str(value), quote=False)


def send(text):
    ssm = boto3.client("ssm")
    token = ssm.get_parameter(Name=os.environ["TELEGRAM_TOKEN_PARAM"], WithDecryption=True)["Parameter"]["Value"]
    chat_id = ssm.get_parameter(Name=os.environ["TELEGRAM_CHAT_ID_PARAM"], WithDecryption=True)["Parameter"]["Value"]

    if len(text) > MAX_LENGTH:
        text = text[: MAX_LENGTH - 20] + "\n…(truncated)"

    body = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode()
    http_request(
        "POST",
        f"https://api.telegram.org/bot{token}/sendMessage",
        headers={"Content-Type": "application/json"},
        data=body,
        log_url="https://api.telegram.org/bot***/sendMessage",
    )
    log.info("Sent Telegram message (%d chars)", len(text))
