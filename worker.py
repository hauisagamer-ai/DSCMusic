"""
worker.py — GitHub Actions Worker (extraction-only, Option A)
No Discord connection, no voice, no UDP. Calls the shared extractor.py,
then POSTs the result back to Controller's /worker/callback.

Env vars required (set by workflow from repository_dispatch payload):
  GUILD_ID
  YOUTUBE_URL
  CALLBACK_URL
  CALLBACK_SECRET

Optional:
  YTDLP_COOKIES_PATH   (default: cookies.txt)
"""

import logging
import os

import requests

from extractor import extract, to_payload

GUILD_ID = os.environ["GUILD_ID"]
YOUTUBE_URL = os.environ["YOUTUBE_URL"]
CALLBACK_URL = os.environ["CALLBACK_URL"]
CALLBACK_SECRET = os.environ["CALLBACK_SECRET"]
COOKIES_PATH = os.environ.get("YTDLP_COOKIES_PATH", "cookies.txt")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")


def send_callback(payload: dict):
    payload["secret"] = CALLBACK_SECRET
    payload["guild_id"] = GUILD_ID
    try:
        resp = requests.post(CALLBACK_URL, json=payload, timeout=15)
        log.info("Callback response: %s %s", resp.status_code, resp.text)
    except requests.RequestException as e:
        log.error("Failed to send callback: %s", e)


def main():
    try:
        info = extract(YOUTUBE_URL, cookies_path=COOKIES_PATH)
    except Exception as e:  # noqa: BLE001
        log.error("Extraction failed: %s", e)
        send_callback({"status": "error", "message": str(e)})
        return

    payload = to_payload(info)

    if not payload.get("stream_url"):
        send_callback({"status": "error", "message": "No playable stream found."})
        return

    payload["status"] = "ok"
    send_callback(payload)


if __name__ == "__main__":
    main()