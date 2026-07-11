"""
extractor.py — shared yt-dlp extraction logic.
Used by worker.py (Option A, GitHub Actions) and controller.py
(Option C fallback, Render). Keeping this in one place guarantees both
paths behave identically.

Requires: yt-dlp CLI on PATH, Deno on PATH (for --js-runtimes deno).
"""

import json
import logging
import os
import subprocess

log = logging.getLogger("extractor")


def resolve_client(url: str) -> str:
    """music.youtube.com always forces web_music; everything else uses mweb."""
    if "music.youtube.com" in url:
        return "web_music"
    return "mweb"


def extract(url: str, cookies_path: str | None = None, timeout: int = 90) -> dict:
    """
    Runs yt-dlp CLI to extract stream info as JSON.
    Returns the parsed --dump-single-json dict (contains 'title', 'url',
    'http_headers', 'duration', etc.) on success.
    Raises RuntimeError on failure.
    """
    client = resolve_client(url)

    cmd = [
        "yt-dlp",
        "--js-runtimes", "deno",
        "--extractor-args", f"youtube:player_client={client}",
        "-f", "bestaudio/best",
        "--no-playlist",
        "--dump-single-json",
        "--no-warnings",
    ]

    if cookies_path and os.path.exists(cookies_path):
        cmd += ["--cookies", cookies_path]

    cmd.append(url)

    log.info("Running extraction (client=%s): %s", client, url)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"yt-dlp timed out after {timeout}s") from e

    if result.returncode != 0:
        stderr_tail = result.stderr.strip()[-500:]
        log.error("yt-dlp stderr: %s", stderr_tail)
        raise RuntimeError(f"yt-dlp extraction failed: {stderr_tail}")

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"yt-dlp returned invalid JSON: {e}") from e


def to_payload(info: dict) -> dict:
    """Normalize extracted info into the shape Controller expects."""
    return {
        "title": info.get("title", "Unknown title"),
        "stream_url": info.get("url"),
        "http_headers": info.get("http_headers", {}),
        "duration": info.get("duration", 0),
    }
