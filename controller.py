"""
controller.py — Render Controller
Persistent voice player + state machine. Normal path (Option A): dispatches
Worker (GitHub Actions) for yt-dlp extraction, receives result via
/worker/callback, plays it. Fallback path (Option C): if Worker dispatch
fails, times out, or reports an error, Controller extracts locally using
the same extractor.py.
"""

import asyncio
import base64
import logging
import os
import re
import threading
import time

import discord
import requests
from flask import Flask, jsonify, request

from extractor import extract, to_payload

BOT_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
GITHUB_TOKEN = os.environ["GITHUB_PAT"]
GITHUB_OWNER = os.environ["GITHUB_OWNER"]
GITHUB_REPO = os.environ["GITHUB_REPO"]
CALLBACK_SECRET = os.environ["CALLBACK_SECRET"]
PUBLIC_CALLBACK_URL = os.environ.get("PUBLIC_CALLBACK_URL", "")
DISPATCH_EVENT_TYPE = os.environ.get("DISPATCH_EVENT_TYPE", "extract_song")
COOKIES_B64 = os.environ.get("YTDLP_COOKIES_B64", "")

COMMAND_PREFIX = "!!YT"
LOAD_TIMEOUT_SEC = 5 * 60
IDLE_TIMEOUT_SEC = 8 * 60
MAX_SONG_DURATION_SEC = 15 * 60

YOUTUBE_URL_RE = re.compile(
    r"^(https?://)?([\w-]+\.)?(youtube\.com/watch\?v=|youtu\.be/)[\w\-]+"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("controller")

LOCAL_COOKIES_PATH = "cookies.txt"
if COOKIES_B64:
    try:
        with open(LOCAL_COOKIES_PATH, "wb") as f:
            f.write(base64.b64decode(COOKIES_B64))
        log.info("Local fallback cookies written.")
    except Exception as e:  # noqa: BLE001
        log.warning("Failed to write local cookies: %s", e)

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
client = discord.Client(intents=intents)

discord_loop = None


class Session:
    def __init__(self, guild_id, text_channel_id, voice_channel_id):
        self.guild_id = guild_id
        self.text_channel_id = text_channel_id
        self.voice_channel_id = voice_channel_id
        self.voice_client = None
        self.state = "LOADING"
        self.current_url = None
        self.loop_enabled = False
        self.switching_song = False
        self.cap_triggered = False
        self.idle_timer = None
        self.load_timer = None
        self.song_cap_timer = None
        self.created_at = time.time()


sessions = {}
sessions_lock = threading.Lock()


def get_session(guild_id):
    with sessions_lock:
        return sessions.get(guild_id)


def set_session(guild_id, session):
    with sessions_lock:
        sessions[guild_id] = session


def remove_session(guild_id):
    with sessions_lock:
        sessions.pop(guild_id, None)


def cancel_task(task):
    if task and not task.done():
        task.cancel()


async def say(session, msg):
    log.info("[guild=%s] %s", session.guild_id, msg)
    channel = client.get_channel(session.text_channel_id)
    if channel:
        try:
            await channel.send(msg)
        except discord.HTTPException:
            log.warning("Failed to send message")


def stop_current_audio(session):
    if session.voice_client and (session.voice_client.is_playing() or session.voice_client.is_paused()):
        session.voice_client.stop()


def dispatch_extraction(guild_id, text_channel_id, youtube_url):
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "event_type": DISPATCH_EVENT_TYPE,
        "client_payload": {
            "guild_id": str(guild_id),
            "text_channel_id": str(text_channel_id),
            "youtube_url": youtube_url,
            "callback_url": PUBLIC_CALLBACK_URL,
            "callback_secret": CALLBACK_SECRET,
        },
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
    except requests.RequestException as e:
        log.error("GitHub dispatch failed: %s", e)
        return False
    if resp.status_code != 204:
        log.error("GitHub dispatch failed: %s %s", resp.status_code, resp.text)
        return False
    return True


def _extract_locally_sync(youtube_url):
    cookies = LOCAL_COOKIES_PATH if os.path.exists(LOCAL_COOKIES_PATH) else None
    info = extract(youtube_url, cookies_path=cookies)
    return to_payload(info)


async def extract_locally(session, youtube_url):
    await say(session, "Worker unavailable — extracting locally, this may take a moment...")
    loop = asyncio.get_running_loop()
    try:
        payload = await loop.run_in_executor(None, _extract_locally_sync, youtube_url)
    except Exception as e:  # noqa: BLE001
        log.error("Local fallback extraction failed: %s", e)
        await _handle_extraction_error(session, str(e))
        return

    if not payload.get("stream_url"):
        await _handle_extraction_error(session, "No playable stream found (local fallback).")
        return

    await play_stream(
        session,
        payload["title"],
        payload["stream_url"],
        payload["http_headers"],
        payload["duration"],
    )


async def _idle_timeout(session):
    try:
        await asyncio.sleep(IDLE_TIMEOUT_SEC)
        await say(session, "Idle for 8 minutes. Disconnecting.")
        await teardown(session)
    except asyncio.CancelledError:
        pass


def start_idle_timer(session):
    cancel_task(session.idle_timer)
    session.idle_timer = asyncio.create_task(_idle_timeout(session))


async def _load_timeout(session, youtube_url):
    try:
        await asyncio.sleep(LOAD_TIMEOUT_SEC)
        await say(session, "Worker took too long. Falling back to local extraction...")
        await extract_locally(session, youtube_url)
    except asyncio.CancelledError:
        pass


def start_load_timer(session, youtube_url):
    cancel_task(session.load_timer)
    session.load_timer = asyncio.create_task(_load_timeout(session, youtube_url))


async def _song_cap_timeout(session):
    try:
        await asyncio.sleep(MAX_SONG_DURATION_SEC)
        await say(session, "Song exceeded 15-minute cap. Stopping.")
        session.cap_triggered = True
        stop_current_audio(session)
    except asyncio.CancelledError:
        pass


def start_song_cap_timer(session):
    cancel_task(session.song_cap_timer)
    session.song_cap_timer = asyncio.create_task(_song_cap_timeout(session))


async def begin_load(session, youtube_url):
    session.state = "LOADING"
    session.current_url = youtube_url
    cancel_task(session.idle_timer)
    start_load_timer(session, youtube_url)

    ok = dispatch_extraction(session.guild_id, session.text_channel_id, youtube_url)
    if not ok:
        cancel_task(session.load_timer)
        await say(session, "Dispatch failed — falling back to local extraction...")
        await extract_locally(session, youtube_url)


async def play_stream(session, title, stream_url, http_headers, duration):
    cancel_task(session.load_timer)

    if duration and duration > MAX_SONG_DURATION_SEC:
        await say(session, f"Rejected: video is {duration // 60} min, exceeds 15-min cap.")
        session.state = "IDLE"
        start_idle_timer(session)
        return

    if not session.voice_client or not session.voice_client.is_connected():
        guild = client.get_guild(session.guild_id)
        vc_channel = guild.get_channel(session.voice_channel_id) if guild else None
        if not vc_channel:
            await say(session, "Voice channel no longer available.")
            await teardown(session)
            return
        try:
            session.voice_client = await vc_channel.connect()
        except Exception as e:  # noqa: BLE001
            await say(session, f"Failed to join voice: {e}")
            await teardown(session)
            return

    header_str = "".join(f"{k}: {v}\r\n" for k, v in (http_headers or {}).items())
    before_opts = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
    if header_str:
        before_opts += f' -headers "{header_str}"'

    source = discord.FFmpegPCMAudio(stream_url, before_options=before_opts, options="-vn")

    def after_playback(error):
        if error:
            log.error("Playback error: %s", error)
        if discord_loop:
            asyncio.run_coroutine_threadsafe(on_song_finished(session), discord_loop)

    session.voice_client.play(source, after=after_playback)
    session.state = "PLAYING"
    start_song_cap_timer(session)
    await say(session, f"Now playing: {title}")


async def on_song_finished(session):
    cancel_task(session.song_cap_timer)

    if session.switching_song:
        session.switching_song = False
        return

    if session.state == "SHUTDOWN":
        return

    if session.cap_triggered:
        session.cap_triggered = False
        session.state = "IDLE"
        start_idle_timer(session)
        await say(session, "Idling — send a new `!!YT <link>` to continue.")
        return

    if session.loop_enabled and session.current_url:
        await begin_load(session, session.current_url)
        return

    session.state = "IDLE"
    start_idle_timer(session)
    await say(session, "Playback finished. Idling for 8 minutes — send a new `!!YT <link>` to continue.")


async def teardown(session):
    session.state = "SHUTDOWN"
    cancel_task(session.idle_timer)
    cancel_task(session.load_timer)
    cancel_task(session.song_cap_timer)
    stop_current_audio(session)
    if session.voice_client and session.voice_client.is_connected():
        await session.voice_client.disconnect(force=True)
    remove_session(session.guild_id)


async def _handle_extraction_error(session, message):
    await say(session, f"Could not load that link: {message}")
    cancel_task(session.load_timer)
    session.state = "IDLE"
    start_idle_timer(session)


@client.event
async def on_ready():
    global discord_loop
    discord_loop = asyncio.get_running_loop()
    log.info("Controller logged in as %s", client.user)


@client.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return
    if not message.content.startswith(COMMAND_PREFIX):
        return

    arg = message.content[len(COMMAND_PREFIX):].strip()
    guild_id = message.guild.id
    session = get_session(guild_id)

    if arg == "pause":
        if session and session.state == "PLAYING" and session.voice_client and session.voice_client.is_playing():
            session.voice_client.pause()
            cancel_task(session.song_cap_timer)
            session.state = "IDLE"
            start_idle_timer(session)
            await message.channel.send("Paused.")
        else:
            await message.channel.send("Nothing is playing.")
        return

    if arg == "resume":
        if session and session.state == "IDLE" and session.voice_client and session.voice_client.is_paused():
            session.voice_client.resume()
            cancel_task(session.idle_timer)
            session.state = "PLAYING"
            start_song_cap_timer(session)
            await message.channel.send("Resumed.")
        else:
            await message.channel.send("Nothing paused to resume.")
        return

    if arg == "exit":
        if session:
            await message.channel.send("Exiting.")
            await teardown(session)
        else:
            await message.channel.send("No active session.")
        return

    if arg == "loop":
        if session:
            session.loop_enabled = not session.loop_enabled
            await message.channel.send(f"Loop {'enabled' if session.loop_enabled else 'disabled'}.")
        else:
            await message.channel.send("No active session.")
        return

    if YOUTUBE_URL_RE.match(arg):
        if session and session.state in ("PLAYING", "IDLE") and session.voice_client:
            session.switching_song = True
            stop_current_audio(session)
            await begin_load(session, arg)
            await message.channel.send("Loading new song...")
            return

        member = message.guild.get_member(message.author.id)
        if not member or not member.voice or not member.voice.channel:
            await message.channel.send("Join a voice channel first.")
            return

        new_session = Session(
            guild_id=guild_id,
            text_channel_id=message.channel.id,
            voice_channel_id=member.voice.channel.id,
        )
        set_session(guild_id, new_session)
        await begin_load(new_session, arg)
        await message.channel.send("Starting playback...")
        return


app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify(status="ok"), 200


@app.route("/")
def root():
    return jsonify(status="controller alive"), 200


@app.route("/worker/callback", methods=["POST"])
def worker_callback():
    data = request.get_json(force=True, silent=True) or {}

    if data.get("secret") != CALLBACK_SECRET:
        return jsonify(error="unauthorized"), 401

    try:
        guild_id = int(data["guild_id"])
    except (KeyError, ValueError):
        return jsonify(error="invalid guild_id"), 400

    session = get_session(guild_id)
    if not session:
        return jsonify(error="no active session"), 404

    if session.state != "LOADING":
        return jsonify(status="ignored, session not loading"), 200

    if data.get("status") == "error":
        message = data.get("message", "extraction failed")
        if discord_loop:
            asyncio.run_coroutine_threadsafe(_worker_reported_error(session, message), discord_loop)
        return jsonify(status="received"), 200

    title = data.get("title", "Unknown title")
    stream_url = data.get("stream_url")
    http_headers = data.get("http_headers", {})
    duration = data.get("duration", 0)

    if not stream_url:
        return jsonify(error="missing stream_url"), 400

    if discord_loop:
        asyncio.run_coroutine_threadsafe(
            play_stream(session, title, stream_url, http_headers, duration), discord_loop
        )
    return jsonify(status="received"), 200


async def _worker_reported_error(session, message):
    await say(session, f"Worker extraction failed ({message}). Falling back to local extraction...")
    if session.current_url:
        await extract_locally(session, session.current_url)
    else:
        await _handle_extraction_error(session, message)


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

def run_discord():
    try:
        client.run(BOT_TOKEN)
    except discord.errors.HTTPException as e:
        if e.status == 429:
            wait = 120
            log.warning("Discord login rate limited, sleeping %ss before exit", wait)
            time.sleep(wait)
            raise SystemExit(1)
        raise

if __name__ == "__main__":
    time.sleep(3)
    if not PUBLIC_CALLBACK_URL:
        log.warning("PUBLIC_CALLBACK_URL not set — Worker won't know where to send results!")
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_discord()
