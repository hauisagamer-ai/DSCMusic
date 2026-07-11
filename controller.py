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

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
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
    log.debug("get_session(guild_id=%s)", guild_id)
    with sessions_lock:
        return sessions.get(guild_id)


def set_session(guild_id, session):
    log.info("set_session(guild_id=%s) — new session created", guild_id)
    with sessions_lock:
        sessions[guild_id] = session


def remove_session(guild_id):
    log.info("remove_session(guild_id=%s)", guild_id)
    with sessions_lock:
        sessions.pop(guild_id, None)


def cancel_task(task):
    if task and not task.done():
        log.debug("cancel_task(%s)", task)
        task.cancel()


async def say(session, msg):
    log.info("say(guild=%s): %s", session.guild_id, msg)
    channel = client.get_channel(session.text_channel_id)
    if channel:
        try:
            await channel.send(msg)
        except discord.HTTPException:
            log.warning("Failed to send message")


def stop_current_audio(session):
    log.info("stop_current_audio(guild=%s) state=%s", session.guild_id, session.state)
    if session.voice_client and (session.voice_client.is_playing() or session.voice_client.is_paused()):
        session.voice_client.stop()


def dispatch_extraction(guild_id, text_channel_id, youtube_url):
    log.info("dispatch_extraction(guild=%s, url=%s) CALLED", guild_id, youtube_url)
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
        log.error("dispatch_extraction FAILED (request exception): %s", e)
        return False
    if resp.status_code != 204:
        log.error("dispatch_extraction FAILED: %s %s", resp.status_code, resp.text)
        return False
    log.info("dispatch_extraction(guild=%s) SUCCESS", guild_id)
    return True


def _extract_locally_sync(youtube_url):
    log.info("_extract_locally_sync(url=%s) CALLED", youtube_url)
    cookies = LOCAL_COOKIES_PATH if os.path.exists(LOCAL_COOKIES_PATH) else None
    info = extract(youtube_url, cookies_path=cookies)
    log.info("_extract_locally_sync(url=%s) SUCCESS", youtube_url)
    return to_payload(info)


async def extract_locally(session, youtube_url):
    log.info("extract_locally(guild=%s, url=%s) CALLED", session.guild_id, youtube_url)
    await say(session, "Worker unavailable — extracting locally, this may take a moment...")
    loop = asyncio.get_running_loop()
    try:
        payload = await loop.run_in_executor(None, _extract_locally_sync, youtube_url)
    except Exception as e:  # noqa: BLE001
        log.error("extract_locally(guild=%s) FAILED: %s", session.guild_id, e)
        await _handle_extraction_error(session, str(e))
        return

    if not payload.get("stream_url"):
        log.error("extract_locally(guild=%s) FAILED: no stream_url", session.guild_id)
        await _handle_extraction_error(session, "No playable stream found (local fallback).")
        return

    log.info("extract_locally(guild=%s) SUCCESS", session.guild_id)
    await play_stream(
        session,
        payload["title"],
        payload["stream_url"],
        payload["http_headers"],
        payload["duration"],
    )


async def _idle_timeout(session):
    log.info("_idle_timeout(guild=%s) STARTED, sleeping %ss", session.guild_id, IDLE_TIMEOUT_SEC)
    try:
        await asyncio.sleep(IDLE_TIMEOUT_SEC)
        log.info("_idle_timeout(guild=%s) FIRED", session.guild_id)
        await say(session, "Idle for 8 minutes. Disconnecting.")
        await teardown(session)
    except asyncio.CancelledError:
        log.debug("_idle_timeout(guild=%s) CANCELLED", session.guild_id)


def start_idle_timer(session):
    log.info("start_idle_timer(guild=%s)", session.guild_id)
    cancel_task(session.idle_timer)
    session.idle_timer = asyncio.create_task(_idle_timeout(session))


async def _load_timeout(session, youtube_url):
    log.info("_load_timeout(guild=%s) STARTED, sleeping %ss", session.guild_id, LOAD_TIMEOUT_SEC)
    try:
        await asyncio.sleep(LOAD_TIMEOUT_SEC)
        log.info("_load_timeout(guild=%s) FIRED — no callback received in time", session.guild_id)
        await say(session, "Worker took too long. Falling back to local extraction...")
        await extract_locally(session, youtube_url)
    except asyncio.CancelledError:
        log.debug("_load_timeout(guild=%s) CANCELLED", session.guild_id)


def start_load_timer(session, youtube_url):
    log.info("start_load_timer(guild=%s)", session.guild_id)
    cancel_task(session.load_timer)
    session.load_timer = asyncio.create_task(_load_timeout(session, youtube_url))


async def _song_cap_timeout(session):
    log.info("_song_cap_timeout(guild=%s) STARTED, sleeping %ss", session.guild_id, MAX_SONG_DURATION_SEC)
    try:
        await asyncio.sleep(MAX_SONG_DURATION_SEC)
        log.info("_song_cap_timeout(guild=%s) FIRED", session.guild_id)
        await say(session, "Song exceeded 15-minute cap. Stopping.")
        session.cap_triggered = True
        stop_current_audio(session)
    except asyncio.CancelledError:
        log.debug("_song_cap_timeout(guild=%s) CANCELLED", session.guild_id)


def start_song_cap_timer(session):
    log.info("start_song_cap_timer(guild=%s)", session.guild_id)
    cancel_task(session.song_cap_timer)
    session.song_cap_timer = asyncio.create_task(_song_cap_timeout(session))


async def begin_load(session, youtube_url):
    log.info("begin_load(guild=%s, url=%s) CALLED — state was %s", session.guild_id, youtube_url, session.state)
    session.state = "LOADING"
    session.current_url = youtube_url
    cancel_task(session.idle_timer)
    start_load_timer(session, youtube_url)

    ok = dispatch_extraction(session.guild_id, session.text_channel_id, youtube_url)
    if not ok:
        cancel_task(session.load_timer)
        await say(session, "Dispatch failed — falling back to local extraction...")
        await extract_locally(session, youtube_url)
    log.info("begin_load(guild=%s) FINISHED (dispatch ok=%s)", session.guild_id, ok)


async def play_stream(session, title, stream_url, http_headers, duration):
    log.info("play_stream(guild=%s, title=%s) CALLED", session.guild_id, title)
    cancel_task(session.load_timer)

    if duration and duration > MAX_SONG_DURATION_SEC:
        log.info("play_stream(guild=%s) REJECTED — duration %ss exceeds cap", session.guild_id, duration)
        await say(session, f"Rejected: video is {duration // 60} min, exceeds 15-min cap.")
        session.state = "IDLE"
        start_idle_timer(session)
        return

    if not session.voice_client or not session.voice_client.is_connected():
        log.info("play_stream(guild=%s) — voice not connected, connecting now", session.guild_id)
        guild = client.get_guild(session.guild_id)
        vc_channel = guild.get_channel(session.voice_channel_id) if guild else None
        if not vc_channel:
            log.error("play_stream(guild=%s) — voice channel not found", session.guild_id)
            await say(session, "Voice channel no longer available.")
            await teardown(session)
            return
        try:
            session.voice_client = await vc_channel.connect()
            log.info("play_stream(guild=%s) — voice connected", session.guild_id)
        except Exception as e:  # noqa: BLE001
            log.error("play_stream(guild=%s) — voice connect failed: %s", session.guild_id, e)
            await say(session, f"Failed to join voice: {e}")
            await teardown(session)
            return

    header_str = "".join(f"{k}: {v}\r\n" for k, v in (http_headers or {}).items())
    before_opts = "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
    if header_str:
        before_opts += f' -headers "{header_str}"'

    source = discord.FFmpegPCMAudio(stream_url, before_options=before_opts, options="-vn")

    def after_playback(error):
        log.info("play_stream(guild=%s) after_playback callback fired, error=%s", session.guild_id, error)
        if error:
            log.error("Playback error: %s", error)
        if discord_loop:
            asyncio.run_coroutine_threadsafe(on_song_finished(session), discord_loop)

    session.voice_client.play(source, after=after_playback)
    session.state = "PLAYING"
    start_song_cap_timer(session)
    log.info("play_stream(guild=%s) — now PLAYING", session.guild_id)
    await say(session, f"Now playing: {title}")


async def on_song_finished(session):
    log.info("on_song_finished(guild=%s) CALLED — switching_song=%s cap_triggered=%s loop=%s",
              session.guild_id, session.switching_song, session.cap_triggered, session.loop_enabled)
    cancel_task(session.song_cap_timer)

    if session.switching_song:
        session.switching_song = False
        log.info("on_song_finished(guild=%s) — was switching, no idle transition", session.guild_id)
        return

    if session.state == "SHUTDOWN":
        log.info("on_song_finished(guild=%s) — already SHUTDOWN, ignoring", session.guild_id)
        return

    if session.cap_triggered:
        session.cap_triggered = False
        session.state = "IDLE"
        start_idle_timer(session)
        await say(session, "Idling — send a new `!!YT <link>` to continue.")
        return

    if session.loop_enabled and session.current_url:
        loop_url = session.current_url
        log.info("on_song_finished(guild=%s) — looping url=%s", session.guild_id, loop_url)
        await begin_load(session, loop_url)
        return

    session.state = "IDLE"
    start_idle_timer(session)
    await say(session, "Playback finished. Idling for 8 minutes — send a new `!!YT <link>` to continue.")


async def teardown(session):
    log.info("teardown(guild=%s) CALLED — state was %s", session.guild_id, session.state)
    session.state = "SHUTDOWN"
    cancel_task(session.idle_timer)
    cancel_task(session.load_timer)
    cancel_task(session.song_cap_timer)
    stop_current_audio(session)
    if session.voice_client and session.voice_client.is_connected():
        await session.voice_client.disconnect(force=True)
        log.info("teardown(guild=%s) — voice disconnected", session.guild_id)
    remove_session(session.guild_id)


async def _handle_extraction_error(session, message):
    log.info("_handle_extraction_error(guild=%s): %s", session.guild_id, message)
    await say(session, f"Could not load that link: {message}")
    cancel_task(session.load_timer)
    session.state = "IDLE"
    start_idle_timer(session)


@client.event
async def on_ready():
    global discord_loop
    discord_loop = asyncio.get_running_loop()
    log.info("on_ready() CALLED — logged in as %s (session id may indicate reconnect)", client.user)


@client.event
async def on_message(message):
    log.info(
        "on_message() CALLED — author=%s bot=%s content=%r channel=%s",
        message.author, message.author.bot, message.content, message.channel.id,
    )
    if message.author.bot or not message.guild:
        return
    if not message.content.startswith(COMMAND_PREFIX):
        return

    arg = message.content[len(COMMAND_PREFIX):].strip()
    guild_id = message.guild.id
    session = get_session(guild_id)
    log.info("on_message() — parsed arg=%r guild=%s existing_session=%s", arg, guild_id, bool(session))

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
        log.info("on_message() — matched YouTube URL, guild=%s session_state=%s", guild_id, session.state if session else None)
        if session and session.state in ("PLAYING", "IDLE") and session.voice_client:
            log.info("on_message() — SWITCHING SONG path, guild=%s", guild_id)
            session.switching_song = True
            stop_current_audio(session)
            await begin_load(session, arg)
            await message.channel.send("Loading new song...")
            return

        member = message.guild.get_member(message.author.id)
        if not member or not member.voice or not member.voice.channel:
            await message.channel.send("Join a voice channel first.")
            return

        log.info("on_message() — NEW SESSION path, guild=%s", guild_id)
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
    log.info("worker_callback() CALLED — guild=%s status=%s", data.get("guild_id"), data.get("status"))

    if data.get("secret") != CALLBACK_SECRET:
        log.warning("worker_callback() — UNAUTHORIZED (bad secret)")
        return jsonify(error="unauthorized"), 401

    try:
        guild_id = int(data["guild_id"])
    except (KeyError, ValueError):
        log.warning("worker_callback() — invalid guild_id: %r", data.get("guild_id"))
        return jsonify(error="invalid guild_id"), 400

    session = get_session(guild_id)
    if not session:
        log.warning("worker_callback() — no active session for guild=%s", guild_id)
        return jsonify(error="no active session"), 404

    if session.state != "LOADING":
        log.info("worker_callback() — IGNORED, session state is %s not LOADING (guild=%s)", session.state, guild_id)
        return jsonify(status="ignored, session not loading"), 200

    if data.get("status") == "error":
        message = data.get("message", "extraction failed")
        log.info("worker_callback() — Worker reported error: %s (guild=%s)", message, guild_id)
        if discord_loop:
            asyncio.run_coroutine_threadsafe(_worker_reported_error(session, message), discord_loop)
        return jsonify(status="received"), 200

    title = data.get("title", "Unknown title")
    stream_url = data.get("stream_url")
    http_headers = data.get("http_headers", {})
    duration = data.get("duration", 0)

    if not stream_url:
        log.warning("worker_callback() — missing stream_url (guild=%s)", guild_id)
        return jsonify(error="missing stream_url"), 400

    log.info("worker_callback() — SUCCESS, dispatching play_stream (guild=%s title=%s)", guild_id, title)
    if discord_loop:
        asyncio.run_coroutine_threadsafe(
            play_stream(session, title, stream_url, http_headers, duration), discord_loop
        )
    return jsonify(status="received"), 200


async def _worker_reported_error(session, message):
    log.info("_worker_reported_error(guild=%s): %s", session.guild_id, message)
    await say(session, f"Worker extraction failed ({message}). Falling back to local extraction...")
    if session.current_url:
        await extract_locally(session, session.current_url)
    else:
        await _handle_extraction_error(session, message)


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    log.info("run_flask() starting on port %s", port)
    app.run(host="0.0.0.0", port=port)


def run_discord():
    log.info("run_discord() starting client.run()")
    client.run(BOT_TOKEN)


if __name__ == "__main__":
    log.info("Controller process starting (pid=%s)", os.getpid())
    if not PUBLIC_CALLBACK_URL:
        log.warning("PUBLIC_CALLBACK_URL not set — Worker won't know where to send results!")
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    run_discord()
