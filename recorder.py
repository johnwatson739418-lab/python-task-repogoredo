"""
YouTube Live Recorder -- Cloud Runner Engine (Final Hardened 2-File Edition)
Based on proven sucessgithub.txt with:
- Free disk space monitoring (stops cleanly before runner 14GB disk fills up)
- Safe FFmpeg concat check (skips concat & preserves raw chunks if free disk < chunk sum)
- Default + Android + Web_Safari + HTML badge + RSS multi-detector
"""

import atexit
import glob
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from queue import Empty, Queue

socket.setdefaulttimeout(15)

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE", "@Apka-aashu").strip()
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "UCGFi8l9sf5ab4hVJf7byBZg").strip()
START_AT_IST = os.getenv("START_AT_IST", "").strip()
HOURS = min(4.5, max(0.05, float(os.getenv("HOURS", "4.0"))))
OUT_DIR = os.getenv("OUT_DIR", "./streams").strip()

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.txt").strip()
if COOKIE_FILE and os.path.isfile(COOKIE_FILE):
    try:
        raw_cookie = open(COOKIE_FILE, "r", encoding="utf-8-sig", errors="replace").read().lstrip("\ufeff")
        open(COOKIE_FILE, "w", encoding="utf-8", newline="\n").write(raw_cookie)
    except Exception:
        pass

USE_COOKIES = (
    os.getenv("USE_COOKIES", "false").lower() == "true"
    and bool(COOKIE_FILE)
    and os.path.isfile(COOKIE_FILE)
    and os.path.getsize(COOKIE_FILE) > 20
)

FORMAT_SELECTOR = (
    "b[height=720][protocol*=m3u8]"
    "/b[protocol*=m3u8]"
    "/b[height=720]"
    "/b"
)
SORT_EXPR = "res,fps,br"

PRIMARY_RECHECK_SECONDS = 20
ACTIVE_SESSION_RECHECK_SECONDS = 120
SECONDARY_WAITING_RECHECK_SECONDS = 90
SECONDARY_ACTIVE_RECHECK_SECONDS = 120

PROBE_TIMEOUT_SECONDS = 15
HTML_TIMEOUT_SECONDS = 8

HEARTBEAT_SECONDS = 15
POLL_INTERVAL_SECONDS = 2
STALL_SECONDS = 240
OFFLINE_GRACE_SECONDS = 180
FAILURE_COOLDOWN_SECONDS = 15

MIN_SAVE_BYTES = 1_000_000
MIN_FREE_DISK_BYTES = 512 * 1024 * 1024  # 512 MB safety floor on runner
COPY_BUFFER_BYTES = 8 * 1024 * 1024

SHORT_EXIT_PROTECTION_SECONDS = 90
SHORT_EXIT_RETRIES = 2
SHORT_EXIT_BACKOFF_SECONDS = 4
STALL_RETRY_BACKOFF_SECONDS = 4
CLIENT_SWITCH_DELAY_SECONDS = 5

DOWNLOAD_CLIENTS = ["android", None]
VALIDATION_CLIENTS = ["android", "web_safari", None]

SESSION_ACTIVE = threading.Event()
DOWNLOAD_ACTIVE = threading.Event()

STOP_REQUESTED = False
DISK_LIMIT_REACHED = False
CURRENT_PROC = None
CURRENT_PROC_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()


def slug_handle(handle):
    value = str(handle).strip().rstrip("/").split("/")[-1]
    value = value.replace("@", "")
    return re.sub(r"[^A-Za-z0-9_-]+", "_", value) or "channel"


def build_channel_url(channel_input):
    value = str(channel_input).strip().rstrip("/")
    if value.startswith(("http://", "https://")):
        return value
    if value.startswith("@"):
        return f"https://www.youtube.com/{value}"
    if value.startswith("UC"):
        return f"https://www.youtube.com/channel/{value}"
    return f"https://www.youtube.com/@{value}"


CHANNEL_SLUG = slug_handle(CHANNEL_HANDLE)
CHANNEL_URL = build_channel_url(CHANNEL_HANDLE)
LIVE_URL = f"{CHANNEL_URL}/live"
STREAMS_URL = f"{CHANNEL_URL}/streams"
SHORTS_URL = f"{CHANNEL_URL}/shorts"

CHUNKS_DIR = os.path.join(OUT_DIR, "_chunks")
LOG_PATH = os.path.join(OUT_DIR, f"{CHANNEL_SLUG}_session.log")
SUMMARY_PATH = os.path.join(OUT_DIR, f"{CHANNEL_SLUG}_summary.json")
STATE_PATH = os.path.join(OUT_DIR, f"{CHANNEL_SLUG}_active_state.json")

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(CHUNKS_DIR, exist_ok=True)


def log(message):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    with LOG_LOCK:
        print(line, flush=True)
        try:
            with open(LOG_PATH, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except Exception:
            pass


def log_raw(prefix, message):
    value = str(message).rstrip()
    if value:
        log(f"{prefix} | {value}")


def hms(seconds):
    seconds = max(0, int(seconds))
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def client_label(client):
    return client or "default"


def normalize_token(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def normalize_message(value):
    value = unicodedata.normalize("NFKD", str(value or ""))
    return (
        value.replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .casefold()
    )


BOTCHECK_MARKERS = ["sign in to confirm you're not a bot", "confirm you're not a bot"]
NO_FORMAT_MARKERS = ["no video formats found", "requested format is not available"]
RATE_LIMIT_MARKERS = ["http error 403", "http error 429", "try again later", "too many requests"]
NOT_LIVE_MARKERS = ["the channel is not currently live", "this live event will begin", "this live event has ended"]
NORMAL_END_MARKERS = ["finished downloading", "download complete", "has already been downloaded"]


SUMMARY = {
    "channel": CHANNEL_HANDLE,
    "channel_url": CHANNEL_URL,
    "target_channel_id": TARGET_CHANNEL_ID or None,
    "video_id": None,
    "detect_source": None,
    "client_used": None,
    "final_file": None,
    "final_size_mb": 0.0,
    "chunk_count": 0,
    "status": "init",
    "failure_reason": None,
}


def write_summary(**updates):
    SUMMARY.update(updates)
    try:
        with open(SUMMARY_PATH, "w", encoding="utf-8") as fh:
            json.dump(SUMMARY, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass


def command_exists(name):
    return shutil.which(name) is not None


def install_dependencies():
    log(f"installing dependencies for {CHANNEL_HANDLE} (cookies={USE_COOKIES})")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-U", "yt-dlp[default,curl-cffi]"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        log_raw("yt_dlp_install_error", exc)

    required = []
    if not command_exists("curl"):
        required.append("curl")
    if not command_exists("ffmpeg"):
        required.append("ffmpeg")
    if not command_exists("node") and not command_exists("nodejs"):
        required.append("nodejs")

    if required:
        log(f"installing_os_packages | {','.join(required)}")
        sudo_prefix = ["sudo"] if command_exists("sudo") else []
        try:
            subprocess.run(sudo_prefix + ["apt-get", "update", "-qq"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(sudo_prefix + ["apt-get", "install", "-y", "-qq"] + required, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as exc:
            log_raw("os_dependency_error", exc)

    version = ""
    try:
        result = subprocess.run([sys.executable, "-m", "yt_dlp", "--version"], text=True, capture_output=True, timeout=15)
        version = result.stdout.strip()
    except Exception:
        pass
    log(f"yt_dlp_version | {version or 'unknown'}")


def js_runtime_args():
    args = ["--remote-components", "ejs:github"]
    if command_exists("node"):
        args += ["--js-runtimes", "node"]
    elif command_exists("nodejs"):
        args += ["--js-runtimes", "nodejs"]
    return args


JS_ARGS = []


def fetch_text(url):
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return ""

    cmd = [
        curl_bin, "-s", "-L", "--compressed", "--max-time", str(HTML_TIMEOUT_SECONDS),
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "-H", "Accept-Language: en-US,en;q=0.9",
    ]
    if USE_COOKIES and COOKIE_FILE and os.path.isfile(COOKIE_FILE):
        cmd += ["-b", COOKIE_FILE]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=HTML_TIMEOUT_SECONDS + 2)
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception:
        pass
    return ""


TARGET = {
    "channel_id": TARGET_CHANNEL_ID.strip() or None,
    "exact_aliases": set(),
    "norm_aliases": set(),
}


def add_target_alias(value):
    value = str(value or "").strip()
    if not value:
        return
    lower = value.lower()
    TARGET["exact_aliases"].add(lower)
    if lower.startswith("@"):
        TARGET["exact_aliases"].add(lower[1:])
    normalized = normalize_token(lower)
    if len(normalized) >= 4:
        TARGET["norm_aliases"].add(normalized)


def resolve_target_identity():
    add_target_alias(CHANNEL_HANDLE)
    add_target_alias(CHANNEL_SLUG)
    add_target_alias(CHANNEL_URL)

    html = fetch_text(CHANNEL_URL)
    if not html:
        log(f"target_identity | channel_id={TARGET['channel_id'] or 'configured-only'}")
        return

    if not TARGET["channel_id"]:
        for pattern in (
            r'"externalId"\s*:\s*"(UC[A-Za-z0-9_-]{22})"',
            r'"channelId"\s*:\s*"(UC[A-Za-z0-9_-]{22})"',
            r'"browseId"\s*:\s*"(UC[A-Za-z0-9_-]{22})"',
            r'<meta\s+itemprop="channelId"\s+content="(UC[A-Za-z0-9_-]{22})"',
        ):
            match = re.search(pattern, html)
            if match:
                TARGET["channel_id"] = match.group(1)
                break

    for pattern in (
        r'"canonicalBaseUrl"\s*:\s*"(/@[^\"]+)"',
        r'"vanityChannelUrl"\s*:\s*"(https://www\.youtube\.com/[^\"]+)"',
        r'<link rel="canonical" href="https://www\.youtube\.com/([^\"]+)"',
    ):
        for match in re.finditer(pattern, html):
            add_target_alias(match.group(1))

    write_summary(target_channel_id=TARGET["channel_id"])
    log(f"target_identity | channel_id={TARGET['channel_id'] or 'unknown'}")


def target_match(info):
    if not info:
        return False, "empty_info"
    direct_id = str(info.get("channel_id") or "").strip()
    if TARGET.get("channel_id"):
        if direct_id:
            if direct_id == TARGET["channel_id"]:
                return True, "channel_id"
            return False, "wrong_channel_id"

    fields = [
        info.get("channel"), info.get("uploader"), info.get("channel_url"),
        info.get("uploader_url"), info.get("channel_id"), info.get("uploader_id"),
    ]
    blob = " ".join(str(value) for value in fields if value)
    lower_blob = blob.lower()

    for alias in TARGET["exact_aliases"]:
        if alias and alias in lower_blob:
            return True, "alias"

    normalized = normalize_token(blob)
    for alias in TARGET["norm_aliases"]:
        if alias and alias in normalized:
            return True, "alias_normalized"

    return False, blob or "owner_unknown"


def is_live_candidate(info):
    if not info or not info.get("id"):
        return False
    live_status = str(info.get("live_status") or "").casefold()
    return bool(info.get("is_live") or live_status == "is_live")


def run_ytdlp_json(url, client):
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "-J",
        "--skip-download", "--no-warnings", "--ignore-no-formats-error",
        "--playlist-items", "1",
    ] + JS_ARGS

    if client:
        command += ["--extractor-args", f"youtube:player_client={client}"]
    if USE_COOKIES and COOKIE_FILE and os.path.isfile(COOKIE_FILE):
        command += ["--cookies", COOKIE_FILE]
    command.append(url)

    try:
        result = subprocess.run(command, text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=PROBE_TIMEOUT_SECONDS)
        if result.returncode == 0 and result.stdout.strip():
            raw = json.loads(result.stdout)
            if raw.get("_type") == "playlist" and raw.get("entries"):
                for item in raw["entries"]:
                    if item and is_live_candidate(item):
                        return item
                return raw["entries"][0]
            return raw
    except Exception:
        pass
    return None


def extract_live_badge_ids(html):
    if not html:
        return []
    scores = {}
    patterns = [r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', r'watch\?v=([A-Za-z0-9_-]{11})', r'shorts/([A-Za-z0-9_-]{11})']
    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.IGNORECASE):
            video_id = match.group(1)
            context = html[max(0, match.start() - 1500):min(len(html), match.end() + 1500)].lower()
            score = scores.get(video_id, 0)
            if any(k in context for k in ("islivenow", "is_live_now", "badge_style_type_live_now", '"style":"live"')):
                score += 10
            if re.search(r'"label"\s*:\s*"live"', context) or re.search(r'"text"\s*:\s*"live"', context):
                score += 5
            if "watching" in context:
                score += 2
            scores[video_id] = score

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [video_id for video_id, score in ranked if score >= 5]


def extract_rss_video_ids(xml):
    if not xml:
        return []
    return re.findall(r"<yt:videoId>([A-Za-z0-9_-]{11})</yt:videoId>", xml)[:3]


def validate_candidate(video_id, client):
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    info = run_ytdlp_json(watch_url, client)
    if not info or not is_live_candidate(info):
        return None
    matched, reason = target_match(info)
    if not matched:
        return None
    return {"id": video_id, "url": watch_url, "client": client, "match_reason": reason}


def verify_live_id(video_id, source_kind):
    video_id = str(video_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        return None

    started = time.monotonic()
    executor = ThreadPoolExecutor(max_workers=len(VALIDATION_CLIENTS))
    futures = {executor.submit(validate_candidate, video_id, client): client for client in VALIDATION_CLIENTS}
    result = None

    try:
        for future in as_completed(futures, timeout=PROBE_TIMEOUT_SECONDS + 2):
            try:
                candidate = future.result()
            except Exception:
                candidate = None
            if candidate:
                result = candidate
                break
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False)

    if result:
        elapsed = time.monotonic() - started
        log(f"detect_live | source={source_kind} | live id={result['id']} | client={client_label(result['client'])} | match={result['match_reason']} | validation={elapsed:.1f}s")
        return {"id": result["id"], "url": result["url"], "source": source_kind}
    return None


def probe_fast():
    executor = ThreadPoolExecutor(max_workers=4)
    futures = {
        executor.submit(fetch_text, CHANNEL_URL): "home",
        executor.submit(run_ytdlp_json, LIVE_URL, None): "default_live",
        executor.submit(run_ytdlp_json, LIVE_URL, "android"): "android_live",
        executor.submit(run_ytdlp_json, LIVE_URL, "web_safari"): "web_live",
    }
    home_html = ""
    live_infos = {}

    try:
        for future in as_completed(futures, timeout=PROBE_TIMEOUT_SECONDS + 4):
            kind = futures[future]
            try:
                value = future.result()
            except Exception:
                value = None
            if kind == "home":
                home_html = value or ""
            else:
                live_infos[kind] = value
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False)

    for key, client in (("default_live", None), ("android_live", "android"), ("web_live", "web_safari")):
        info = live_infos.get(key)
        if not info or not is_live_candidate(info):
            continue
        matched, reason = target_match(info)
        if not matched:
            continue
        video_id = info["id"]
        log(f"detect_live | source=live_endpoint | live id={video_id} | client={client_label(client)} | match={reason}")
        return {"id": video_id, "url": f"https://www.youtube.com/watch?v={video_id}", "source": "live_endpoint"}

    for video_id in extract_live_badge_ids(home_html)[:2]:
        result = verify_live_id(video_id, "home_live_badge")
        if result:
            return result
    return None


def probe_secondary():
    channel_id = TARGET.get("channel_id")
    if not channel_id:
        return None

    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    executor = ThreadPoolExecutor(max_workers=3)
    futures = {
        executor.submit(fetch_text, rss_url): "rss",
        executor.submit(fetch_text, STREAMS_URL): "streams",
        executor.submit(fetch_text, SHORTS_URL): "shorts",
    }
    values = {"rss": "", "streams": "", "shorts": ""}

    try:
        for future in as_completed(futures, timeout=HTML_TIMEOUT_SECONDS + 4):
            kind = futures[future]
            try:
                value = future.result()
            except Exception:
                value = None
            values[kind] = value or ""
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False)

    for video_id in extract_rss_video_ids(values["rss"]):
        result = verify_live_id(video_id, "rss_feed")
        if result:
            return result

    for page_name in ("streams", "shorts"):
        for video_id in extract_live_badge_ids(values[page_name])[:2]:
            result = verify_live_id(video_id, f"{page_name}_badge")
            if result:
                return result
    return None


def stop_proc(process):
    if not process:
        return
    try:
        if process.poll() is not None:
            return
        process.terminate()
        process.wait(timeout=10)
        return
    except Exception:
        try:
            process.kill()
            process.wait(timeout=5)
        except Exception:
            pass


def sigterm_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log("termination_requested | stopping download; saved files will be recovered")
    with CURRENT_PROC_LOCK:
        process = CURRENT_PROC
    stop_proc(process)


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)
atexit.register(lambda: stop_proc(CURRENT_PROC))


def build_final_path(live_id, session_index):
    suffix = "" if session_index == 1 else f"_r{session_index}"
    return os.path.join(OUT_DIR, f"{CHANNEL_SLUG}_{live_id}{suffix}.ts")


def build_chunk_dir(live_id, session_index):
    return os.path.join(CHUNKS_DIR, f"{CHANNEL_SLUG}_{live_id}_r{session_index}")


def build_chunk_path(active, index=None):
    index = index or active["chunk_index"]
    os.makedirs(active["chunk_dir"], exist_ok=True)
    return os.path.join(active["chunk_dir"], f"{CHANNEL_SLUG}_{active['live_id']}_r{active['session_index']}_chunk{index:04d}.ts")


def atomic_json_write(path, payload):
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def save_active_state(active):
    if active is None:
        try:
            os.remove(STATE_PATH)
        except Exception:
            pass
        return
    try:
        atomic_json_write(STATE_PATH, active)
    except Exception as exc:
        log_raw("state_write_error", exc)


def chunk_number(path):
    match = re.search(r"_chunk(\d+)\.ts(?:\.part)?$", os.path.basename(path))
    return int(match.group(1)) if match else 0


def is_archived_short_exit(path):
    return "_short_exit_" in os.path.basename(path)


def register_recovered_chunks(active):
    os.makedirs(active["chunk_dir"], exist_ok=True)
    registered = [
        path for path in active.get("chunks", [])
        if os.path.isfile(path) and os.path.getsize(path) >= MIN_SAVE_BYTES and not is_archived_short_exit(path)
    ]
    known = set(registered)
    recovered = []

    candidates = glob.glob(os.path.join(active["chunk_dir"], "*.ts")) + glob.glob(os.path.join(active["chunk_dir"], "*.ts.part"))
    for path in sorted(candidates, key=chunk_number):
        if is_archived_short_exit(path):
            continue
        final_path = path[:-5] if path.endswith(".part") else path
        if path.endswith(".part") and not os.path.exists(final_path) and os.path.getsize(path) >= MIN_SAVE_BYTES:
            try:
                os.replace(path, final_path)
                path = final_path
            except Exception:
                pass
        if path.endswith(".part") or not os.path.isfile(path) or os.path.getsize(path) < MIN_SAVE_BYTES:
            continue
        if path not in known:
            registered.append(path)
            known.add(path)
            recovered.append(path)

    registered.sort(key=chunk_number)
    active["chunks"] = registered
    highest = max([chunk_number(path) for path in registered] + [0])
    active["chunk_index"] = max(int(active.get("chunk_index", 1)), highest + 1)
    active["recording_path"] = None
    if recovered:
        log(f"recovered_chunks | count={len(recovered)} | total={len(registered)}")
    return active


def load_active_state():
    if not os.path.exists(STATE_PATH):
        return None
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            active = json.load(fh)
        required = {"live_id", "session_index", "chunk_index", "final_ts", "chunk_dir", "chunks"}
        if not required.issubset(active):
            raise ValueError("missing required state fields")
        active.setdefault("preferred_client", "android")
        active.setdefault("client_locked", False)
        active.setdefault("short_exit_streak", 0)
        active.setdefault("short_exit_seq", 0)
        active.setdefault("recording_path", None)
        active.setdefault("last_failure_reason", None)
        active.setdefault("recovery_needed", True)
        active["offline_since"] = None
        active["cooldown_until"] = 0
        register_recovered_chunks(active)
        save_active_state(active)
        SESSION_ACTIVE.set()
        log(f"resume_state_loaded | live id={active['live_id']} | chunks={len(active['chunks'])}")
        return active
    except Exception as exc:
        log_raw("state_load_error", f"ignoring corrupt state | {exc}")
        return None


def new_session(live):
    live_id = live["id"]
    session_index = 1
    while os.path.exists(build_final_path(live_id, session_index)) or os.path.isdir(build_chunk_dir(live_id, session_index)):
        session_index += 1

    active = {
        "live_id": live_id,
        "live_url": f"https://www.youtube.com/watch?v={live_id}",
        "preferred_client": "android",
        "client_locked": False,
        "short_exit_streak": 0,
        "short_exit_seq": 0,
        "session_index": session_index,
        "chunk_index": 1,
        "final_ts": build_final_path(live_id, session_index),
        "chunk_dir": build_chunk_dir(live_id, session_index),
        "chunks": [],
        "recording_path": None,
        "offline_since": None,
        "cooldown_until": 0,
        "last_failure_reason": None,
        "recovery_needed": False,
    }
    os.makedirs(active["chunk_dir"], exist_ok=True)
    save_active_state(active)
    SESSION_ACTIVE.set()
    write_summary(video_id=live_id, detect_source=live.get("source"), status="session_open")
    log(f"opened_session | live id={live_id} | source={live.get('source')} | output={os.path.basename(active['final_ts'])}")
    return active


def archive_short_exit(active, chunk_path):
    if not chunk_path or not os.path.isfile(chunk_path):
        return None
    active["short_exit_seq"] = int(active.get("short_exit_seq", 0)) + 1
    stem, ext = os.path.splitext(chunk_path)
    archived = f"{stem}_short_exit_{active['short_exit_seq']:02d}{ext}"
    try:
        os.replace(chunk_path, archived)
        return archived
    except Exception:
        return chunk_path


def build_final_file(active):
    register_recovered_chunks(active)
    chunks = [
        path for path in active["chunks"]
        if os.path.isfile(path) and os.path.getsize(path) >= MIN_SAVE_BYTES and not is_archived_short_exit(path)
    ]
    chunks.sort(key=chunk_number)
    if not chunks:
        return None

    final_path = active["final_ts"]
    if len(chunks) == 1:
        try:
            os.replace(chunks[0], final_path)
        except Exception:
            try:
                shutil.copyfile(chunks[0], final_path)
            except Exception:
                return None
        return final_path if os.path.isfile(final_path) and os.path.getsize(final_path) >= MIN_SAVE_BYTES else None

    # Check if there is enough free disk space to safely concat multiple chunks
    total_chunk_bytes = sum(os.path.getsize(p) for p in chunks if os.path.isfile(p))
    free_bytes = shutil.disk_usage(OUT_DIR).free
    if free_bytes < total_chunk_bytes + 256 * 1024 * 1024:
        log(f"concat_skipped_low_disk | free={free_bytes / 1048576:.0f}MB < required={(total_chunk_bytes / 1048576) + 256:.0f}MB | preserving {len(chunks)} chunks for direct upload")
        return None

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log("finalize_error | ffmpeg unavailable; preserving chunks")
        return None

    list_path = os.path.join(active["chunk_dir"], "concat_list.txt")
    try:
        with open(list_path, "w", encoding="utf-8") as fh:
            for path in chunks:
                escaped = os.path.abspath(path).replace("'", "'\\''")
                fh.write(f"file '{escaped}'\n")

        result = subprocess.run(
            [ffmpeg, "-hide_banner", "-loglevel", "error", "-fflags", "+genpts", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", "-y", final_path],
            text=True, capture_output=True, timeout=1800,
        )
        if result.returncode == 0 and os.path.isfile(final_path) and os.path.getsize(final_path) >= MIN_SAVE_BYTES:
            for path in chunks:
                try: os.remove(path)
                except Exception: pass
            try: os.remove(list_path)
            except Exception: pass
            return final_path
        log_raw("ffmpeg_concat_failed", result.stderr)
    except Exception as exc:
        log_raw("ffmpeg_concat_error", exc)
    return None


def close_session(active, reason):
    SESSION_ACTIVE.clear()
    DOWNLOAD_ACTIVE.clear()
    final_path = build_final_file(active)
    if final_path and os.path.isfile(final_path):
        size_mb = os.path.getsize(final_path) / 1048576
        log(f"session_closed | {reason} | {size_mb:.1f} MB | {os.path.basename(final_path)} | chunks={len(active['chunks'])}")
    else:
        size_mb = sum(os.path.getsize(p) for p in active.get("chunks", []) if os.path.isfile(p)) / 1048576
        log(f"session_closed | chunks_preserved | {reason} | live id={active['live_id']} | chunks={len(active['chunks'])} | {size_mb:.1f} MB")

    write_summary(
        video_id=active["live_id"], final_file=(os.path.basename(final_path) if final_path else None),
        final_size_mb=round(size_mb, 1), chunk_count=len(active["chunks"]), status="session_closed", failure_reason=None,
    )
    save_active_state(None)


def output_reader(stream, output_state):
    for raw_line in iter(stream.readline, ""):
        if not raw_line:
            break
        print(raw_line, end="", flush=True)
        lowered = normalize_message(raw_line)
        if "chosen=" in raw_line or "error:" in lowered or "warning:" in lowered:
            log_raw("yt_dlp", raw_line)
        output_state["botcheck"] |= any(marker in lowered for marker in BOTCHECK_MARKERS)
        output_state["no_formats"] |= any(marker in lowered for marker in NO_FORMAT_MARKERS)
        output_state["rate_limited"] |= any(marker in lowered for marker in RATE_LIMIT_MARKERS)
        output_state["not_live"] |= any(marker in lowered for marker in NOT_LIVE_MARKERS)
        output_state["normal_exit"] |= any(marker in lowered for marker in NORMAL_END_MARKERS)


def classify_output(output_state):
    for key in ("botcheck", "no_formats", "rate_limited", "not_live"):
        if output_state[key]:
            return key
    return "normal_end" if output_state["normal_exit"] else "no_file"


def make_download_cmd(active, chunk_path, client):
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "--newline", "--no-warnings",
        "--restrict-filenames", "--no-part", "--no-playlist", "--no-live-from-start",
        "--print", "before_dl:chosen=%(format_id)s | %(resolution)s | tbr=%(tbr)s | protocol=%(protocol)s",
        "--retries", "20", "--extractor-retries", "20", "--file-access-retries", "20",
        "--fragment-retries", "infinite", "--retry-sleep", "http:2", "--retry-sleep", "fragment:1",
        "--skip-unavailable-fragments", "--hls-use-mpegts", "--downloader", "m3u8:native", "-N", "1",
        "-f", FORMAT_SELECTOR, "-S", SORT_EXPR, "-o", chunk_path,
    ] + JS_ARGS

    if client:
        command += ["--extractor-args", f"youtube:player_client={client}"]
    if USE_COOKIES and COOKIE_FILE and os.path.isfile(COOKIE_FILE):
        command += ["--cookies", COOKIE_FILE]
    command.append(active["live_url"])
    return command


def current_saved_path(base_path):
    if os.path.isfile(base_path):
        return base_path
    part_path = base_path + ".part"
    return part_path if os.path.isfile(part_path) else None


def finalize_download_path(base_path):
    path = current_saved_path(base_path)
    if path and path.endswith(".part"):
        try:
            os.replace(path, base_path)
            return base_path
        except Exception:
            return path
    return path


def append_chunk(active, path, client, reason):
    if path not in active["chunks"]:
        active["chunks"].append(path)
    active["chunks"].sort(key=chunk_number)
    active["chunk_index"] = max(active["chunk_index"], chunk_number(path) + 1)
    active["preferred_client"] = client
    active["client_locked"] = True
    active["recording_path"] = None
    active["last_failure_reason"] = None
    active["short_exit_streak"] = 0
    active["recovery_needed"] = False
    active["cooldown_until"] = 0
    active["offline_since"] = None
    save_active_state(active)

    total_mb = sum(os.path.getsize(item) for item in active["chunks"] if os.path.isfile(item)) / 1048576
    log(f"chunk_saved | reason={reason} | client={client_label(client)} | chunk={chunk_number(path):04d} | {os.path.getsize(path) / 1048576:.1f} MB | total={total_mb:.1f} MB")
    write_summary(video_id=active["live_id"], client_used=client_label(client), status="recording", chunk_count=len(active["chunks"]))


def take_replacement_live(active, live_queue):
    replacement = None
    try:
        while True:
            candidate = live_queue.get_nowait()
            if candidate and candidate.get("id") and candidate.get("id") != active["live_id"]:
                replacement = candidate
    except Empty:
        pass
    return replacement


def drain_latest_candidate(live_queue):
    latest = None
    try:
        while True:
            candidate = live_queue.get_nowait()
            if candidate and candidate.get("id"):
                latest = candidate
    except Empty:
        pass
    return latest


def run_download_attempt(active, session_end, client, live_queue):
    global CURRENT_PROC, STOP_REQUESTED, DISK_LIMIT_REACHED
    if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
        DISK_LIMIT_REACHED = True
        STOP_REQUESTED = True
        log("disk_floor_reached | stopping capture safely to preserve recorded chunks")
        return False, "termination", None

    index = active["chunk_index"]
    chunk_path = build_chunk_path(active, index)
    active["recording_path"] = chunk_path
    save_active_state(active)

    command = make_download_cmd(active, chunk_path, client)
    left = max(0, session_end - time.time())
    log(f"[{hms(left)} left] download_started | chunk={index:04d} | client={client_label(client)}")

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1)
    except Exception as exc:
        log_raw("download_launch_error", exc)
        active["recording_path"] = None
        save_active_state(active)
        return False, "launch_error", None

    with CURRENT_PROC_LOCK:
        CURRENT_PROC = process
    DOWNLOAD_ACTIVE.set()

    output_state = {"botcheck": False, "no_formats": False, "rate_limited": False, "not_live": False, "normal_exit": False}
    reader = threading.Thread(target=output_reader, args=(process.stdout, output_state), daemon=True)
    reader.start()

    started_at = time.time()
    last_heartbeat = 0
    last_size = -1
    last_growth_at = started_at
    stop_reason = None
    replacement_live = None

    try:
        while process.poll() is None and time.time() < session_end and not STOP_REQUESTED:
            time.sleep(POLL_INTERVAL_SECONDS)
            now = time.time()

            if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
                DISK_LIMIT_REACHED = True
                STOP_REQUESTED = True
                stop_reason = "low_disk"
                log("disk_floor_reached_mid_stream | stopping download safely")
                stop_proc(process)
                break

            replacement_live = take_replacement_live(active, live_queue)
            if replacement_live:
                stop_reason = "new_live_id"
                log(f"replacement_live_detected | old={active['live_id']} | new={replacement_live['id']}")
                stop_proc(process)
                break

            path = current_saved_path(chunk_path)
            size = os.path.getsize(path) if path else 0
            if size > last_size:
                last_size, last_growth_at = size, now

            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                log(f"[{hms(session_end - now)} left] heartbeat | chunk={index:04d} | client={client_label(client)} | {size / 1048576:.1f} MB")
                last_heartbeat = now

            if now - last_growth_at >= STALL_SECONDS:
                stop_reason = "stalled"
                log(f"download_stalled | chunk={index:04d} | no_growth={now - last_growth_at:.0f}s | restarting from live edge")
                stop_proc(process)
                break
    finally:
        if process.poll() is None:
            stop_proc(process)
        with CURRENT_PROC_LOCK:
            CURRENT_PROC = None
        DOWNLOAD_ACTIVE.clear()

    reader.join(timeout=8)
    duration = time.time() - started_at
    saved_path = finalize_download_path(chunk_path)
    saved_bytes = os.path.getsize(saved_path) if (saved_path and os.path.isfile(saved_path)) else 0
    explicit_end = output_state["not_live"] or output_state["normal_exit"]

    unexpected_short_exit = (
        stop_reason is None
        and duration < SHORT_EXIT_PROTECTION_SECONDS
        and not explicit_end
        and not STOP_REQUESTED
        and time.time() < session_end
    )

    if unexpected_short_exit:
        if saved_path and os.path.isfile(saved_path):
            archived = archive_short_exit(active, saved_path)
            active["short_exit_streak"] = int(active.get("short_exit_streak", 0)) + 1
            active["recording_path"] = None
            save_active_state(active)
            log(f"short_exit_detected | client={client_label(client)} | duration={duration:.1f}s | size={saved_bytes / 1048576:.1f}MB | archived={os.path.basename(archived) if archived else 'none'}")
        else:
            active["short_exit_streak"] = int(active.get("short_exit_streak", 0)) + 1
            active["recording_path"] = None
            save_active_state(active)
            log(f"short_exit_detected | client={client_label(client)} | duration={duration:.1f}s | size=0.0MB | archived=none")
        return False, "short_exit", replacement_live

    if saved_path and os.path.isfile(saved_path) and saved_bytes >= MIN_SAVE_BYTES:
        if STOP_REQUESTED: reason = "termination"
        elif time.time() >= session_end: reason = "session_end"
        elif stop_reason == "new_live_id": reason = "new_live_id"
        elif explicit_end: reason = classify_output(output_state)
        elif stop_reason == "stalled": reason = "stalled"
        else: reason = "downloader_exit"

        append_chunk(active, saved_path, client, reason)
        if replacement_live: return True, "new_live_id", replacement_live
        if reason in {"termination", "session_end", "not_live", "normal_end"}: return True, reason, None
        return True, reason, None

    if saved_path and os.path.isfile(saved_path):
        try: os.remove(saved_path)
        except Exception: pass

    active["recording_path"] = None
    reason = classify_output(output_state)
    if stop_reason and reason == "no_file":
        reason = stop_reason
    log(f"download_failed | client={client_label(client)} | reason={reason} | duration={duration:.1f}s")
    return False, reason, replacement_live


def record_until_stop(active, session_end, live_queue):
    if active.get("client_locked") and active.get("preferred_client") in DOWNLOAD_CLIENTS:
        preferred = active["preferred_client"]
        clients = [preferred] + [c for c in DOWNLOAD_CLIENTS if c != preferred]
    else:
        clients = list(DOWNLOAD_CLIENTS)

    last_reason = "download_failed"
    for client in clients:
        short_attempts = int(active.get("short_exit_streak", 0)) if client == active.get("preferred_client") else 0
        while time.time() < session_end and not STOP_REQUESTED:
            ok, reason, replacement_live = run_download_attempt(active, session_end, client, live_queue)
            if replacement_live: return False, "new_live_id", replacement_live
            if STOP_REQUESTED: return False, "termination", None
            if time.time() >= session_end: return False, "session_end", None
            if reason in {"not_live", "normal_end"}: return False, reason, None
            last_reason = reason

            if reason == "short_exit":
                short_attempts += 1
                if short_attempts <= SHORT_EXIT_RETRIES:
                    log(f"short_exit_retry | client={client_label(client)} | retry={short_attempts}/{SHORT_EXIT_RETRIES} | waiting={SHORT_EXIT_BACKOFF_SECONDS}s")
                    time.sleep(SHORT_EXIT_BACKOFF_SECONDS)
                    continue
                log(f"short_exit_limit_reached | client={client_label(client)} | moving to next client")
                break

            if ok and reason in {"stalled", "downloader_exit"}:
                short_attempts = 0
                if reason == "stalled":
                    time.sleep(STALL_RETRY_BACKOFF_SECONDS)
                continue
            break

        if client != clients[-1] and last_reason not in {"not_live", "normal_end"}:
            log(f"client_fallback_wait | {CLIENT_SWITCH_DELAY_SECONDS}s before fallback")
            time.sleep(CLIENT_SWITCH_DELAY_SECONDS)

    active["last_failure_reason"] = last_reason
    active["recovery_needed"] = True
    save_active_state(active)
    return False, last_reason, None


def primary_detector(session_end, stop_event, live_queue):
    next_run = time.time()
    while not stop_event.is_set() and time.time() < session_end and not STOP_REQUESTED:
        if time.time() < next_run:
            time.sleep(1)
            continue
        try:
            payload = probe_fast()
            if payload:
                live_queue.put(payload)
        except Exception as exc:
            log_raw("primary_detector_error", exc)

        interval = ACTIVE_SESSION_RECHECK_SECONDS if (SESSION_ACTIVE.is_set() and DOWNLOAD_ACTIVE.is_set()) else PRIMARY_RECHECK_SECONDS
        next_run = time.time() + interval


def secondary_detector(session_end, stop_event, live_queue):
    next_run = time.time() + 10
    while not stop_event.is_set() and time.time() < session_end and not STOP_REQUESTED:
        if time.time() < next_run:
            time.sleep(1)
            continue
        try:
            payload = probe_secondary()
            if payload:
                live_queue.put(payload)
        except Exception as exc:
            log_raw("secondary_detector_error", exc)

        interval = SECONDARY_ACTIVE_RECHECK_SECONDS if (SESSION_ACTIVE.is_set() and DOWNLOAD_ACTIVE.is_set()) else SECONDARY_WAITING_RECHECK_SECONDS
        next_run = time.time() + interval


def wait_for_start_time():
    value = str(START_AT_IST or "").strip()
    if not value or ZoneInfo is None:
        return
    try:
        hour, minute = map(int, value.split(":"))
        tz = ZoneInfo("Asia/Kolkata")
        now = datetime.now(tz)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        seconds = (target - now).total_seconds()
        log(f"scheduled_start | IST={target.strftime('%Y-%m-%d %H:%M:%S')} | waiting={hms(seconds)}")
        while seconds > 0 and not STOP_REQUESTED:
            time.sleep(min(5, seconds))
            now = datetime.now(tz)
            seconds = (target - now).total_seconds()
    except Exception as exc:
        log_raw("start_time_error", f"invalid START_AT_IST={value!r} | {exc}")


# =============================================================================
# MAIN
# =============================================================================

install_dependencies()
JS_ARGS = js_runtime_args()
resolve_target_identity()
wait_for_start_time()

session_end = time.time() + HOURS * 3600
active = load_active_state()

live_queue = Queue()
stop_event = threading.Event()

primary_thread = threading.Thread(target=primary_detector, args=(session_end, stop_event, live_queue), daemon=True)
secondary_thread = threading.Thread(target=secondary_detector, args=(session_end, stop_event, live_queue), daemon=True)

primary_thread.start()
secondary_thread.start()

log(f"started | channel={CHANNEL_HANDLE} | id={TARGET_CHANNEL_ID} | duration={HOURS}h | cookies={USE_COOKIES} | stall=240s")

try:
    while time.time() < session_end and not STOP_REQUESTED:
        now = time.time()
        candidate = drain_latest_candidate(live_queue)

        if candidate:
            if active is None:
                active = new_session(candidate)
            elif candidate["id"] != active["live_id"]:
                close_session(active, "new target live ID detected")
                active = new_session(candidate)
            else:
                active["offline_since"] = None
                active["cooldown_until"] = 0
                active["recovery_needed"] = False
                save_active_state(active)

        if active:
            active["live_url"] = f"https://www.youtube.com/watch?v={active['live_id']}"
            if (
                not DOWNLOAD_ACTIVE.is_set()
                and now >= active.get("cooldown_until", 0)
                and (active.get("recovery_needed", False) or not active.get("chunks") or candidate is not None)
            ):
                recorded, reason, replacement_live = record_until_stop(active, session_end, live_queue)
                if replacement_live:
                    close_session(active, "new target live ID detected while recording")
                    active = new_session(replacement_live)
                    continue

                if reason in {"termination", "session_end"}:
                    break

                if reason in {
                    "not_live", "normal_end", "download_failed", "launch_error",
                    "botcheck", "rate_limited", "no_formats", "no_file", "short_exit",
                }:
                    active["recovery_needed"] = True
                    active["last_failure_reason"] = reason
                    active["cooldown_until"] = time.time() + FAILURE_COOLDOWN_SECONDS
                    save_active_state(active)
                    log(f"recovery_cooldown | reason={reason} | waiting={FAILURE_COOLDOWN_SECONDS}s")
                    if active["offline_since"] is None:
                        active["offline_since"] = time.time()
                        save_active_state(active)

            if (
                active
                and active.get("offline_since") is not None
                and (time.time() - active["offline_since"]) >= OFFLINE_GRACE_SECONDS
                and not DOWNLOAD_ACTIVE.is_set()
            ):
                close_session(active, f"offline for {OFFLINE_GRACE_SECONDS}s")
                active = None

        time.sleep(1)

finally:
    stop_event.set()
    primary_thread.join(timeout=5)
    secondary_thread.join(timeout=5)
    with CURRENT_PROC_LOCK:
        process = CURRENT_PROC
    stop_proc(process)
    DOWNLOAD_ACTIVE.clear()

if active:
    close_session(active, "session complete")

log("session_complete")