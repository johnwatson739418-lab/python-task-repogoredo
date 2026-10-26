"""
YouTube Live Recorder -- Cloud Runner Engine
"""

import atexit
import contextlib
import glob
import importlib.util
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import tempfile
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
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "").strip()
START_AT_IST = os.getenv("START_AT_IST", "").strip()
HOURS = float(os.getenv("HOURS", "5.0"))
if not 0 < HOURS <= 5.0:
    raise SystemExit("HOURS must be greater than 0 and at most 5.0")
OUT_DIR = os.getenv("OUT_DIR", "./streams").strip()

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.txt").strip()

USE_COOKIES = (
    os.getenv("USE_COOKIES", "false").lower() == "true"
    and bool(COOKIE_FILE)
    and os.path.isfile(COOKIE_FILE)
    and os.path.getsize(COOKIE_FILE) > 20
)

FORMAT_SELECTOR = os.getenv(
    "FORMAT_SELECTOR",
    "bv*[height<=1080]+ba/b[height<=1080]/bv*[height<=720]+ba/b[height<=720]/bv*+ba/b",
)
SORT_EXPR = "res:1080,fps,br"

PRIMARY_RECHECK_SECONDS = 25
ACTIVE_SESSION_RECHECK_SECONDS = 120
SECONDARY_WAITING_RECHECK_SECONDS = 90
SECONDARY_ACTIVE_RECHECK_SECONDS = 120

PROBE_TIMEOUT_SECONDS = 35
HTML_TIMEOUT_SECONDS = 15

HEARTBEAT_SECONDS = 15
POLL_INTERVAL_SECONDS = 2
STALL_SECONDS = int(os.getenv("STALL_SECONDS", "120"))
OFFLINE_GRACE_SECONDS = 180
FAILURE_COOLDOWN_SECONDS = 30

FATAL_ERRORS_COUNT = 0
LAST_FATAL_ERROR = None
FATAL_ERROR_LOCK = threading.Lock()
LIVE_DETECTED_EVER = False


AUTH_BLOCKED = threading.Event()
PROBE_OBSERVATION_LOCK = threading.Lock()
PROBE_OBSERVATIONS = {"live": 0, "offline": 0, "unknown": 0}


def record_probe_observation(state):
    with PROBE_OBSERVATION_LOCK:
        PROBE_OBSERVATIONS[state] += 1


def record_fatal_error(reason):
    global FATAL_ERRORS_COUNT, LAST_FATAL_ERROR, STOP_REQUESTED
    with FATAL_ERROR_LOCK:
        FATAL_ERRORS_COUNT += 1
        LAST_FATAL_ERROR = str(reason)
    if reason in {"botcheck", "rate_limited", "invalid_cookies", "unavailable"}:
        # A denied account/source is not a reason to rotate extraction clients.
        AUTH_BLOCKED.set()
        STOP_REQUESTED = True


@contextlib.contextmanager
def private_cookie_copy():
    """Each child owns a 0600 jar; the original exported jar is never rewritten."""
    path = None
    try:
        if USE_COOKIES:
            fd, path = tempfile.mkstemp(prefix="recorder-cookie-", suffix=".txt")
            try:
                os.chmod(path, 0o600)
                target = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
                fd = None  # fd ownership transferred; never double-close after source failure.
                with target:
                    with open(COOKIE_FILE, "r", encoding="utf-8-sig") as source:
                        target.write(source.read())
            finally:
                if fd is not None:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
        yield path
    finally:
        if path:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def observation_result(saved_paths):
    """Return a truthful outcome for this observation window, including partial failures."""
    if AUTH_BLOCKED.is_set():
        return ("partial_failed" if saved_paths else "failed", LAST_FATAL_ERROR or "auth_blocked", 2)
    if saved_paths:
        return ("recorded", None, 0)
    if LIVE_DETECTED_EVER or FATAL_ERRORS_COUNT:
        return ("failed", LAST_FATAL_ERROR or "live_detected_but_zero_bytes", 2)
    with PROBE_OBSERVATION_LOCK:
        counts = dict(PROBE_OBSERVATIONS)
    if counts["unknown"] or not counts["offline"]:
        return ("unknown", "observation_incomplete", 2)
    return ("offline", None, 0)



MIN_SAVE_BYTES = 1880  # Preserve small captures; the publisher probes video validity.
MIN_FREE_BYTES = 512 * 1024 * 1024
COPY_BUFFER_BYTES = 8 * 1024 * 1024

SHORT_EXIT_PROTECTION_SECONDS = 90
SHORT_EXIT_RETRIES = 2
SHORT_EXIT_BACKOFF_SECONDS = 30
STALL_RETRY_BACKOFF_SECONDS = 4
CLIENT_SWITCH_DELAY_SECONDS = 5

# Use yt-dlp default client selection; never rotate clients after denial.
DOWNLOAD_CLIENTS = [None]
VALIDATION_CLIENTS = [None]

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
INVALID_COOKIE_MARKERS = ["cookies are no longer valid", "cookies have expired", "cookies are invalid"]
NO_FORMAT_MARKERS = ["no video formats found", "requested format is not available"]
RATE_LIMIT_MARKERS = ["http error 403", "http error 429", "try again later", "too many requests"]
NOT_LIVE_MARKERS = ["the channel is not currently live", "this live event will begin", "this live event has ended"]
UNAVAILABLE_MARKERS = ["private video", "removed by the uploader", "video has been removed", "sign in to confirm your age"]
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
    if os.getenv("SKIP_DEPENDENCY_INSTALL", "false").lower() == "true":
        required = [name for name in ("curl", "ffmpeg", "ffprobe") if not command_exists(name)]
        if not any(command_exists(name) for name in ("deno", "node", "nodejs")):
            required.append("javascript_runtime")
        if importlib.util.find_spec("yt_dlp") is None:
            required.append("yt_dlp")
        if required:
            raise SystemExit("Pre-provisioned dependencies missing: " + ",".join(required))
        log("dependencies_preprovisioned | no runtime package installation")
        return
    log(f"installing dependencies for {CHANNEL_HANDLE} (cookies={USE_COOKIES})")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "yt-dlp[default,curl-cffi]==2026.10.21"],
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

    global JS_ARGS
    JS_ARGS = js_runtime_args()


def js_runtime_args():
    args = []
    if command_exists("deno"):
        args += ["--js-runtimes", "deno"]
    elif command_exists("node"):
        args += ["--js-runtimes", "node"]
    elif command_exists("nodejs"):
        args += ["--js-runtimes", "node:" + shutil.which("nodejs")]
    return args


JS_ARGS = js_runtime_args()


def fetch_text(url):
    if AUTH_BLOCKED.is_set():
        return ""
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return ""
    cmd = [
        curl_bin, "-s", "-L", "--compressed", "--max-time", str(HTML_TIMEOUT_SECONDS),
        "-H", "User-Agent: Mozilla/5.0",
        "-H", "Accept-Language: en-US,en;q=0.9",
    ]
    try:
        with private_cookie_copy() as cookie_path:
            if cookie_path:
                cmd += ["-b", cookie_path]
            cmd.append(url)
            result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                                    errors="replace", timeout=HTML_TIMEOUT_SECONDS + 2)
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


def extraction_error_reason(stderr):
    message = normalize_message(stderr)
    groups = (("invalid_cookies", INVALID_COOKIE_MARKERS), ("botcheck", BOTCHECK_MARKERS),
              ("rate_limited", RATE_LIMIT_MARKERS), ("no_formats", NO_FORMAT_MARKERS),
              ("unavailable", UNAVAILABLE_MARKERS), ("not_live", NOT_LIVE_MARKERS))
    return next((reason for reason, markers in groups if any(m in message for m in markers)), None)


def register_owned_process(process, kind):
    """Allow the capture wrapper to clean groups if this recorder is hard-killed."""
    registry = os.environ.get("RECORDER_PROCESS_REGISTRY", "")
    if not registry or os.name != "posix":
        return
    directory = os.path.abspath(registry)
    if not os.path.isdir(directory) or os.path.islink(directory):
        raise RuntimeError("invalid_owned_process_registry")
    pid = process.pid
    if not isinstance(pid, int) or pid <= 1 or os.getpgid(pid) != pid:
        raise RuntimeError("capture_process_group_not_owned")
    with open(f"/proc/{pid}/stat", encoding="utf-8") as stat_file:
        fields = stat_file.read().rsplit(")", 1)[1].split()
    ticks = int(fields[19])  # /proc stat field22; comm may contain spaces/parentheses.
    fd, temporary = tempfile.mkstemp(prefix=".register-", dir=directory)
    destination = os.path.join(directory, f"{pid}-{ticks}.json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            json.dump({"pid": pid, "proc_start_ticks": ticks, "kind": kind}, target)
        os.replace(temporary, destination)
        process._recorder_registry_path = destination
    finally:
        try:
            os.remove(temporary)
        except FileNotFoundError:
            pass


def bounded_metadata_run(command, *, timeout):
    """Reap yt-dlp and its JS descendants before its private cookie jar is removed."""
    process = subprocess.Popen(command, text=True, encoding="utf-8", errors="replace",
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=(os.name == "posix"))
    process._recorder_owned_group = (os.name == "posix")
    try:
        register_owned_process(process, "metadata")
        stdout, stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    finally:
        stop_proc(process)
        for stream in (process.stdout, process.stderr):
            if stream:
                stream.close()


def run_ytdlp_json(url, client):
    global LIVE_DETECTED_EVER
    if AUTH_BLOCKED.is_set():
        return None
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "--no-cache-dir", "-J",
        "--skip-download", "--socket-timeout", "15", "--retries", "1",
        "--extractor-retries", "1", "--playlist-items", "1",
    ] + JS_ARGS
    if client:
        command += ["--extractor-args", f"youtube:player_client={client}"]
    log(f"ytdlp_exec | client={client_label(client)} | source=metadata")
    try:
        with private_cookie_copy() as cookie_path:
            if cookie_path:
                command += ["--cookies", cookie_path]
            command.append(url)
            result = bounded_metadata_run(command, timeout=PROBE_TIMEOUT_SECONDS)
        log(f"ytdlp_done | client={client_label(client)} | exit={result.returncode}")
        reason = extraction_error_reason(result.stderr)
        # Check access errors even when yt-dlp returned partial JSON with exit 0.
        if reason == "unavailable" and url != LIVE_URL:
            # An old discovery/RSS item may be private after its live ended.
            # It is not evidence that the account or current live endpoint is blocked.
            log("candidate_rejected | reason=unavailable")
            return None
        if reason in {"invalid_cookies", "botcheck", "rate_limited", "unavailable", "no_formats"}:
            record_fatal_error(reason)
            log(f"probe_failed | client={client_label(client)} | reason={reason}")
            return None
        if result.returncode:
            if reason == "not_live" and url == LIVE_URL:
                record_probe_observation("offline")
                return {"live_status": "not_live", "_observation": "offline"}
            else:
                record_probe_observation("unknown")
            log(f"probe_failed | client={client_label(client)} | reason={reason or 'extraction_failed'}")
            return None
        raw = json.loads(result.stdout)
        if not isinstance(raw, dict):
            raise ValueError("metadata_not_object")
        if raw.get("_type") == "playlist":
            entries = [item for item in raw.get("entries", []) if isinstance(item, dict)]
            raw = next((item for item in entries if is_live_candidate(item)), entries[0] if entries else {})
        status = str(raw.get("live_status") or "").casefold()
        if is_live_candidate(raw):
            formats = [item for item in (raw.get("formats") or [])
                       if isinstance(item, dict) and str(item.get("url") or "").startswith(("https://", "http://"))
                       and str(item.get("protocol") or "").lower() != "mhtml"
                       and str(item.get("ext") or "").lower() not in {"mhtml", "jpg", "jpeg", "png", "webp"}]
            has_video = any(item.get("vcodec") not in {None, "none", "images"} for item in formats)
            has_audio = any(item.get("acodec") not in {None, "none"} for item in formats)
            if not (has_video and has_audio):
                record_probe_observation("unknown")
                log("probe_incomplete | reason=no_playable_formats")
                return None
            LIVE_DETECTED_EVER = True
            record_probe_observation("live")
            return raw
        if url == LIVE_URL and status in {"not_live", "was_live", "post_live", "is_upcoming"}:
            record_probe_observation("offline")
            return raw
        record_probe_observation("unknown")
        log("probe_incomplete | reason=live_status_unknown")
    except subprocess.TimeoutExpired:
        record_probe_observation("unknown")
        log(f"probe_failed | client={client_label(client)} | reason=timeout")
    except Exception as exc:
        record_probe_observation("unknown")
        log(f"probe_failed | client={client_label(client)} | reason={type(exc).__name__}")
    return None


def extract_live_badge_ids(html):
    """Parse complete JSON renderer objects; badges never bleed across siblings."""
    if not html:
        return []
    decoder = json.JSONDecoder()
    found = []
    renderer = re.compile(r'"(?:videoRenderer|compactVideoRenderer|gridVideoRenderer)"\s*:\s*(?=\{)')
    def live_badge(value):
        if isinstance(value, dict):
            if value.get("isLiveNow") is True or value.get("is_live_now") is True:
                return True
            if value.get("style") in {"BADGE_STYLE_TYPE_LIVE_NOW", "LIVE"}:
                return True
            if value.get("label") == "LIVE" or value.get("text") == "LIVE":
                return True
            return any(live_badge(v) for k, v in value.items()
                       if k not in {"videoRenderer", "compactVideoRenderer", "gridVideoRenderer"})
        if isinstance(value, list):
            return any(live_badge(v) for v in value)
        return False
    for match in renderer.finditer(html):
        try:
            item, _ = decoder.raw_decode(html, match.end())
        except (ValueError, RecursionError):
            continue
        video_id = item.get("videoId") if isinstance(item, dict) else None
        scoped_badge = isinstance(item, dict) and (
            item.get("isLiveNow") is True or item.get("is_live_now") is True
            or any(live_badge(item.get(key)) for key in ("badges", "thumbnailOverlays")))
        if isinstance(video_id, str) and re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) and scoped_badge:
            if video_id not in found:
                found.append(video_id)
    return found


def extract_rss_video_ids(xml):
    if not xml:
        return []
    return re.findall(r"<yt:videoId>([A-Za-z0-9_-]{11})</yt:videoId>", xml)[:3]


def validate_candidate(video_id, client):
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    info = run_ytdlp_json(watch_url, client)
    if not info:
        log(f"validate_candidate | vid={video_id} | client={client_label(client)} | no_info")
        return None
    is_live = is_live_candidate(info)
    matched, reason = target_match(info)
    log(f"validate_candidate | vid={video_id} | client={client_label(client)} | is_live={is_live} | matched={matched} ({reason})")
    if not is_live or not matched:
        return None
    return {"id": video_id, "url": watch_url, "client": client, "match_reason": reason}


def verify_live_id(video_id, source_kind):
    video_id = str(video_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        return None

    started = time.monotonic()
    for client in VALIDATION_CLIENTS:
        if AUTH_BLOCKED.is_set():
            return None
        candidate = validate_candidate(video_id, client)
        if candidate:
            global LIVE_DETECTED_EVER
            LIVE_DETECTED_EVER = True
            elapsed = time.monotonic() - started
            log(f"detect_live | source={source_kind} | live id={candidate['id']} | client={client_label(client)} | match={candidate['match_reason']} | validation={elapsed:.1f}s")
            return {"id": candidate["id"], "url": candidate["url"], "source": source_kind, "detected_client": client}

    return None


def probe_fast():
    log(f"probe_fast_start | LIVE_URL={LIVE_URL}")
    info = run_ytdlp_json(LIVE_URL, None)
    if not info:
        log("probe_fast | ytdlp returned no info")
    else:
        if str(info.get("live_status") or "").casefold() in {"not_live", "was_live", "post_live", "is_upcoming"}:
            return None
        video_id = info.get("id")
        is_live = is_live_candidate(info)
        matched, reason = target_match(info)
        log(f"probe_fast_eval | id={video_id} | is_live={is_live} | live_status={info.get('live_status')} | matched={matched} ({reason})")
        if is_live and matched:
            global LIVE_DETECTED_EVER
            LIVE_DETECTED_EVER = True
            log(f"detect_live | source=live_endpoint | live id={video_id} | client=default | match={reason}")
            return {"id": video_id, "url": f"https://www.youtube.com/watch?v={video_id}", "source": "live_endpoint", "detected_client": None}

        # If info gave a video ID (e.g. from channel tab entry where live_status is omitted), verify its watch URL directly
        if video_id and re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            result = verify_live_id(video_id, "live_endpoint_watch")
            if result:
                return result

    if AUTH_BLOCKED.is_set():
        return None
    # Fallback to channel home page if direct live endpoint was inconclusive
    home_html = fetch_text(CHANNEL_URL)
    badge_ids = extract_live_badge_ids(home_html)
    log(f"probe_fast_fallback | badge_ids={badge_ids}")
    for video_id in badge_ids[:2]:
        result = verify_live_id(video_id, "home_live_badge")
        if result:
            return result
    return None


def probe_secondary():
    if AUTH_BLOCKED.is_set():
        return None
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
    owned_group = bool(getattr(process, "_recorder_owned_group", False))
    pid = getattr(process, "pid", None)
    owned_group = owned_group and isinstance(pid, int) and pid > 1 and os.name == "posix"
    group_cleanup_done = False
    def send(signum):
        nonlocal group_cleanup_done
        try:
            if owned_group:
                os.killpg(pid, signum)
                if signum == signal.SIGKILL:
                    group_cleanup_done = True
            elif process.poll() is None:
                process.send_signal(signum)
        except ProcessLookupError:
            if owned_group:
                group_cleanup_done = True
        except OSError:
            pass
    try:
        if process.poll() is None:
            send(signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                send(signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    send(signal.SIGKILL)
                    process.wait(timeout=5)
    except Exception:
        send(signal.SIGKILL)
    finally:
        # The leader may have exited while an ffmpeg descendant is still alive.
        # Only a capture process explicitly created by us owns this group.
        if owned_group:
            send(signal.SIGKILL)
        try:
            process.wait(timeout=2)
        except Exception:
            pass
        registry_path = getattr(process, "_recorder_registry_path", None)
        if owned_group and group_cleanup_done and isinstance(registry_path, str):
            try:
                os.remove(registry_path)
            except FileNotFoundError:
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
    match = re.search(r"_chunk(\d+)", os.path.basename(path))
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

    candidates = [
        p for p in (
            glob.glob(os.path.join(active["chunk_dir"], "*.ts*"))
            + glob.glob(os.path.join(active["chunk_dir"], "*.mp4*"))
            + glob.glob(os.path.join(active["chunk_dir"], "*.mkv*"))
        )
        if not is_archived_short_exit(p)
    ]
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
        "preferred_client": live.get("detected_client", "web_safari"),
        "client_locked": True,
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

    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log("finalize_error | ffmpeg unavailable; preserving chunks")
        return None

    required = sum(os.path.getsize(path) for path in chunks) + MIN_FREE_BYTES
    if shutil.disk_usage(OUT_DIR).free < required:
        log("finalize_skipped | insufficient disk for a second full copy; preserving source chunks for upload")
        return None

    list_path = os.path.join(active["chunk_dir"], "concat_list.txt")
    try:
        with open(list_path, "w", encoding="utf-8") as fh:
            for path in chunks:
                escaped = os.path.abspath(path).replace("'", "'\\''")
                fh.write(f"file '{escaped}'\n")

        result = subprocess.run(
            [ffmpeg, "-hide_banner", "-loglevel", "error", "-fflags", "+genpts", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", "-y", final_path],
            text=True, capture_output=True, timeout=180,
        )
        if result.returncode == 0 and os.path.isfile(final_path) and os.path.getsize(final_path) >= MIN_SAVE_BYTES:
            # A successful mux/size check is not a verified remote receipt.
            # Preserve every source chunk for independent recovery/publication.
            try: os.remove(list_path)
            except Exception: pass
            return final_path
        log_raw("ffmpeg_concat_failed", result.stderr)
        # Do not let the workflow mistake an incomplete FFmpeg output for a
        # finished recording; keep the source chunks for per-chunk upload.
        try:
            if os.path.isfile(final_path):
                os.remove(final_path)
        except Exception:
            pass
    except Exception as exc:
        log_raw("ffmpeg_concat_error", exc)
        try:
            if os.path.isfile(final_path):
                os.remove(final_path)
        except Exception:
            pass
    return None


def close_session(active, reason):
    SESSION_ACTIVE.clear()
    DOWNLOAD_ACTIVE.clear()
    final_path = build_final_file(active)
    if final_path and os.path.isfile(final_path):
        size_mb = os.path.getsize(final_path) / 1048576
        log(f"session_closed | {reason} | {size_mb:.1f} MB | {os.path.basename(final_path)} | chunks={len(active['chunks'])}")
    else:
        size_mb = 0.0
        log(f"session_closed | no_final_file | {reason} | live id={active['live_id']} | chunks_preserved={len(active['chunks'])}")

    write_summary(
        video_id=active["live_id"], final_file=(os.path.basename(final_path) if final_path else None),
        final_size_mb=round(size_mb, 1), chunk_count=len(active["chunks"]), status="session_closed", failure_reason=None,
    )
    save_active_state(None)


def output_reader(stream, output_state):
    for raw_line in iter(stream.readline, ""):
        if not raw_line:
            break
        lowered = normalize_message(raw_line)
        if "chosen=" in raw_line:
            safe = re.sub(r"https?://\S+", "[URL]", raw_line.strip())
            log_raw("yt_dlp_format", safe[:300])
        output_state["botcheck"] |= any(marker in lowered for marker in BOTCHECK_MARKERS + INVALID_COOKIE_MARKERS)
        output_state["no_formats"] |= any(marker in lowered for marker in NO_FORMAT_MARKERS)
        output_state["rate_limited"] |= any(marker in lowered for marker in RATE_LIMIT_MARKERS)
        output_state["not_live"] |= any(marker in lowered for marker in NOT_LIVE_MARKERS)
        output_state["normal_exit"] |= any(marker in lowered for marker in NORMAL_END_MARKERS)
        output_state["unavailable"] |= any(marker in lowered for marker in UNAVAILABLE_MARKERS)


def classify_output(output_state):
    for key in ("botcheck", "rate_limited", "unavailable", "no_formats", "not_live"):
        if output_state.get(key, False):
            return key
    return "normal_end" if output_state["normal_exit"] else "no_file"


def make_download_cmd(active, chunk_path, client, cookie_path=None):
    transport = os.getenv("CAPTURE_TRANSPORT", "native").strip().lower()
    if USE_COOKIES and not cookie_path:
        raise RuntimeError("private_cookie_path_required")
    if transport == "streamlink_hls":
        helper = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hls_capture.py")
        if not os.path.isfile(helper):
            raise RuntimeError("hls_capture_helper_missing")
        command = [sys.executable, helper, "--url", active["live_url"], "--output", chunk_path,
                   "--format", FORMAT_SELECTOR, "--client", client_label(client)]
        if cookie_path:
            command += ["--cookies", cookie_path]
        return command
    if transport != "native":
        raise RuntimeError("unsupported_capture_transport")
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "--newline", "--no-warnings",
        "--socket-timeout", "15", "--restrict-filenames", "--no-part", "--no-playlist", "--no-live-from-start",
        "--print", "before_dl:chosen=%(format_id)s | %(resolution)s | tbr=%(tbr)s | protocol=%(protocol)s",
        "--retries", "20", "--extractor-retries", "20", "--file-access-retries", "20",
        "--fragment-retries", "infinite", "--retry-sleep", "http:2", "--retry-sleep", "fragment:1",
        "--skip-unavailable-fragments", "--hls-use-mpegts", "--downloader", "m3u8:native", "-N", "1",
        "-f", FORMAT_SELECTOR, "-S", SORT_EXPR, "-o", chunk_path,
    ]

    command += JS_ARGS
    if client:
        command += ["--extractor-args", f"youtube:player_client={client}"]
    if USE_COOKIES and not cookie_path:
        raise RuntimeError("private_cookie_path_required")
    if cookie_path:
        command += ["--cookies", cookie_path]
    command.append(active["live_url"])
    return command


def current_saved_path(base_path):
    if os.path.isfile(base_path):
        return base_path
    for ext in (".part", ".mp4", ".ts.mp4", ".mkv", ".webm"):
        candidate = base_path + ext
        if os.path.isfile(candidate):
            return candidate
    base_no_ext = os.path.splitext(base_path)[0]
    for ext in (".mp4", ".ts", ".mkv", ".webm"):
        candidate = base_no_ext + ext
        if os.path.isfile(candidate):
            return candidate
    return None


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
    active["access_failure_streak"] = 0
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
    if AUTH_BLOCKED.is_set():
        return False, LAST_FATAL_ERROR or "auth_blocked", None
    with private_cookie_copy() as cookie_path:
        return _run_download_attempt(active, session_end, client, live_queue, cookie_path)


def _run_download_attempt(active, session_end, client, live_queue, cookie_path):
    global CURRENT_PROC, STOP_REQUESTED, DISK_LIMIT_REACHED
    index = active["chunk_index"]
    chunk_path = build_chunk_path(active, index)
    active["recording_path"] = chunk_path
    save_active_state(active)

    command = make_download_cmd(active, chunk_path, client, cookie_path)
    left = max(0, session_end - time.time())
    log(f"[{hms(left)} left] download_started | chunk={index:04d} | client={client_label(client)}")

    process = None
    try:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
            start_new_session=(os.name == "posix"),
        )
        process._recorder_owned_group = (os.name == "posix")
        register_owned_process(process, "capture")
    except Exception as exc:
        if process is not None:
            stop_proc(process)
        log_raw("download_launch_error", type(exc).__name__)
        active["recording_path"] = None
        save_active_state(active)
        if "record_fatal_error" in globals() and callable(globals()["record_fatal_error"]):
            globals()["record_fatal_error"]("launch_error")
        return False, "launch_error", None



    with CURRENT_PROC_LOCK:
        CURRENT_PROC = process
    DOWNLOAD_ACTIVE.set()

    output_state = {"botcheck": False, "no_formats": False, "rate_limited": False, "not_live": False, "normal_exit": False, "unavailable": False}
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
            if output_state["botcheck"] or output_state["rate_limited"] or output_state["unavailable"]:
                stop_reason = "access_denied"
                stop_proc(process)
                break
            if shutil.disk_usage(OUT_DIR).free < MIN_FREE_BYTES:
                DISK_LIMIT_REACHED = True
                STOP_REQUESTED = True
                log("disk_limit_reached | stopping capture gracefully; upload saved media and mark recording incomplete")
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

    # Preserve explicit authentication/rate-limit errors before short-exit handling.
    if output_state["botcheck"] or output_state["rate_limited"] or output_state["unavailable"] or output_state["no_formats"]:
        # Keep partial media even if a later request loses access.
        if saved_path and saved_bytes >= MIN_SAVE_BYTES:
            append_chunk(active, saved_path, client, "access_lost")
        active["recording_path"] = None
        reason = classify_output(output_state)
        save_active_state(active)
        if "record_fatal_error" in globals() and callable(globals()["record_fatal_error"]):
            globals()["record_fatal_error"](reason)
        log(f"download_blocked | client={client_label(client)} | reason={reason}")
        return False, reason, replacement_live


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
    if reason in {"botcheck", "rate_limited", "unavailable", "no_formats", "extraction_failed"}:
        if "record_fatal_error" in globals() and callable(globals()["record_fatal_error"]):
            globals()["record_fatal_error"](reason)
    return False, reason, replacement_live




def recovery_delay(reason, active):
    if reason in {"botcheck", "rate_limited", "unavailable"}:
        streak = int(active.get("access_failure_streak", 0)) + 1
        active["access_failure_streak"] = streak
        initial = 600 if reason == "unavailable" else 300
        return min(3600, initial * (2 ** min(streak - 1, 4)))
    return max(30, FAILURE_COOLDOWN_SECONDS)


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
            if reason in {"not_live", "normal_end", "botcheck", "rate_limited", "unavailable"}:
                # Switching clients repeats the same denied video request.
                return False, reason, None
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

def main():
    global JS_ARGS, STOP_REQUESTED, DISK_LIMIT_REACHED, CURRENT_PROC
    log(f"process_boot | channel={CHANNEL_HANDLE} | utc={datetime.utcnow().isoformat(timespec='seconds')}Z")
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

    log(f"started | channel={CHANNEL_HANDLE} | id={TARGET_CHANNEL_ID} | duration={HOURS}h | cookies={USE_COOKIES} | stall={STALL_SECONDS}s")


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
                        "botcheck", "rate_limited", "unavailable", "no_formats", "no_file", "short_exit",
                    }:
                        active["recovery_needed"] = True
                        active["last_failure_reason"] = reason
                        cooldown = recovery_delay(reason, active)
                        active["cooldown_until"] = time.time() + cooldown
                        save_active_state(active)
                        log(f"recovery_cooldown | reason={reason} | waiting={cooldown}s")
                        if reason in {"not_live", "normal_end"} and active["offline_since"] is None:
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
    if DISK_LIMIT_REACHED:
        raise SystemExit(3)
    saved_paths = [
        path for path in (
            glob.glob(os.path.join(OUT_DIR, "**", "*.ts"), recursive=True)
            + glob.glob(os.path.join(OUT_DIR, "**", "*.mp4"), recursive=True)
            + glob.glob(os.path.join(OUT_DIR, "**", "*.mkv"), recursive=True)
        )
        if os.path.isfile(path) and os.path.getsize(path) >= MIN_SAVE_BYTES and not path.endswith(".part")
    ]

    status, failure_reason, exit_code = observation_result(saved_paths)
    with PROBE_OBSERVATION_LOCK:
        counts = dict(PROBE_OBSERVATIONS)
    write_summary(status=status, failure_reason=failure_reason,
                  observation_status=status, probe_observations=counts,
                  saved_file_count=len(saved_paths), media_saved=bool(saved_paths))
    log(f"window_result | status={status} | saved_files={len(saved_paths)} | reason={failure_reason or 'none'}")
    raise SystemExit(exit_code)



if __name__ == "__main__":
    main()
