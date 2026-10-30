"""
YouTube Live Recorder -- Cloud Runner Engine (v8.8)

Builds on v8.7. Changes in v8.8:
 1. probe_fast() now checks /live, channel home AND /streams in parallel (3 workers),
    so vertical / Shorts-style lives that only show a LIVE badge on /streams are caught in ~20s.
 2. Chrome TLS impersonation is gated on a real capability check (yt-dlp --list-impersonate-targets),
    so a failed curl-cffi install can no longer break every web-type client.
 3. WARP preference is time-boxed (WARP_PREFER_SECONDS) instead of sticky-for-the-whole-run, and
    every route (WARP / direct) is tried in order, so a blocked WARP IP falls back to direct.
 4. "experiencing technical difficulties" is a TRANSIENT, client-specific error: it switches client
    but only escalates to an IP-block (WARP) after it hits 2+ different clients in one pass.
 5. Dispatch of the next shift tries GH_TOKEN, then GH_FALLBACK_TOKEN, and logs real errors.
 6. Dropped ffmpeg-only downloader args that were ignored by the native HLS downloader.
 7. botcheck streak resets after a successful chunk; WARP health check is cached (15s).
 8. Optional use_cookies pass-through to continuation shifts.
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
from datetime import datetime, timedelta, timezone
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
SHIFTS_LEFT = int(os.getenv("SHIFTS_LEFT", "0") or "0")
ALWAYS_CONTINUE = os.getenv("ALWAYS_CONTINUE", "true").strip().lower() in ("1", "true", "yes")
OUT_DIR = os.getenv("OUT_DIR", "./streams").strip()

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.txt").strip()
if COOKIE_FILE and os.path.isfile(COOKIE_FILE):
    try:
        raw_cookie = open(COOKIE_FILE, "r", encoding="utf-8-sig", errors="replace").read().lstrip("\ufeff")
        open(COOKIE_FILE, "w", encoding="utf-8", newline="\n").write(raw_cookie)
    except Exception:
        pass

USE_COOKIES = (
    os.getenv("FORCE_COOKIES", "false").lower() == "true"
    and os.getenv("USE_COOKIES", "false").lower() == "true"
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
FAILURE_COOLDOWN_SECONDS = 8
PREWARM_LEAD_SECONDS = 180
PREWARM_MAX_ATTEMPTS = 2
PREWARM_BACKOFF_SECONDS = 60
PREWARM_ACTIVE_GROWTH_SEC = 15

MIN_SAVE_BYTES = 1_000_000
MIN_FREE_DISK_BYTES = 512 * 1024 * 1024
COPY_BUFFER_BYTES = 8 * 1024 * 1024

SHORT_EXIT_PROTECTION_SECONDS = 90
SHORT_EXIT_RETRIES = 3
SHORT_EXIT_BACKOFF_SECONDS = 4
STALL_RETRY_BACKOFF_SECONDS = 2
CLIENT_SWITCH_DELAY_SECONDS = 4

DOWNLOAD_CLIENTS = ["android_vr", "android", "ios", "mweb", "web_safari", "tv", None]
VALIDATION_CLIENTS = ["android_vr", "mweb", "web_safari"]
SKIP_WEBPAGE_CLIENTS = {"android_vr", "android", "ios"}
COOKIE_INCOMPATIBLE_CLIENTS = {"android_vr", "android", "ios"}
IMPERSONATE_CLIENTS = {"mweb", "web_safari", "tv", None}
BOTCHECK_BASE_COOLDOWN_SECONDS = 45

# How long to keep preferring the WARP route after a direct (Azure) IP block was seen.
WARP_PREFER_SECONDS = 600
WARP_CHECK_CACHE_SECONDS = 15

BGUTIL_PROC = None
POT_SERVER_ACTIVE = False
IMPERSONATE_OK = False
NON_LIVE_VOD_CACHE = set()
YTDLP_PROBE_COOLDOWN_UNTIL = 0.0
WARP_PREFERRED_UNTIL = 0.0
LAST_BLOCKED_LOG_AT = 0.0
LAST_HEARTBEAT_LOG_AT = 0.0
PROBE_STATS = {"clean_offline": 0, "blocked": 0, "error": 0, "live_hits": 0}
_WARP_CACHE = {"t": 0.0, "v": ""}

SESSION_ACTIVE = threading.Event()
DOWNLOAD_ACTIVE = threading.Event()

STOP_REQUESTED = False
DISK_LIMIT_REACHED = False
CONTINUATION_DISPATCHED = False
PREWARM_ATTEMPTS = 0
NEXT_PREWARM_AT = 0.0
CURRENT_PROC = None
CURRENT_PROC_LOCK = threading.Lock()
LOG_LOCK = threading.Lock()
RUN_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%MUTC")


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
RATE_LIMIT_MARKERS = [
    "rate-limited by youtube",
    "the current session has been rate-limited",
    "http error 429",
    "too many requests",
]
# Client-specific / temporary YouTube-side error. Switch client first; only treat as an IP block
# if it keeps happening across several different clients (see record_until_stop).
TRANSIENT_MARKERS = ["experiencing technical difficulties"]
ACCESS_RESTRICTED_MARKERS = ["http error 403", "try again later", "this content isn't available"]
NOT_LIVE_MARKERS = [
    "the channel is not currently live",
    "this live event will begin",
    "this live event has ended",
    "does not pass filter",
]
NORMAL_END_MARKERS = ["finished downloading", "download complete", "has already been downloaded"]

IP_BLOCK_REASONS = {"botcheck", "rate_limited", "access_restricted"}
CLIENT_SWITCH_REASONS = IP_BLOCK_REASONS | {"no_formats", "transient"}


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
    "close_reason": None,
    "still_live_at_end": False,
    "continuation_dispatched": False,
    "blocked_by_youtube": False,
    "downloader_rate_limited": False,
    "probe_stats": PROBE_STATS,
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


# --------------------------------------------------------------------------------------
# Route selection (direct vs Cloudflare WARP)
# --------------------------------------------------------------------------------------

def warp_proxy_url():
    """Verify local Cloudflare WARP SOCKS5 proxy on 127.0.0.1:40000 via SOCKS5 handshake. Cached briefly when active."""
    now = time.time()
    if now - _WARP_CACHE["t"] < WARP_CHECK_CACHE_SECONDS and _WARP_CACHE["v"]:
        return _WARP_CACHE["v"]
    value = ""
    try:
        with socket.create_connection(("127.0.0.1", 40000), timeout=0.8) as sock:
            sock.settimeout(0.8)
            sock.sendall(b"\x05\x01\x00")
            if sock.recv(2) == b"\x05\x00":
                value = os.getenv("WARP_PROXY", "").strip() or "socks5://127.0.0.1:40000"
    except Exception:
        value = ""
    _WARP_CACHE["t"], _WARP_CACHE["v"] = now, value
    return value


def prefer_warp():
    """True only while the time-boxed WARP preference is active and the proxy is really up."""
    return time.time() < WARP_PREFERRED_UNTIL and bool(warp_proxy_url())


def mark_ip_blocked(reason):
    """Record a direct-IP block. Prefers WARP for WARP_PREFER_SECONDS, then re-tests direct."""
    global WARP_PREFERRED_UNTIL
    write_summary(blocked_by_youtube=True)
    if warp_proxy_url():
        was_preferred = prefer_warp()
        WARP_PREFERRED_UNTIL = time.time() + WARP_PREFER_SECONDS
        if not was_preferred:
            log(f"warp_preferred | reason={reason} | for={WARP_PREFER_SECONDS}s then direct is re-tested")


def route_order():
    """Ordered list of routes to try: True = WARP proxy, False = direct."""
    if not warp_proxy_url():
        return [False]
    return [True, False] if prefer_warp() else [False, True]


# --------------------------------------------------------------------------------------
# Dependencies / yt-dlp arguments
# --------------------------------------------------------------------------------------

def start_bgutil_pot_server():
    """Start or attach to local bgutil PO-token HTTP server on 127.0.0.1:4416."""
    global BGUTIL_PROC, POT_SERVER_ACTIVE
    curl_bin = shutil.which("curl")
    if curl_bin:
        try:
            chk = subprocess.run(
                [curl_bin, "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "2", "http://127.0.0.1:4416/ping"],
                capture_output=True, text=True, timeout=3,
            )
            if chk.returncode == 0 and chk.stdout.strip() == "200":
                POT_SERVER_ACTIVE = True
                return True
        except Exception:
            pass

    if not command_exists("node") or not command_exists("npm") or not command_exists("git"):
        return False

    bgutil_dir = "/tmp/bgutil-ytdlp-pot-provider"
    server_dir = os.path.join(bgutil_dir, "server")
    try:
        if not os.path.isdir(server_dir):
            subprocess.run(
                ["git", "clone", "--depth", "1", "https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git", bgutil_dir],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60,
            )
        main_js = os.path.join(server_dir, "build", "main.js")
        if not os.path.isfile(main_js):
            subprocess.run(["npm", "install", "--no-audit", "--no-fund"], cwd=server_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
            subprocess.run(["npx", "tsc"], cwd=server_dir, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if not os.path.isfile(main_js):
            return False

        BGUTIL_PROC = subprocess.Popen(
            ["node", main_js, "--port", "4416"],
            cwd=server_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
        )
        for _ in range(10):
            time.sleep(1)
            if curl_bin:
                chk = subprocess.run(
                    [curl_bin, "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "2", "http://127.0.0.1:4416/ping"],
                    capture_output=True, text=True, timeout=3,
                )
                if chk.returncode == 0 and chk.stdout.strip() == "200":
                    POT_SERVER_ACTIVE = True
                    return True
    except Exception as exc:
        log_raw("bgutil_server_warning", exc)
    return False


def detect_impersonation():
    """Enable --impersonate chrome only if yt-dlp reports a usable (non-'unavailable') Chrome target."""
    global IMPERSONATE_OK
    IMPERSONATE_OK = False
    try:
        result = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--list-impersonate-targets"],
            text=True, capture_output=True, timeout=30,
        )
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        for line in output.splitlines():
            low = line.strip().lower()
            if low.startswith("chrome") and "unavailable" not in low:
                IMPERSONATE_OK = True
                break
    except Exception as exc:
        log_raw("impersonation_probe_error", exc)
    return IMPERSONATE_OK


def impersonate_args(client):
    if IMPERSONATE_OK and client in IMPERSONATE_CLIENTS:
        return ["--impersonate", "chrome"]
    return []


def build_extractor_args(client, skip_webpage=False):
    args = []
    yt_parts = []
    if client:
        yt_parts.append(f"player_client={client}")
        if skip_webpage and client in SKIP_WEBPAGE_CLIENTS:
            yt_parts.append("player_skip=webpage,configs")
    if POT_SERVER_ACTIVE:
        yt_parts.append("fetch_pot=always")
    if yt_parts:
        args += ["--extractor-args", "youtube:" + ";".join(yt_parts)]
    if POT_SERVER_ACTIVE:
        args += ["--extractor-args", "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416"]
    return args


def install_dependencies():
    log(f"installing dependencies for {CHANNEL_HANDLE} (cookies={USE_COOKIES})")
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-U", "yt-dlp[default,curl-cffi]", "bgutil-ytdlp-pot-provider"],
            check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as exc:
        log_raw("yt_dlp_install_error", exc)

    required = []
    if not command_exists("curl"):
        required.append("curl")
    if not command_exists("ffmpeg"):
        required.append("ffmpeg")
    if not command_exists("git"):
        required.append("git")
    if not command_exists("node") and not command_exists("nodejs"):
        required.extend(["nodejs", "npm"])

    if required:
        log(f"installing_os_packages | {','.join(required)}")
        sudo_prefix = ["sudo"] if command_exists("sudo") else []
        try:
            subprocess.run(sudo_prefix + ["apt-get", "update", "-qq"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(sudo_prefix + ["apt-get", "install", "-y", "-qq"] + required, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as exc:
            log_raw("os_dependency_error", exc)

    pot_ok = start_bgutil_pot_server()
    imp_ok = detect_impersonation()
    version = ""
    try:
        result = subprocess.run([sys.executable, "-m", "yt_dlp", "--version"], text=True, capture_output=True, timeout=15)
        version = result.stdout.strip()
    except Exception:
        pass
    log(
        f"yt_dlp_version | {version or 'unknown'} | pot_server={'ACTIVE' if pot_ok else 'INACTIVE'}"
        f" | impersonate_chrome={'ON' if imp_ok else 'OFF'} | warp={'ON' if warp_proxy_url() else 'OFF'}"
    )


def js_runtime_args():
    args = ["--remote-components", "ejs:github"]
    if command_exists("node"):
        args += ["--js-runtimes", "node"]
    elif command_exists("nodejs"):
        args += ["--js-runtimes", "nodejs"]
    return args


JS_ARGS = []


# --------------------------------------------------------------------------------------
# HTML fetching / classification
# --------------------------------------------------------------------------------------

def _run_curl_fetch(curl_bin, url, use_proxy=False):
    cmd = [
        curl_bin, "-s", "-L", "--compressed", "--max-time", str(HTML_TIMEOUT_SECONDS),
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "-H", "Accept-Language: en-US,en;q=0.9",
        "-H", "Cookie: CONSENT=YES+cb.20210328-17-p0.en+FX+999; SOCS=CAISHAgBEhJnd3NfMjAyMzA4MTVfMF9SQzIaAmVuIAEaBgiAo_CmBg",
    ]
    if use_proxy and warp_proxy_url():
        cmd += ["--socks5-hostname", "127.0.0.1:40000"]
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=HTML_TIMEOUT_SECONDS + 2)
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception:
        pass
    return ""


def fetch_text(url):
    """Fetch a page, trying routes in route_order(); return the first clean page, else the best effort."""
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return ""
    best = ""
    for use_proxy in route_order():
        html = _run_curl_fetch(curl_bin, url, use_proxy=use_proxy)
        if not html:
            continue
        state, _ = inspect_youtube_html(html)
        if state in ("LIVE", "UPCOMING", "NOT_LIVE"):
            return html
        best = best or html
    return best


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


def cache_non_live_vod(video_id):
    if not video_id:
        return
    if len(NON_LIVE_VOD_CACHE) >= 500:
        NON_LIVE_VOD_CACHE.clear()
    NON_LIVE_VOD_CACHE.add(video_id)


def extract_json_object_after(html, marker, max_scan=250000):
    """Extract a balanced JSON object immediately following `marker`."""
    if not html:
        return None
    key = marker.rstrip(":")
    m = re.search(re.escape(key) + r"\s*:", html)
    if not m:
        return None
    brace_start = html.find("{", m.end())
    if brace_start == -1:
        return None

    depth = 0
    in_str = False
    escape = False
    limit = min(len(html), brace_start + max_scan)

    for i in range(brace_start, limit):
        ch = html[i]
        if escape:
            escape = False
            continue
        if ch == "\\" and in_str:
            escape = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if not in_str:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        obj = json.loads(html[brace_start : i + 1])
                        return obj if isinstance(obj, dict) else None
                    except Exception:
                        return None
    return None


def parse_strict_video_details(html):
    """
    Balanced-brace JSON parser for `videoDetails` and `playerMicroformatRenderer.liveBroadcastDetails`.
    State precedence: explicit isLiveNow=True > explicit isUpcoming=True > explicit isLiveNow=False > endTimestamp (when not isLive).
    """
    details = extract_json_object_after(html, '"videoDetails":')
    if not details or not details.get("videoId"):
        return None

    micro = extract_json_object_after(html, '"playerMicroformatRenderer":') or {}
    broadcast = micro.get("liveBroadcastDetails")
    if not isinstance(broadcast, dict):
        broadcast = extract_json_object_after(html, '"liveBroadcastDetails":') or {}

    video_id = str(details.get("videoId") or "")
    channel_id = str(details.get("channelId") or "") or None
    is_upcoming = bool(details.get("isUpcoming"))
    is_live = bool(details.get("isLive"))
    ended = bool(("isLive" in details) and not is_live and not is_upcoming)
    is_live_now = broadcast.get("isLiveNow")

    if is_live_now is True:
        is_live, is_upcoming, ended = True, False, False
    elif is_upcoming:
        is_live, ended = False, False
    elif is_live_now is False:
        is_live, ended = False, True
    elif broadcast.get("endTimestamp") and not is_live and not is_upcoming:
        ended = True

    return {
        "videoId": video_id,
        "channelId": channel_id,
        "isLive": is_live,
        "isUpcoming": is_upcoming,
        "ended": ended,
    }


def inspect_youtube_html(html):
    """
    Classify fetched YouTube HTML into 5 distinct states across the FULL HTML string:
      - ("LIVE", details_dict)
      - ("UPCOMING", details_dict)
      - ("NOT_LIVE", details_dict_or_None)
      - ("BLOCKED", None)
      - ("ERROR", None)
    """
    if not html or not html.strip():
        return "ERROR", None

    lowered = normalize_message(html)
    if any(
        marker in lowered
        for marker in (
            "sign in to confirm you're not a bot",
            "confirm you're not a bot",
            "our systems have detected unusual traffic",
            "unusual traffic from your computer network",
            "rate-limited by youtube",
            "g-recaptcha",
            'class="g-recaptcha"',
        )
    ):
        return "BLOCKED", None

    details = parse_strict_video_details(html)
    if details:
        if details.get("isLive") and not details.get("ended") and not details.get("isUpcoming"):
            return "LIVE", details
        if details.get("isUpcoming") and not details.get("ended"):
            return "UPCOMING", details
        return "NOT_LIVE", details

    if any(
        marker in lowered
        for marker in (
            "this channel is not currently live",
            "this channel has no live streams",
            "this live event has ended",
            "live stream recording is not available",
        )
    ):
        return "NOT_LIVE", None

    if "ytinitialdata" in lowered or "ytinitialplayerresponse" in lowered or "<yt:videoid>" in lowered:
        target_cid = TARGET.get("channel_id")
        if target_cid and target_cid.lower() not in lowered and "ytinitialdata" in lowered:
            other_cid = re.search(r'"(?:externalId|channelId|browseId)"\s*:\s*"(UC[A-Za-z0-9_-]{22})"', html)
            if other_cid and other_cid.group(1) != target_cid:
                return "ERROR", None
        return "NOT_LIVE", None

    return "ERROR", None


# --------------------------------------------------------------------------------------
# yt-dlp JSON probe
# --------------------------------------------------------------------------------------

def _exec_ytdlp_json(url, client, use_proxy=False):
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "-J",
        "--skip-download", "--no-warnings", "--ignore-no-formats-error",
        "--force-ipv4", "--playlist-items", "1",
    ] + JS_ARGS + build_extractor_args(client, skip_webpage=False) + impersonate_args(client)
    if use_proxy:
        proxy = warp_proxy_url()
        if proxy:
            command += ["--proxy", proxy]
    if USE_COOKIES and COOKIE_FILE and os.path.isfile(COOKIE_FILE) and client not in COOKIE_INCOMPATIBLE_CLIENTS:
        command += ["--cookies", COOKIE_FILE]
    command.append(url)
    return subprocess.run(command, text=True, encoding="utf-8", errors="replace", capture_output=True, timeout=PROBE_TIMEOUT_SECONDS)


def run_ytdlp_json(url, client):
    global YTDLP_PROBE_COOLDOWN_UNTIL
    routes = route_order()
    for idx, use_proxy in enumerate(routes):
        try:
            result = _exec_ytdlp_json(url, client, use_proxy=use_proxy)
            if result.returncode == 0 and result.stdout.strip():
                raw = json.loads(result.stdout)
                if raw.get("_type") == "playlist" and raw.get("entries"):
                    for item in raw["entries"]:
                        if item and is_live_candidate(item):
                            return item
                    return raw["entries"][0]
                return raw
            stderr_low = normalize_message(result.stderr or "")
            if any(m in stderr_low for m in BOTCHECK_MARKERS + RATE_LIMIT_MARKERS):
                if not use_proxy:
                    mark_ip_blocked("ytdlp_probe")
                if idx < len(routes) - 1:
                    continue
                YTDLP_PROBE_COOLDOWN_UNTIL = time.time() + 180
                PROBE_STATS["blocked"] += 1
                write_summary(blocked_by_youtube=True, probe_stats=PROBE_STATS)
                log(f"ytdlp_probe_blocked | client={client_label(client)} | cooling down yt-dlp probes for 180s")
        except Exception:
            pass
    return None


# --------------------------------------------------------------------------------------
# Live detection
# --------------------------------------------------------------------------------------

def extract_live_badge_ids(html):
    if not html:
        return []
    scores = {}
    state, strict_details = inspect_youtube_html(html)
    if state == "LIVE" and strict_details and strict_details.get("videoId"):
        scores[strict_details["videoId"]] = 40

    patterns = [r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', r'watch\?v=([A-Za-z0-9_-]{11})', r'shorts/([A-Za-z0-9_-]{11})']
    for pattern in patterns:
        for match in re.finditer(pattern, html, flags=re.IGNORECASE):
            video_id = match.group(1)
            if video_id in NON_LIVE_VOD_CACHE:
                continue
            context = html[max(0, match.start() - 1500):min(len(html), match.end() + 1500)].lower()
            score = scores.get(video_id, 0)
            if any(k in context for k in ("islivenow", "is_live_now", "badge_style_type_live_now", '"style":"live"', '"style": "live"')):
                score += 10
            if re.search(r'"label"\s*:\s*"live"', context) or re.search(r'"text"\s*:\s*"live"', context):
                score += 5
            if "watching" in context:
                score += 2
            scores[video_id] = score

    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [video_id for video_id, score in ranked if score >= 5 and video_id not in NON_LIVE_VOD_CACHE]


def extract_rss_video_ids(xml):
    if not xml:
        return []
    return [vid for vid in re.findall(r"<yt:videoId>([A-Za-z0-9_-]{11})</yt:videoId>", xml)[:3] if vid not in NON_LIVE_VOD_CACHE]


def validate_candidate(video_id, client):
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    info = run_ytdlp_json(watch_url, client)
    if not info:
        return None
    live_status = str(info.get("live_status") or "").casefold()
    if live_status == "is_upcoming" or info.get("is_upcoming"):
        return None
    if not is_live_candidate(info):
        cache_non_live_vod(video_id)
        return None
    matched, reason = target_match(info)
    if not matched:
        cache_non_live_vod(video_id)
        return None
    return {"id": video_id, "url": watch_url, "client": client, "match_reason": reason}


def verify_live_id(video_id, source_kind):
    video_id = str(video_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) or video_id in NON_LIVE_VOD_CACHE:
        return None

    started = time.monotonic()
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    html = fetch_text(watch_url)
    state, details = inspect_youtube_html(html)

    if state == "LIVE" and details and details.get("videoId") == video_id:
        target_cid = TARGET.get("channel_id")
        if not target_cid or details.get("channelId") == target_cid:
            elapsed = time.monotonic() - started
            PROBE_STATS["live_hits"] += 1
            write_summary(probe_stats=PROBE_STATS)
            log(f"detect_live | source={source_kind} | live id={video_id} | client=html_strict | match=strict_videoDetails | validation={elapsed:.1f}s")
            return {"id": video_id, "url": watch_url, "source": source_kind, "client": "android_vr"}
        cache_non_live_vod(video_id)
        return None

    if state == "UPCOMING":
        return None

    if state == "NOT_LIVE":
        cache_non_live_vod(video_id)
        return None

    if state == "BLOCKED":
        PROBE_STATS["blocked"] += 1
        write_summary(blocked_by_youtube=True, probe_stats=PROBE_STATS)
    else:
        PROBE_STATS["error"] += 1
        write_summary(probe_stats=PROBE_STATS)

    if time.time() < YTDLP_PROBE_COOLDOWN_UNTIL:
        return None

    for client in VALIDATION_CLIENTS[:2]:
        candidate = validate_candidate(video_id, client)
        if candidate:
            elapsed = time.monotonic() - started
            PROBE_STATS["live_hits"] += 1
            write_summary(probe_stats=PROBE_STATS)
            log(f"detect_live | source={source_kind} | live id={candidate['id']} | client={client_label(candidate['client'])} | match={candidate['match_reason']} | validation={elapsed:.1f}s")
            return {"id": candidate["id"], "url": candidate["url"], "source": source_kind, "client": candidate["client"]}
        if video_id in NON_LIVE_VOD_CACHE or time.time() < YTDLP_PROBE_COOLDOWN_UNTIL:
            break
    return None


def probe_fast():
    """
    Zero-yt-dlp idle probe, run every ~20s. Fetches in parallel:
      - /live      (normal lives; redirects to the watch page when live)
      - channel home
      - /streams   (vertical / Shorts-style lives often show only a LIVE badge here)
    BLOCKED takes priority over NOT_LIVE so a blocked page is never masked by a clean one.
    """
    global LAST_BLOCKED_LOG_AT, LAST_HEARTBEAT_LOG_AT, YTDLP_PROBE_COOLDOWN_UNTIL
    executor = ThreadPoolExecutor(max_workers=3)
    futures = {
        executor.submit(fetch_text, LIVE_URL): "live",
        executor.submit(fetch_text, CHANNEL_URL): "home",
        executor.submit(fetch_text, STREAMS_URL): "streams",
    }
    pages = {"live": "", "home": "", "streams": ""}

    try:
        for future in as_completed(futures, timeout=HTML_TIMEOUT_SECONDS + 4):
            kind = futures[future]
            try:
                pages[kind] = future.result() or ""
            except Exception:
                pages[kind] = ""
    except Exception:
        pass
    finally:
        executor.shutdown(wait=False)

    live_state, live_details = inspect_youtube_html(pages["live"])
    home_state, _ = inspect_youtube_html(pages["home"])
    streams_state, _ = inspect_youtube_html(pages["streams"])

    if live_state == "LIVE" and live_details and live_details.get("videoId"):
        target_cid = TARGET.get("channel_id")
        if not target_cid or live_details.get("channelId") == target_cid:
            vid = live_details["videoId"]
            PROBE_STATS["live_hits"] += 1
            write_summary(probe_stats=PROBE_STATS)
            log(f"detect_live | source=direct_live_html | live id={vid} | client=html_strict | match=strict_videoDetails")
            return {"id": vid, "url": f"https://www.youtube.com/watch?v={vid}", "source": "direct_live_html", "client": "android_vr"}

    seen = set()
    for page_key in ("live", "home", "streams"):
        for video_id in extract_live_badge_ids(pages[page_key])[:3]:
            if video_id in seen:
                continue
            seen.add(video_id)
            result = verify_live_id(video_id, f"{page_key}_live_badge")
            if result:
                return result

    now = time.time()
    clean = ("NOT_LIVE", "UPCOMING")
    if "BLOCKED" in (live_state, home_state, streams_state):
        PROBE_STATS["blocked"] += 1
        write_summary(blocked_by_youtube=True, probe_stats=PROBE_STATS)
        mark_ip_blocked("html_probe")
        if now - LAST_BLOCKED_LOG_AT >= 120:
            LAST_BLOCKED_LOG_AT = now
            log(f"probe_blocked_warning | YouTube returned botcheck/captcha on HTML probe (blocked={PROBE_STATS['blocked']}, clean={PROBE_STATS['clean_offline']})")
    elif live_state in clean and home_state in clean:
        PROBE_STATS["clean_offline"] += 1
        write_summary(probe_stats=PROBE_STATS)
        if now - LAST_HEARTBEAT_LOG_AT >= 1800 and not SESSION_ACTIVE.is_set():
            LAST_HEARTBEAT_LOG_AT = now
            log(f"detector_heartbeat | status=offline_clean | clean_offline={PROBE_STATS['clean_offline']} | blocked={PROBE_STATS['blocked']} | error={PROBE_STATS['error']}")
        return None
    else:
        PROBE_STATS["error"] += 1
        write_summary(probe_stats=PROBE_STATS)

    if now >= YTDLP_PROBE_COOLDOWN_UNTIL:
        YTDLP_PROBE_COOLDOWN_UNTIL = now + 180
        info = run_ytdlp_json(LIVE_URL, "android_vr")
        if info and is_live_candidate(info):
            matched, reason = target_match(info)
            if matched:
                video_id = info["id"]
                PROBE_STATS["live_hits"] += 1
                write_summary(probe_stats=PROBE_STATS)
                log(f"detect_live | source=live_endpoint_fallback | live id={video_id} | client=android_vr | match={reason}")
                return {"id": video_id, "url": f"https://www.youtube.com/watch?v={video_id}", "source": "live_endpoint", "client": "android_vr"}

    return None


def probe_secondary():
    channel_id = TARGET.get("channel_id")
    executor = ThreadPoolExecutor(max_workers=3)
    futures = {
        executor.submit(fetch_text, STREAMS_URL): "streams",
        executor.submit(fetch_text, SHORTS_URL): "shorts",
    }
    if channel_id:
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        futures[executor.submit(fetch_text, rss_url)] = "rss"
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


def check_live_state(video_id):
    if not video_id:
        return "ENDED"
    target_cid = TARGET.get("channel_id")
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    html = fetch_text(watch_url)
    state, details = inspect_youtube_html(html)
    if details and details.get("videoId") == video_id:
        if target_cid and details.get("channelId") and details.get("channelId") != target_cid:
            return "UNKNOWN"
        if state == "LIVE":
            return "LIVE"
        if state == "NOT_LIVE" and details.get("ended") is True:
            return "ENDED"
    if time.time() >= YTDLP_PROBE_COOLDOWN_UNTIL:
        info = run_ytdlp_json(watch_url, "android_vr")
        if info and info.get("id") == video_id:
            matched, _ = target_match(info)
            if not matched:
                return "UNKNOWN"
            if is_live_candidate(info):
                return "LIVE"
            live_status = str(info.get("live_status") or "").casefold()
            if live_status in {"was_live", "not_live", "post_live"} or info.get("is_live") is False:
                return "ENDED"
    return "UNKNOWN"


def confirm_still_live(video_id):
    state = check_live_state(video_id)
    if state == "LIVE":
        return True
    if state == "ENDED":
        return False
    for probe_fn in (probe_fast, probe_secondary):
        try:
            res = probe_fn()
            if res and res.get("id") == video_id:
                return True
        except Exception:
            pass
    return state == "UNKNOWN"


# --------------------------------------------------------------------------------------
# Continuation shift dispatch
# --------------------------------------------------------------------------------------

def dispatch_tokens():
    """GH_TOKEN first (may be a PAT), then GH_FALLBACK_TOKEN (the built-in GITHUB_TOKEN). De-duplicated."""
    tokens = []
    for name in ("GH_TOKEN", "GH_FALLBACK_TOKEN"):
        value = os.getenv(name, "").strip()
        if value and value not in tokens:
            tokens.append(value)
    return tokens


def maybe_prewarm_continuation_shift():
    global CONTINUATION_DISPATCHED, PREWARM_ATTEMPTS, NEXT_PREWARM_AT
    now = time.time()
    if (
        CONTINUATION_DISPATCHED
        or SHIFTS_LEFT <= 0
        or HOURS < 0.1
        or PREWARM_ATTEMPTS >= PREWARM_MAX_ATTEMPTS
        or now < NEXT_PREWARM_AT
    ):
        return

    gh_bin = shutil.which("gh")
    tokens = dispatch_tokens()
    if not gh_bin or not tokens:
        PREWARM_ATTEMPTS = PREWARM_MAX_ATTEMPTS
        write_summary(continuation_dispatched=False)
        log("prewarm_skipped | gh or token unavailable; deferring to workflow post-run step")
        return

    PREWARM_ATTEMPTS += 1
    NEXT_PREWARM_AT = now + PREWARM_BACKOFF_SECONDS

    ref_name = os.getenv("GITHUB_REF_NAME", "main")
    target_data = os.getenv("ACTIVE_TARGET_DATA") or f"{CHANNEL_HANDLE}|{TARGET_CHANNEL_ID}"
    next_shifts = str(max(0, SHIFTS_LEFT - 1))
    cmd = [
        gh_bin, "workflow", "run", "record.yml",
        "--ref", ref_name,
        "-f", f"hours={HOURS}",
        "-f", f"shifts_left={next_shifts}",
        "-f", f"custom_target={target_data}",
    ]
    if os.getenv("ALWAYS_CONTINUE") is not None:
        cmd += ["-f", f"always_continue={'true' if ALWAYS_CONTINUE else 'false'}"]
    if os.getenv("INPUT_USE_COOKIES"):
        cmd += ["-f", f"use_cookies={os.getenv('INPUT_USE_COOKIES').strip().lower()}"]

    last_error = ""
    for position, token in enumerate(tokens, start=1):
        env = dict(os.environ, GH_TOKEN=token)
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=20, env=env)
        except Exception as exc:
            last_error = f"token#{position} exception: {exc}"
            continue
        if res.returncode == 0:
            CONTINUATION_DISPATCHED = True
            log(f"prewarm_continuation_dispatched | attempt={PREWARM_ATTEMPTS} | token#{position} | shifts_left={next_shifts} | target={target_data}")
            write_summary(continuation_dispatched=True)
            return
        last_error = f"token#{position} rc={res.returncode} {res.stderr}"
        log_raw("prewarm_dispatch_token_failed", last_error)

    write_summary(continuation_dispatched=False)
    log_raw("prewarm_dispatch_failed", f"attempt={PREWARM_ATTEMPTS}/{PREWARM_MAX_ATTEMPTS} | {last_error}")


# --------------------------------------------------------------------------------------
# Process control
# --------------------------------------------------------------------------------------

def stop_proc(process):
    if process is None:
        return
    if os.name != "posix":
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        return

    pgid = process.pid
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError as exc:
        log_raw("process_group_sigterm_error", exc)

    try:
        process.wait(timeout=8)
    except Exception:
        pass

    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            return
        except OSError:
            break

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError as exc:
        log_raw("process_group_sigkill_error", exc)

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
            time.sleep(0.05)
        except ProcessLookupError:
            return
        except OSError:
            return


def sigterm_handler(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log("termination_requested | stopping download; saved files will be recovered")
    with CURRENT_PROC_LOCK:
        process = CURRENT_PROC
    stop_proc(process)


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)
atexit.register(lambda: (stop_proc(CURRENT_PROC), stop_proc(BGUTIL_PROC)))


# --------------------------------------------------------------------------------------
# Session / chunk bookkeeping
# --------------------------------------------------------------------------------------

def build_final_path(live_id, session_index=1):
    suffix = "" if session_index == 1 else f"_r{session_index}"
    return os.path.join(OUT_DIR, f"{CHANNEL_SLUG}_{live_id}_{RUN_STAMP}{suffix}.ts")


def build_chunk_dir(live_id, session_index=1):
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
        active["preferred_client"] = None
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
    final_ts = build_final_path(live_id, session_index)
    chunk_dir = build_chunk_dir(live_id, session_index)
    os.makedirs(chunk_dir, exist_ok=True)

    if os.path.isfile(final_ts) and os.path.getsize(final_ts) >= MIN_SAVE_BYTES:
        existing_chunks = glob.glob(os.path.join(chunk_dir, "*.ts"))
        next_idx = max([chunk_number(p) for p in existing_chunks] + [0]) + 1
        restored_chunk = os.path.join(chunk_dir, f"{CHANNEL_SLUG}_{live_id}_r{session_index}_chunk{next_idx:04d}.ts")
        try:
            os.replace(final_ts, restored_chunk)
            log(f"session_reused | moved prior {os.path.basename(final_ts)} -> {os.path.basename(restored_chunk)} to prevent _r2 split")
        except Exception as exc:
            log_raw("session_reuse_error", exc)

    active = {
        "live_id": live_id,
        "live_url": f"https://www.youtube.com/watch?v={live_id}",
        "preferred_client": None,
        "client_locked": False,
        "short_exit_streak": 0,
        "short_exit_seq": 0,
        "session_index": session_index,
        "chunk_index": 1,
        "final_ts": final_ts,
        "chunk_dir": chunk_dir,
        "chunks": [],
        "recording_path": None,
        "offline_since": None,
        "cooldown_until": 0,
        "last_failure_reason": None,
        "recovery_needed": False,
    }
    register_recovered_chunks(active)
    save_active_state(active)
    SESSION_ACTIVE.set()
    write_summary(video_id=live_id, detect_source=live.get("source"), status="session_open")
    log(f"opened_session | live id={live_id} | source={live.get('source')} | prior_chunks={len(active['chunks'])} | output={os.path.basename(active['final_ts'])}")
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
                try:
                    os.remove(path)
                except Exception:
                    pass
            try:
                os.remove(list_path)
            except Exception:
                pass
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

    still_live = False
    if reason == "session complete":
        still_live = CONTINUATION_DISPATCHED or confirm_still_live(active["live_id"])

    write_summary(
        video_id=active["live_id"],
        final_file=(os.path.basename(final_path) if final_path else None),
        final_size_mb=round(size_mb, 1),
        chunk_count=len(active["chunks"]),
        status="session_closed",
        close_reason=reason,
        still_live_at_end=still_live,
        continuation_dispatched=CONTINUATION_DISPATCHED,
        failure_reason=None,
    )
    save_active_state(None)


# --------------------------------------------------------------------------------------
# Downloading
# --------------------------------------------------------------------------------------

def output_reader(stream, output_state):
    for raw_line in iter(stream.readline, ""):
        if not raw_line:
            break
        print(raw_line, end="", flush=True)
        lowered = normalize_message(raw_line)
        if "chosen=" in raw_line:
            output_state["chosen_seen"] = True
        if "chosen=" in raw_line or "error:" in lowered or "warning:" in lowered:
            log_raw("yt_dlp", raw_line)
        output_state["botcheck"] |= any(marker in lowered for marker in BOTCHECK_MARKERS)
        output_state["no_formats"] |= any(marker in lowered for marker in NO_FORMAT_MARKERS)
        output_state["rate_limited"] |= any(marker in lowered for marker in RATE_LIMIT_MARKERS)
        output_state["transient"] |= any(marker in lowered for marker in TRANSIENT_MARKERS)
        output_state["access_restricted"] |= any(marker in lowered for marker in ACCESS_RESTRICTED_MARKERS)
        output_state["not_live"] |= any(marker in lowered for marker in NOT_LIVE_MARKERS)
        output_state["normal_exit"] |= any(marker in lowered for marker in NORMAL_END_MARKERS)


def classify_output(output_state):
    for key in ("botcheck", "rate_limited", "access_restricted", "transient", "no_formats", "not_live"):
        if output_state.get(key):
            return key
    return "normal_end" if output_state.get("normal_exit") else "no_file"


def make_download_cmd(active, chunk_path, client, skip_webpage=True, use_proxy=False):
    # Note: --downloader-args ffmpeg_i:... was removed in v8.8; it is ignored by the native
    # HLS downloader (-downloader m3u8:native) which does its own fragment retries.
    command = [
        sys.executable, "-m", "yt_dlp", "--no-config", "--newline", "--no-warnings",
        "--restrict-filenames", "--no-part", "--no-playlist", "--no-live-from-start", "--match-filters", "is_live",
        "--force-ipv4", "--fixup", "never",
        "--print", "before_dl:chosen=%(format_id)s | %(resolution)s | tbr=%(tbr)s | protocol=%(protocol)s", "--no-quiet",
        "--retries", "20", "--extractor-retries", "20", "--file-access-retries", "20",
        "--fragment-retries", "infinite", "--retry-sleep", "http:2", "--retry-sleep", "fragment:1",
        "--skip-unavailable-fragments", "--hls-use-mpegts", "--downloader", "m3u8:native", "-N", "1",
        "-f", FORMAT_SELECTOR, "-S", SORT_EXPR, "-o", chunk_path,
    ] + JS_ARGS

    command += impersonate_args(client)
    if use_proxy:
        proxy = warp_proxy_url()
        if proxy:
            command += ["--proxy", proxy]
    command += build_extractor_args(client, skip_webpage=skip_webpage)
    if USE_COOKIES and COOKIE_FILE and os.path.isfile(COOKIE_FILE) and client not in COOKIE_INCOMPATIBLE_CLIENTS:
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

def validate_finalized_recordings(directory_or_files, min_bytes=MIN_SAVE_BYTES):
    """
    Scans for finalized, non-empty media files.
    Strictly excludes:
    - Incomplete/unfinalized files (*.part, *.temp, *.tmp)
    - Short exit artifacts (*_short_exit_*)
    - Zero-byte or truncated files (< min_bytes)
    Returns list of tuples: [(path, size_bytes), ...]
    """
    valid = []
    if isinstance(directory_or_files, str):
        if os.path.isdir(directory_or_files):
            candidates = [os.path.join(directory_or_files, f) for f in os.listdir(directory_or_files)]
        elif os.path.isfile(directory_or_files):
            candidates = [directory_or_files]
        else:
            candidates = []
    elif isinstance(directory_or_files, (list, tuple, set)):
        candidates = list(directory_or_files)
    else:
        return []

    valid_exts = {".ts", ".mp4", ".mkv", ".flv"}
    for item in sorted(candidates, key=lambda x: str(x)):
        path = item[0] if isinstance(item, (tuple, list)) else item
        if not isinstance(path, str) or not os.path.isfile(path):
            continue
        base = os.path.basename(path)
        if base.endswith(".part") or base.endswith(".tmp") or base.endswith(".temp"):
            continue
        if "_short_exit_" in base:
            continue
        ext = os.path.splitext(base)[1].lower()
        if ext not in valid_exts:
            continue
        try:
            sz = os.path.getsize(path)
            if sz >= min_bytes:
                valid.append((path, sz))
        except OSError:
            continue
    return valid


def classify_run_status(summary, media_candidates=None):
    """
    Evaluates execution summary and media candidates into a strict status:
    - RECORD_PASS: At least one finalized, non-empty media file (>= min_bytes, not .part) exists.
    - BLOCKED_NO_CAPTURE: Confirmed rate limit or botcheck with 0 media captured.
    - RESTRICTED_UNVERIFIED: Probe-level block or generic restriction without confirmed hourly limit.
    - NO_LIVE_CONFIRMED: Clean offline probe > 0 with 0 blocks and 0 errors.
    - UNVERIFIED: No clean offline, no blocks, no errors (clean_offline=0, blocked=0, error=0).
    - UNVERIFIED_NETWORK_ERROR: Errors > 0 encountered during probes.
    """
    valid_media = validate_finalized_recordings(media_candidates or [])
    chunk_count = int(summary.get("chunk_count", 0) or 0)
    final_size_mb = float(summary.get("final_size_mb", 0.0) or 0.0)

    if valid_media and (chunk_count > 0 or final_size_mb > 0 or any(sz >= MIN_SAVE_BYTES for _, sz in valid_media)):
        return "RECORD_PASS"

    if summary.get("downloader_rate_limited"):
        return "BLOCKED_NO_CAPTURE"

    stats = summary.get("probe_stats") or {}
    c_off = int(stats.get("clean_offline", 0) or 0)
    c_blk = int(stats.get("blocked", 0) or 0)
    c_err = int(stats.get("error", 0) or 0)

    if summary.get("blocked_by_youtube") and summary.get("failure_reason") in {"rate_limited", "botcheck"}:
        return "BLOCKED_NO_CAPTURE"
    if summary.get("blocked_by_youtube") or c_blk > 0:
        return "RESTRICTED_UNVERIFIED"
    if c_off > 0 and c_blk == 0 and c_err == 0:
        return "NO_LIVE_CONFIRMED"
    if c_off == 0 and c_blk == 0 and c_err == 0:
        return "UNVERIFIED"
    if c_err > 0:
        return "UNVERIFIED_NETWORK_ERROR"
    return "UNVERIFIED"



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
    active["botcheck_streak"] = 0
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


def run_download_attempt(active, session_end, client, live_queue, skip_webpage=True, use_proxy=False):
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

    command = make_download_cmd(active, chunk_path, client, skip_webpage=skip_webpage, use_proxy=use_proxy)
    left = max(0, session_end - time.time())
    route = "warp_proxy" if (use_proxy and warp_proxy_url()) else "direct"
    log(f"[{hms(left)} left] download_started | chunk={index:04d} | client={client_label(client)} | route={route} | skip_webpage={skip_webpage}")

    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1, start_new_session=True)
    except Exception as exc:
        log_raw("download_launch_error", exc)
        active["recording_path"] = None
        save_active_state(active)
        return False, "launch_error", None

    with CURRENT_PROC_LOCK:
        CURRENT_PROC = process
    DOWNLOAD_ACTIVE.set()

    output_state = {
        "botcheck": False, "no_formats": False, "rate_limited": False, "transient": False,
        "access_restricted": False, "not_live": False, "normal_exit": False, "chosen_seen": False,
    }
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
                active["offline_since"] = None

            if (session_end - now) <= PREWARM_LEAD_SECONDS and (
                ALWAYS_CONTINUE
                or (last_size >= MIN_SAVE_BYTES and (now - last_growth_at) <= PREWARM_ACTIVE_GROWTH_SEC)
            ):
                maybe_prewarm_continuation_shift()

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
    if process.returncode == 0 and saved_bytes == 0 and not output_state.get("chosen_seen") and stop_reason is None and not STOP_REQUESTED:
        output_state["not_live"] = True
    explicit_end = output_state["not_live"] or output_state["normal_exit"]
    explicit_error = (
        output_state["botcheck"] or output_state["no_formats"] or output_state["rate_limited"]
        or output_state["transient"] or output_state["access_restricted"]
    )

    unexpected_short_exit = (
        stop_reason is None
        and duration < SHORT_EXIT_PROTECTION_SECONDS
        and not explicit_end
        and not explicit_error
        and not STOP_REQUESTED
        and time.time() < session_end
    )

    if unexpected_short_exit:
        if saved_path and os.path.isfile(saved_path) and saved_bytes >= MIN_SAVE_BYTES:
            append_chunk(active, saved_path, client, "short_chunk_preserved")
            return True, "short_chunk_preserved", replacement_live
        elif saved_path and os.path.isfile(saved_path):
            archived = archive_short_exit(active, saved_path)
            active["short_exit_streak"] = int(active.get("short_exit_streak", 0)) + 1
            active["recording_path"] = None
            save_active_state(active)
            log(f"short_exit_detected | client={client_label(client)} | duration={duration:.1f}s | size={saved_bytes / 1048576:.1f}MB | archived={os.path.basename(archived) if archived else 'none'}")
            return False, "short_exit", replacement_live
        else:
            active["short_exit_streak"] = int(active.get("short_exit_streak", 0)) + 1
            active["recording_path"] = None
            save_active_state(active)
            log(f"short_exit_detected | client={client_label(client)} | duration={duration:.1f}s | size=0.0MB | archived=none")
            return False, "short_exit", replacement_live

    if saved_path and os.path.isfile(saved_path) and saved_bytes >= MIN_SAVE_BYTES:
        if STOP_REQUESTED:
            reason = "termination"
        elif time.time() >= session_end:
            reason = "session_end"
        elif stop_reason == "new_live_id":
            reason = "new_live_id"
        elif explicit_end:
            reason = classify_output(output_state)
        elif stop_reason == "stalled":
            reason = "stalled"
        else:
            reason = "downloader_exit"

        append_chunk(active, saved_path, client, reason)
        if replacement_live:
            return True, "new_live_id", replacement_live
        return True, reason, None

    if saved_path and os.path.isfile(saved_path):
        try:
            os.remove(saved_path)
        except Exception:
            pass

    active["recording_path"] = None
    reason = classify_output(output_state)
    if stop_reason and reason == "no_file":
        reason = stop_reason
    if reason in {"rate_limited", "botcheck"}:
        write_summary(blocked_by_youtube=True, downloader_rate_limited=True, failure_reason=reason)
    elif reason == "access_restricted":
        write_summary(blocked_by_youtube=False, downloader_rate_limited=False, failure_reason="restricted_unverified")
    log(f"download_failed | client={client_label(client)} | reason={reason} | duration={duration:.1f}s")
    return False, reason, replacement_live


def can_continue(session_end):
    return (not STOP_REQUESTED) and time.time() < session_end


def attempt_with_fallbacks(active, session_end, client, live_queue):
    """
    One logical download attempt for `client`, walking route_order():
      route -> skip_webpage=True -> (if no_formats on an app client) skip_webpage=False on SAME route
      -> if the failure is an IP-level block, mark it and try the next route (direct <-> WARP).
    Any other failure reason ends the walk and is returned to the caller.
    """
    routes = route_order()
    ok, reason, replacement_live = False, "download_failed", None
    for idx, use_proxy in enumerate(routes):
        ok, reason, replacement_live = run_download_attempt(active, session_end, client, live_queue, skip_webpage=True, use_proxy=use_proxy)
        if not ok and reason == "no_formats" and client in SKIP_WEBPAGE_CLIENTS and can_continue(session_end):
            ok, reason, replacement_live = run_download_attempt(active, session_end, client, live_queue, skip_webpage=False, use_proxy=use_proxy)
        if ok or replacement_live or not can_continue(session_end):
            break
        if reason not in IP_BLOCK_REASONS:
            break
        if not use_proxy:
            mark_ip_blocked(reason)
        if idx < len(routes) - 1:
            log(f"route_fallback | client={client_label(client)} | reason={reason} | from={'warp' if use_proxy else 'direct'} | to={'direct' if use_proxy else 'warp'}")
    return ok, reason, replacement_live


def record_until_stop(active, session_end, live_queue):
    if active.get("client_locked") and active.get("preferred_client") in DOWNLOAD_CLIENTS:
        preferred = active["preferred_client"]
        clients = [preferred] + [c for c in DOWNLOAD_CLIENTS if c != preferred]
    else:
        clients = list(DOWNLOAD_CLIENTS)

    last_reason = "download_failed"
    transient_clients = set()
    for client in clients:
        short_attempts = 0
        while can_continue(session_end):
            ok, reason, replacement_live = attempt_with_fallbacks(active, session_end, client, live_queue)
            if replacement_live:
                return False, "new_live_id", replacement_live
            if STOP_REQUESTED:
                return False, "termination", None
            if time.time() >= session_end:
                return False, "session_end", None
            if reason in {"not_live", "normal_end"}:
                return False, reason, None
            last_reason = reason

            if reason == "transient":
                transient_clients.add(client_label(client))
                if len(transient_clients) >= 2:
                    mark_ip_blocked("transient_on_multiple_clients")

            if reason in CLIENT_SWITCH_REASONS:
                log(f"client_blocked | client={client_label(client)} | reason={reason} | switching to next client")
                break

            if reason == "short_exit":
                short_attempts += 1
                if short_attempts <= SHORT_EXIT_RETRIES:
                    log(f"short_exit_retry | client={client_label(client)} | retry={short_attempts}/{SHORT_EXIT_RETRIES} | waiting={SHORT_EXIT_BACKOFF_SECONDS}s")
                    time.sleep(SHORT_EXIT_BACKOFF_SECONDS)
                    continue
                log(f"short_exit_limit_reached | client={client_label(client)} | moving to next client")
                break

            if ok and reason in {"stalled", "downloader_exit", "short_chunk_preserved"}:
                short_attempts = 0
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


# --------------------------------------------------------------------------------------
# Detector threads / scheduling
# --------------------------------------------------------------------------------------

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


FAILURE_REASONS = {
    "not_live", "normal_end", "download_failed", "launch_error",
    "botcheck", "rate_limited", "access_restricted", "transient", "no_formats", "no_file", "short_exit",
}


if __name__ == "__main__":
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

    write_summary(status="monitoring", continuation_dispatched=False)
    log(f"started | channel={CHANNEL_HANDLE} | id={TARGET_CHANNEL_ID} | duration={HOURS}h | shifts_left={SHIFTS_LEFT} | always_continue={ALWAYS_CONTINUE} | cookies={USE_COOKIES} | stall={STALL_SECONDS}s")

    try:
        while time.time() < session_end and not STOP_REQUESTED:
            now = time.time()
            if ALWAYS_CONTINUE and (session_end - now) <= PREWARM_LEAD_SECONDS:
                maybe_prewarm_continuation_shift()
            candidate = drain_latest_candidate(live_queue)

            if candidate:
                if active is None:
                    active = new_session(candidate)
                elif candidate["id"] != active["live_id"]:
                    close_session(active, "new target live ID detected")
                    active = new_session(candidate)
                else:
                    active["offline_since"] = None
                    if not DOWNLOAD_ACTIVE.is_set():
                        active["recovery_needed"] = True
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

                    if reason in FAILURE_REASONS:
                        active["recovery_needed"] = True
                        active["last_failure_reason"] = reason
                        if reason in {"botcheck", "rate_limited"}:
                            streak = int(active.get("botcheck_streak", 0)) + 1
                            active["botcheck_streak"] = streak
                            wait_s = min(105, BOTCHECK_BASE_COOLDOWN_SECONDS + (streak - 1) * 30)
                            write_summary(blocked_by_youtube=True, failure_reason=reason)
                        else:
                            wait_s = FAILURE_COOLDOWN_SECONDS
                        active["cooldown_until"] = time.time() + wait_s
                        live_state = check_live_state(active["live_id"])
                        if live_state == "LIVE":
                            active["offline_since"] = None
                        elif live_state == "ENDED" and reason in {"not_live", "normal_end"}:
                            close_session(active, f"stream ended ({reason})")
                            active = None
                            continue
                        elif active["offline_since"] is None:
                            active["offline_since"] = time.time()
                        save_active_state(active)
                        log(f"recovery_cooldown | reason={reason} | live_state={live_state} | waiting={wait_s}s")

                if (
                    active
                    and active.get("offline_since") is not None
                    and (time.time() - active["offline_since"]) >= OFFLINE_GRACE_SECONDS
                    and not DOWNLOAD_ACTIVE.is_set()
                ):
                    live_state = check_live_state(active["live_id"])
                    if live_state == "LIVE":
                        log(f"offline_grace_cancelled | live id={active['live_id']} confirmed still live")
                        active["offline_since"] = None
                        active["recovery_needed"] = True
                        save_active_state(active)
                    elif live_state == "UNKNOWN":
                        log(f"offline_grace_deferred | live id={active['live_id']} verification blocked/unknown; keeping session open")
                        active["offline_since"] = time.time()
                        active["recovery_needed"] = True
                        save_active_state(active)
                    else:
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
    elif SUMMARY.get("status") == "session_closed":
        write_summary(continuation_dispatched=CONTINUATION_DISPATCHED)
    else:
        write_summary(status="offline_complete", close_reason="offline throughout shift", continuation_dispatched=CONTINUATION_DISPATCHED)

    log("session_complete")
