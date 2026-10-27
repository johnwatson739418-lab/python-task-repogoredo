#!/usr/bin/env python3
"""
God-level YouTube Live Recorder – Core Capture Engine (v3 Hardened)
Fixes the 0-byte /live channel redirect bug on GitHub Actions (Azure IPs):
- Layer 1: Default yt-dlp client (None) + --ignore-no-formats-error + ejs:github Node runtime
- Layer 2: Fast curl HTML live-badge & canonical watch?v= extractor for /@Handle/live
- Layer 3: Multi-client fallback (default, tv_embedded, web_safari, mweb, android_vr, ios, android)
- Non-blocking stdout reader thread + explicit stderr error logging (never fails silently)
- Continuous live monitoring until window deadline
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import notify_telegram as tg

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE", "").strip() or "@BagadBillagang-p6s"
TARGET_CHANNEL_ID = os.getenv("TARGET_CHANNEL_ID", "").strip()
OUT_DIR = Path(os.getenv("OUT_DIR", "streams"))
COOKIE_FILE = os.getenv("COOKIE_FILE", "").strip()
USE_COOKIES_ENV = os.getenv("USE_COOKIES", "false").lower() == "true"
SESSION_DEADLINE = os.getenv("SESSION_DEADLINE", "").strip()  # ISO UTC
MAX_RUNTIME_SEC = int(os.getenv("MAX_RUNTIME_SEC", "17400"))  # ~4h50m safety
STALL_TIMEOUT_SEC = int(os.getenv("STALL_TIMEOUT_SEC", "90"))
FRAGMENT_RETRIES = int(os.getenv("FRAGMENT_RETRIES", "30"))
PROBE_INTERVAL_SEC = int(os.getenv("PROBE_INTERVAL_SEC", "25"))
MIN_FREE_DISK_BYTES = 512 * 1024 * 1024  # 512 MB safety floor

# Client priority: None = yt-dlp default (with ejs:github + node), followed by resilient fallbacks
DOWNLOAD_CLIENTS: list[Optional[str]] = [
    None,
    "tv_embedded",
    "web_safari",
    "mweb",
    "android_vr",
    "ios",
    "android",
]

VALIDATION_CLIENTS: list[Optional[str]] = [
    None,
    "web_safari",
    "tv_embedded",
    "mweb",
    "android_vr",
    "android",
]

# Pre-merged HLS mpegts stream first, with full best video+audio fallback
FORMAT_SELECTOR = (
    "b[height<=1080][height>=480][protocol*=m3u8]/"
    "b[height<=720][protocol*=m3u8]/"
    "b[protocol*=m3u8]/"
    "b[height<=1080]/"
    "b/best"
)

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------
LIVE_DETECTED_EVER = False
START_TS = time.time()
STOP_REQUESTED = False
FALLBACK_NOTIFIED = False


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def client_label(client: Optional[str]) -> str:
    return client if client else "default"


def js_runtime_args() -> list[str]:
    args = ["--remote-components", "ejs:github"]
    if shutil.which("node"):
        args += ["--js-runtimes", "node"]
    elif shutil.which("nodejs"):
        args += ["--js-runtimes", "nodejs"]
    return args


JS_ARGS = js_runtime_args()


def now_deadline_passed() -> bool:
    if not SESSION_DEADLINE:
        return (time.time() - START_TS) > MAX_RUNTIME_SEC
    try:
        dl = datetime.fromisoformat(SESSION_DEADLINE.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) >= dl
    except Exception:
        return (time.time() - START_TS) > MAX_RUNTIME_SEC


def remaining_seconds() -> int:
    if SESSION_DEADLINE:
        try:
            dl = datetime.fromisoformat(SESSION_DEADLINE.replace("Z", "+00:00"))
            return max(0, int((dl - datetime.now(timezone.utc)).total_seconds()))
        except Exception:
            pass
    return max(0, MAX_RUNTIME_SEC - int(time.time() - START_TS))


def cookies_available() -> bool:
    return (
        USE_COOKIES_ENV
        and bool(COOKIE_FILE)
        and Path(COOKIE_FILE).is_file()
        and Path(COOKIE_FILE).stat().st_size > 50
    )


def build_channel_live_url(handle: str) -> str:
    clean = handle.strip()
    if clean.startswith(("http://", "https://")):
        return clean.rstrip("/") + "/live" if not clean.endswith("/live") else clean
    if clean.startswith("UC") and len(clean) >= 20:
        return f"https://www.youtube.com/channel/{clean}/live"
    if not clean.startswith("@"):
        clean = f"@{clean}"
    return f"https://www.youtube.com/{clean}/live"


def fetch_html_live_video_id(url: str, with_cookies: bool) -> Optional[str]:
    """Fast curl check of /@Handle/live HTML to extract active live videoId."""
    curl_bin = shutil.which("curl")
    if not curl_bin:
        return None
    cmd = [
        curl_bin, "-s", "-L", "--compressed", "--max-time", "15",
        "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "-H", "Accept-Language: en-US,en;q=0.9",
    ]
    if with_cookies and cookies_available():
        cmd += ["-b", COOKIE_FILE]
    cmd.append(url)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=18)
        html = r.stdout or ""
        if not html:
            return None
        # Check if canonical points to a watch?v= URL and page has isLiveNow / isLive
        if any(m in html for m in ('"isLive":true', '"isLiveNow":true', 'BADGE_STYLE_TYPE_LIVE_NOW')):
            canon = re.search(r'<link rel="canonical" href="https://www\.youtube\.com/watch\?v=([A-Za-z0-9_-]{11})"', html)
            if canon:
                return canon.group(1)
            vid_match = re.search(r'"videoDetails"\s*:\s*\{[^}]*"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', html)
            if vid_match:
                return vid_match.group(1)
            # Scored fallback around live badge
            for m in re.finditer(r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', html):
                vid = m.group(1)
                ctx = html[max(0, m.start() - 1200):min(len(html), m.end() + 1200)]
                if '"isLive":true' in ctx or '"isLiveNow":true' in ctx or 'BADGE_STYLE_TYPE_LIVE_NOW' in ctx:
                    return vid
    except Exception as e:
        log(f"html probe exception: {e}")
    return None


def build_yt_dlp_base(client: Optional[str], with_cookies: bool) -> list[str]:
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-config",
        "--no-warnings",
        "--newline",
        "--restrict-filenames",
        "--no-part",
        "--no-playlist",
        "--no-live-from-start",
        "--fragment-retries", str(FRAGMENT_RETRIES),
        "--retries", "20",
        "--extractor-retries", "15",
        "--retry-sleep", "http:2",
        "--retry-sleep", "fragment:1",
        "--skip-unavailable-fragments",
        "--hls-use-mpegts",
        "--downloader", "m3u8:native",
        "-N", "1",
        "-f", FORMAT_SELECTOR,
    ] + JS_ARGS
    if client:
        cmd += ["--extractor-args", f"youtube:player_client={client}"]
    if with_cookies and cookies_available():
        cmd += ["--cookies", COOKIE_FILE]
    return cmd


def probe_live(handle: str) -> Optional[dict]:
    """Return {id, title, client, used_cookies, url} if channel is currently live."""
    live_url = build_channel_live_url(handle)
    cookie_modes = [True, False] if cookies_available() else [False]

    for use_c in cookie_modes:
        # Fast Layer 1: HTML canonical / badge check to resolve /live redirect into direct watch?v= ID
        html_vid = fetch_html_live_video_id(live_url, use_c)
        target_urls = []
        if html_vid:
            log(f"HTML live badge found video_id={html_vid} for {handle}")
            target_urls.append(f"https://www.youtube.com/watch?v={html_vid}")
        target_urls.append(live_url)

        for target_url in target_urls:
            for client in VALIDATION_CLIENTS:
                if now_deadline_passed() or STOP_REQUESTED:
                    return None
                cmd = [
                    sys.executable, "-m", "yt_dlp",
                    "--no-config",
                    "--no-warnings",
                    "--ignore-no-formats-error",
                    "--dump-json",
                    "--skip-download",
                    "--playlist-items", "1",
                ] + JS_ARGS
                if client:
                    cmd += ["--extractor-args", f"youtube:player_client={client}"]
                if use_c and cookies_available():
                    cmd += ["--cookies", COOKIE_FILE]
                cmd.append(target_url)

                try:
                    r = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        errors="replace",
                        timeout=35,
                    )
                    if r.returncode != 0 or not r.stdout.strip():
                        err_snip = " ".join((r.stderr or "").split())[:200]
                        if err_snip and "is not currently live" not in err_snip.lower():
                            log(f"probe warn | client={client_label(client)} | cookies={use_c} | {err_snip}")
                        continue

                    lines = [ln for ln in r.stdout.splitlines() if ln.strip().startswith("{")]
                    if not lines:
                        continue
                    data = json.loads(lines[-1])
                    if data.get("_type") == "playlist" and data.get("entries"):
                        entries = [e for e in data["entries"] if e]
                        if not entries:
                            continue
                        data = entries[0]

                    vid = data.get("id")
                    if not vid or len(str(vid)) != 11:
                        continue
                    live_status = str(data.get("live_status") or "").lower()
                    is_live = bool(data.get("is_live")) or live_status == "is_live"
                    if not is_live:
                        continue
                    return {
                        "id": vid,
                        "title": (data.get("title") or "live")[:100],
                        "client": client,
                        "used_cookies": use_c,
                        "url": f"https://www.youtube.com/watch?v={vid}",
                    }
                except subprocess.TimeoutExpired:
                    log(f"probe timeout | client={client_label(client)} | cookies={use_c}")
                except Exception as e:
                    log(f"probe error ({client_label(client)}, cookies={use_c}): {e}")

        # If HTML found a confirmed live video_id even if dump-json had a transient error, lock onto it!
        if html_vid:
            log(f"Using HTML-resolved live video_id={html_vid} directly")
            return {
                "id": html_vid,
                "title": "live_stream",
                "client": None,
                "used_cookies": use_c,
                "url": f"https://www.youtube.com/watch?v={html_vid}",
            }

    return None


def _stream_reader(proc: subprocess.Popen, state: dict) -> None:
    """Background daemon thread to read stdout without blocking main loop."""
    try:
        assert proc.stdout is not None
        for raw_line in iter(proc.stdout.readline, ""):
            if not raw_line:
                break
            line = raw_line.strip()
            if not line:
                continue
            state["last_output_ts"] = time.time()
            if any(k in line for k in ("ERROR", "WARNING", "chosen=", "Destination", "frame=", "[download]")):
                log(f"yt-dlp: {line[:220]}")
    except Exception:
        pass


def current_file_size(base_path: Path) -> int:
    """Check size of base_path or any .part / format variant."""
    total = 0
    for candidate in (base_path, Path(str(base_path) + ".part")):
        if candidate.exists():
            try:
                total = max(total, candidate.stat().st_size)
            except OSError:
                pass
    return total


def finalize_part_file(base_path: Path) -> Path:
    part_path = Path(str(base_path) + ".part")
    if part_path.exists() and not base_path.exists():
        try:
            part_path.replace(base_path)
        except OSError:
            return part_path
    return base_path


def stop_proc_gracefully(proc: Optional[subprocess.Popen], timeout: float = 20.0) -> None:
    if not proc or proc.poll() is not None:
        return
    try:
        if sys.platform == "win32":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGINT)
        proc.wait(timeout=timeout)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def record_stream(video_id: str, session_idx: int, preferred_client: Optional[str]) -> list[Path]:
    """Record stream in resilient chunks until stream ends or deadline passes."""
    global STOP_REQUESTED, FALLBACK_NOTIFIED
    url = f"https://www.youtube.com/watch?v={video_id}"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    clients: list[Optional[str]] = [preferred_client] + [c for c in DOWNLOAD_CLIENTS if c != preferred_client]
    cookie_modes = [True, False] if cookies_available() else [False]
    recorded_chunks: list[Path] = []
    chunk_idx = 1

    for use_c in cookie_modes:
        if not use_c and cookies_available() and not FALLBACK_NOTIFIED:
            FALLBACK_NOTIFIED = True
            tg.fallback_mode(CHANNEL_HANDLE, "Cookie capture failed; switched to cookie-less client mode")

        for client in clients:
            if now_deadline_passed() or STOP_REQUESTED:
                return recorded_chunks

            if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
                log("Low disk space reached -> stopping capture safely to preserve media")
                STOP_REQUESTED = True
                return recorded_chunks

            chunk_name = f"{CHANNEL_HANDLE.lstrip('@')}_{video_id}_s{session_idx:02d}_part{chunk_idx:03d}.ts"
            chunk_path = OUT_DIR / chunk_name
            log(f"Starting capture | file={chunk_name} | client={client_label(client)} | cookies={use_c}")

            cmd = build_yt_dlp_base(client, use_c) + [
                "-o", str(chunk_path),
                url,
            ]

            proc = None
            state = {"last_output_ts": time.time()}
            last_size = 0
            last_growth_ts = time.time()

            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                )
                reader = threading.Thread(target=_stream_reader, args=(proc, state), daemon=True)
                reader.start()

                while True:
                    if now_deadline_passed() or STOP_REQUESTED:
                        log("Window deadline or stop signal reached -> stopping yt-dlp gracefully")
                        stop_proc_gracefully(proc, timeout=20)
                        break

                    if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
                        log("Disk limit reached during capture -> stopping yt-dlp gracefully")
                        STOP_REQUESTED = True
                        stop_proc_gracefully(proc, timeout=20)
                        break

                    if proc.poll() is not None:
                        break

                    sz = current_file_size(chunk_path)
                    if sz > last_size:
                        last_size = sz
                        last_growth_ts = time.time()
                    elif time.time() - last_growth_ts > STALL_TIMEOUT_SEC:
                        log(f"Stall detected ({STALL_TIMEOUT_SEC}s with no new bytes) -> rotating client")
                        stop_proc_gracefully(proc, timeout=15)
                        break

                    time.sleep(1.0)

            except Exception as e:
                log(f"Capture exception: {e}")
                stop_proc_gracefully(proc, timeout=5)
            finally:
                stop_proc_gracefully(proc, timeout=5)

            final_chunk = finalize_part_file(chunk_path)
            if final_chunk.exists() and final_chunk.stat().st_size > 100_000:
                sz_mb = final_chunk.stat().st_size / 1e6
                log(f"Saved chunk {final_chunk.name} ({sz_mb:.1f} MB)")
                recorded_chunks.append(final_chunk)
                chunk_idx += 1
                if now_deadline_passed() or STOP_REQUESTED:
                    return recorded_chunks
                reprobe = probe_live(CHANNEL_HANDLE)
                if not reprobe or reprobe["id"] != video_id:
                    log("Stream ended or switched video_id -> returning to monitor loop")
                    return recorded_chunks

    return recorded_chunks


def write_summary(videos: list[str], files: list[Path], success: bool) -> None:
    total_bytes = sum(f.stat().st_size for f in files if f.exists())
    summary = {
        "channel": CHANNEL_HANDLE,
        "video_ids": videos,
        "video_id": videos[0] if videos else "none",
        "files": [str(f.name) for f in files if f.exists()],
        "size_bytes": total_bytes,
        "success": success,
        "live_detected": LIVE_DETECTED_EVER,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = OUT_DIR / "recording_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log(f"Summary written -> {summary_path} ({total_bytes / 1e6:.1f} MB total)")


def main() -> int:
    global LIVE_DETECTED_EVER, STOP_REQUESTED, FALLBACK_NOTIFIED

    def _sig_handler(signum, frame):
        global STOP_REQUESTED
        log(f"Signal {signum} received -> graceful stop")
        STOP_REQUESTED = True

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log("=== God-level Recorder (v3 Hardened) Start ===")
    log(f"Channel: {CHANNEL_HANDLE}")
    log(f"Cookies available: {cookies_available()}")
    log(f"JS Runtime Args: {JS_ARGS}")
    log(f"Deadline: {SESSION_DEADLINE or 'MAX_RUNTIME'}")
    log(f"Remaining window time: {remaining_seconds()}s")

    if not cookies_available():
        FALLBACK_NOTIFIED = True
        tg.fallback_mode(CHANNEL_HANDLE, "No cookies configured – using default + mobile client mode")

    all_files: list[Path] = []
    seen_videos: list[str] = []
    session_idx = 1
    probe_count = 0

    while not now_deadline_passed() and not STOP_REQUESTED:
        probe_count += 1
        probe = probe_live(CHANNEL_HANDLE)

        if not probe:
            rem = remaining_seconds()
            if rem <= 5:
                break
            sleep_sec = min(PROBE_INTERVAL_SEC, rem)
            if probe_count == 1 or probe_count % 10 == 0:
                log(f"Channel {CHANNEL_HANDLE} offline (probe #{probe_count}) -> monitoring ({rem}s left in window)...")
            time.sleep(sleep_sec)
            continue

        vid = probe["id"]
        c_label = client_label(probe["client"])
        if vid not in seen_videos:
            seen_videos.append(vid)
        LIVE_DETECTED_EVER = True
        log(f"🔴 LIVE DETECTED | id={vid} | client={c_label} | cookies={probe['used_cookies']}")
        tg.live_detected(CHANNEL_HANDLE, vid, c_label)

        if not probe["used_cookies"] and cookies_available() and not FALLBACK_NOTIFIED:
            FALLBACK_NOTIFIED = True
            tg.fallback_mode(CHANNEL_HANDLE, f"Cookies present but stream captured via {c_label} client")

        chunks = record_stream(vid, session_idx, probe["client"])
        all_files.extend(chunks)
        session_idx += 1

        if not now_deadline_passed() and not STOP_REQUESTED:
            time.sleep(5)

    if not all_files:
        log("NO_RECORDING: Channel remained offline throughout this shift window.")
        write_summary(seen_videos, [], False)
        (OUT_DIR / "NO_RECORDING").write_text("Channel was offline during shift window\n", encoding="utf-8")
        return 0

    total_gb = sum(f.stat().st_size for f in all_files if f.exists()) / 1e9
    write_summary(seen_videos, all_files, True)
    log(f"✅ Window capture finished | {len(all_files)} file(s) | {total_gb:.2f} GB total")
    return 0


if __name__ == "__main__":
    sys.exit(main())
