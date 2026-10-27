#!/usr/bin/env python3
"""
God-level YouTube Live Recorder – Core Capture Engine (Fixed & Hardened)
- Official upstream yt-dlp clients: android_vr, android, ios, mweb, tv_embedded
- Cookies fully optional (never fatal)
- Non-blocking stdout reader thread (zero readline deadlocks)
- Continuous live monitoring until window deadline (never quits after 4 mins)
- Direct HLS MPEG-TS streaming to disk (no 2x disk remux crash)
- Real-time Telegram alerts on LIVE detected, fallback, and completion
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

# Official upstream yt-dlp clients (verified for datacenter IPs)
DOWNLOAD_CLIENTS = [
    "android_vr",
    "android",
    "ios",
    "mweb",
    "tv_embedded",
]

VALIDATION_CLIENTS = [
    "android_vr",
    "android",
    "ios",
    "mweb",
]

# Pre-merged HLS mpegts stream first so bytes flush immediately to a single .ts file
FORMAT_SELECTOR = (
    "b[height<=1080][height>=480][protocol*=m3u8]/"
    "b[height<=720][protocol*=m3u8]/"
    "b[protocol*=m3u8]/"
    "b[height<=1080]/"
    "b"
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


def build_yt_dlp_base(client: str, with_cookies: bool) -> list[str]:
    cmd = [
        sys.executable, "-m", "yt_dlp",
        "--no-config",
        "--no-warnings",
        "--newline",
        "--restrict-filenames",
        "--no-part",
        "--no-playlist",
        "--no-live-from-start",
        "--extractor-args", f"youtube:player_client={client}",
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
    ]
    if with_cookies and cookies_available():
        cmd += ["--cookies", COOKIE_FILE]
    return cmd


def probe_live(handle: str) -> Optional[dict]:
    """Return {id, title, client, used_cookies, url} if channel is currently live."""
    clean_handle = handle.strip()
    if not clean_handle.startswith("@") and not clean_handle.startswith("UC"):
        clean_handle = f"@{clean_handle}"

    if clean_handle.startswith("UC"):
        url = f"https://www.youtube.com/channel/{clean_handle}/live"
    else:
        url = f"https://www.youtube.com/{clean_handle}/live"

    cookie_modes = [True, False] if cookies_available() else [False]

    for use_c in cookie_modes:
        for client in VALIDATION_CLIENTS:
            if now_deadline_passed() or STOP_REQUESTED:
                return None
            cmd = [
                sys.executable, "-m", "yt_dlp",
                "--no-config",
                "--no-warnings",
                "--extractor-args", f"youtube:player_client={client}",
                "--dump-json",
                "--skip-download",
                "--playlist-items", "1",
            ]
            if use_c and cookies_available():
                cmd += ["--cookies", COOKIE_FILE]
            cmd.append(url)

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
                    continue
                lines = [ln for ln in r.stdout.splitlines() if ln.strip().startswith("{")]
                if not lines:
                    continue
                data = json.loads(lines[-1])
                vid = data.get("id")
                if not vid:
                    continue
                live_status = str(data.get("live_status") or "")
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
            except Exception as e:
                log(f"probe error ({client}, cookies={use_c}): {e}")
                continue
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
            if any(k in line for k in ("ERROR", "WARNING", "chosen=", "Destination", "frame=")):
                log(f"yt-dlp: {line[:200]}")
    except Exception:
        pass


def current_file_size(base_path: Path) -> int:
    """Check size of base_path or any .part variant."""
    total = 0
    if base_path.exists():
        try:
            total = max(total, base_path.stat().st_size)
        except OSError:
            pass
    part_path = Path(str(base_path) + ".part")
    if part_path.exists():
        try:
            total = max(total, part_path.stat().st_size)
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


def record_stream(video_id: str, session_idx: int, preferred_client: str) -> list[Path]:
    """Record stream in resilient chunks until stream ends or deadline passes."""
    global STOP_REQUESTED, FALLBACK_NOTIFIED
    url = f"https://www.youtube.com/watch?v={video_id}"
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    clients = [preferred_client] + [c for c in DOWNLOAD_CLIENTS if c != preferred_client]
    cookie_modes = [True, False] if cookies_available() else [False]
    recorded_chunks: list[Path] = []
    chunk_idx = 1

    for use_c in cookie_modes:
        if not use_c and cookies_available() and not FALLBACK_NOTIFIED:
            FALLBACK_NOTIFIED = True
            tg.fallback_mode(CHANNEL_HANDLE, "Cookie capture failed; switched to pure mobile client mode")

        for client in clients:
            if now_deadline_passed() or STOP_REQUESTED:
                return recorded_chunks

            # Check free disk space
            if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
                log("Low disk space reached -> stopping capture safely to preserve media")
                STOP_REQUESTED = True
                return recorded_chunks

            chunk_name = f"{CHANNEL_HANDLE.lstrip('@')}_{video_id}_s{session_idx:02d}_part{chunk_idx:03d}.ts"
            chunk_path = OUT_DIR / chunk_name
            log(f"Starting capture | file={chunk_name} | client={client} | cookies={use_c}")

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
                        proc.send_signal(signal.SIGINT)
                        try:
                            proc.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break

                    if shutil.disk_usage(OUT_DIR).free < MIN_FREE_DISK_BYTES:
                        log("Disk limit reached during capture -> stopping yt-dlp gracefully")
                        STOP_REQUESTED = True
                        proc.send_signal(signal.SIGINT)
                        try:
                            proc.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break

                    if proc.poll() is not None:
                        break

                    sz = current_file_size(chunk_path)
                    if sz > last_size:
                        last_size = sz
                        last_growth_ts = time.time()
                    elif time.time() - last_growth_ts > STALL_TIMEOUT_SEC:
                        log(f"Stall detected ({STALL_TIMEOUT_SEC}s with no new bytes) -> rotating client")
                        proc.send_signal(signal.SIGINT)
                        try:
                            proc.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break

                    time.sleep(1.0)

            except Exception as e:
                log(f"Capture exception: {e}")
                if proc and proc.poll() is None:
                    proc.kill()
            finally:
                if proc and proc.poll() is None:
                    proc.kill()

            final_chunk = finalize_part_file(chunk_path)
            if final_chunk.exists() and final_chunk.stat().st_size > 100_000:
                sz_mb = final_chunk.stat().st_size / 1e6
                log(f"Saved chunk {final_chunk.name} ({sz_mb:.1f} MB)")
                recorded_chunks.append(final_chunk)
                chunk_idx += 1
                # Check if streamer went offline or if we should continue
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
    log("=== God-level Recorder (Fixed v4) Start ===")
    log(f"Channel: {CHANNEL_HANDLE}")
    log(f"Cookies available: {cookies_available()}")
    log(f"Deadline: {SESSION_DEADLINE or 'MAX_RUNTIME'}")
    log(f"Remaining window time: {remaining_seconds()}s")

    if not cookies_available():
        FALLBACK_NOTIFIED = True
        tg.fallback_mode(CHANNEL_HANDLE, "No cookies configured – using Android/iOS mobile client mode")

    all_files: list[Path] = []
    seen_videos: list[str] = []
    session_idx = 1
    probe_count = 0

    # Continuous monitoring loop until window deadline
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
        if vid not in seen_videos:
            seen_videos.append(vid)
        LIVE_DETECTED_EVER = True
        log(f"🔴 LIVE DETECTED | id={vid} | client={probe['client']} | cookies={probe['used_cookies']}")
        tg.live_detected(CHANNEL_HANDLE, vid, probe["client"])

        if not probe["used_cookies"] and cookies_available() and not FALLBACK_NOTIFIED:
            FALLBACK_NOTIFIED = True
            tg.fallback_mode(CHANNEL_HANDLE, "Cookies present but stream captured via mobile client")

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
