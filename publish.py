#!/usr/bin/env python3
"""
Upload pipeline – STRICT order:
  1. Google Drive via rclone   (PRIMARY – permanent 5 TiB storage, auto-detects remote section)
  2. Gofile                    (SECONDARY fallback)
  3. GitHub Releases           (TERTIARY, split < 1.9 GB)
Uploads ALL recorded media chunks and sends clickable links to Telegram.
"""

from __future__ import annotations

import configparser
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import requests
import notify_telegram as tg

OUT_DIR = Path(os.getenv("OUT_DIR", "streams"))
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE", "@BagadBillagang-p6s")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "").strip()
RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "drive:YouTubeRecordings").strip()
GOFILE_TOKEN = os.getenv("GOFILE_TOKEN", "").strip()
MAX_RELEASE_PART = 1_900_000_000  # < 2 GB


def log(msg: str) -> None:
    print(f"[publish] {msg}", flush=True)


def find_media_files() -> list[Path]:
    if not OUT_DIR.exists():
        return []
    candidates = []
    for p in sorted(OUT_DIR.rglob("*")):
        if p.is_file() and p.suffix.lower() in {".ts", ".mp4", ".mkv", ".m4a", ".part"}:
            if p.stat().st_size > 100_000:
                candidates.append(p)
    return candidates


def resolve_rclone_config() -> Optional[Path]:
    """Locate or materialize rclone.conf from env or disk."""
    raw_secret = os.getenv("RCLONE_CONFIG", "").strip()
    if raw_secret and "[" in raw_secret:
        import tempfile
        tmp = Path(tempfile.gettempdir()) / "rclone.conf"
        tmp.write_text(raw_secret, encoding="utf-8")
        try:
            tmp.chmod(0o600)
        except Exception:
            pass
        return tmp

    candidates = [
        Path(os.getenv("RCLONE_CONFIG_PATH", "/tmp/rclone.conf")),
        Path("rclone.conf"),
        Path.home() / ".config" / "rclone" / "rclone.conf",
    ]
    for c in candidates:
        if c.is_file() and c.stat().st_size > 20:
            return c.resolve()
    return None


def resolve_remote_prefix(conf_path: Path, configured_remote: str) -> str:
    """Ensure the remote name (e.g. 'drive:') matches the actual section inside rclone.conf."""
    try:
        cp = configparser.ConfigParser(interpolation=None)
        cp.read(conf_path, encoding="utf-8-sig")
        sections = cp.sections()
        if not sections:
            return configured_remote
        if ":" in configured_remote:
            rem_name, folder = configured_remote.split(":", 1)
        else:
            rem_name, folder = configured_remote, "YouTubeRecordings"
        if rem_name not in sections:
            actual = sections[0]
            log(f"Auto-corrected rclone remote name '{rem_name}:' -> '{actual}:' from rclone.conf")
            return f"{actual}:{folder}"
    except Exception:
        pass
    return configured_remote


def rclone_upload(path: Path) -> Optional[str]:
    """Upload file to Google Drive via rclone and return clickable view URL or remote path."""
    conf_path = resolve_rclone_config()
    if not conf_path:
        log("rclone: no rclone.conf found (set RCLONE_CONFIG secret or include rclone.conf)")
        return None

    remote_base = resolve_remote_prefix(conf_path, RCLONE_REMOTE)
    date_folder = time.strftime("%Y-%m-%d", time.gmtime())
    remote_path = f"{remote_base.rstrip('/')}/{CHANNEL_HANDLE.lstrip('@')}/{date_folder}/{path.name}"

    base_cmd = ["rclone", "--config", str(conf_path)]
    cmd = base_cmd + [
        "copyto",
        str(path),
        remote_path,
        "--transfers", "1",
        "--checkers", "1",
        "--drive-chunk-size", "32M",
        "--retries", "3",
        "--low-level-retries", "5",
        "--contimeout", "20s",
        "--timeout", "120s",
    ]

    try:
        log(f"rclone uploading -> {remote_path}")
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5400)
        if r.returncode != 0:
            log(f"rclone copyto failed: {r.stderr[-400:]}")
            return None

        # Fetch Google Drive File ID for clickable link
        info = subprocess.run(
            base_cmd + ["lsjson", remote_path, "--stat"],
            capture_output=True,
            text=True,
            timeout=45,
        )
        if info.returncode == 0 and info.stdout.strip():
            details = json.loads(info.stdout)
            file_id = details.get("ID", "")
            if re.fullmatch(r"[A-Za-z0-9_-]+", file_id):
                link = f"https://drive.google.com/file/d/{file_id}/view"
                log(f"rclone SUCCESS: {link}")
                return link

        log(f"rclone SUCCESS: {remote_path}")
        return remote_path
    except Exception as e:
        log(f"rclone exception: {e}")
        return None


def gofile_upload(path: Path) -> Optional[str]:
    """Upload to Gofile using standard global upload endpoint with fallback to server lookup."""
    endpoints = ["https://upload.gofile.io/uploadfile"]
    try:
        srv = requests.get("https://api.gofile.io/servers", timeout=15).json()
        servers = srv.get("data", {}).get("servers", [])
        if servers:
            endpoints.append(f"https://{servers[0]['name']}.gofile.io/uploadFile")
    except Exception:
        pass

    for url in endpoints:
        try:
            headers = {}
            if GOFILE_TOKEN:
                headers["Authorization"] = f"Bearer {GOFILE_TOKEN}"
            with path.open("rb") as f:
                files = {"file": (path.name, f)}
                log(f"Gofile upload -> {path.name} ({url})")
                r = requests.post(url, headers=headers, files=files, timeout=1800)
            j = r.json()
            if j.get("status") == "ok":
                link = j.get("data", {}).get("downloadPage")
                if link:
                    log(f"Gofile SUCCESS: {link}")
                    return link
        except Exception as e:
            log(f"Gofile attempt error ({url}): {e}")
    return None


def split_file(path: Path, max_size: int = MAX_RELEASE_PART) -> list[Path]:
    if path.stat().st_size <= max_size:
        return [path]
    parts = []
    with path.open("rb") as src:
        idx = 0
        while True:
            chunk = src.read(max_size)
            if not chunk:
                break
            part = path.with_suffix(f"{path.suffix}.part{idx:02d}")
            part.write_bytes(chunk)
            parts.append(part)
            idx += 1
    return parts


def github_release_upload(path: Path) -> list[str]:
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        log("GitHub Releases: missing token or repository")
        return []
    owner, repo = GITHUB_REPOSITORY.split("/", 1)
    tag = f"rec-{CHANNEL_HANDLE.lstrip('@')}-{int(time.time())}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    create_url = f"https://api.github.com/repos/{owner}/{repo}/releases"
    body = {
        "tag_name": tag,
        "name": f"Recording {CHANNEL_HANDLE} {path.name}",
        "body": f"Automatic recording backup\nChannel: {CHANNEL_HANDLE}\nFile: {path.name}",
        "draft": False,
        "prerelease": False,
    }
    try:
        r = requests.post(create_url, headers=headers, json=body, timeout=30)
        r.raise_for_status()
        release = r.json()
        upload_url = release["upload_url"].split("{")[0]
        release_url = release["html_url"]
    except Exception as e:
        log(f"Create release failed: {e}")
        return []

    links = [release_url]
    parts = split_file(path)
    for part in parts:
        try:
            with part.open("rb") as f:
                headers_up = headers.copy()
                headers_up["Content-Type"] = "application/octet-stream"
                params = {"name": part.name}
                log(f"Uploading release asset {part.name} ({part.stat().st_size / 1e6:.1f} MB)")
                ur = requests.post(
                    upload_url,
                    headers=headers_up,
                    params=params,
                    data=f,
                    timeout=1800,
                )
                if ur.status_code in (200, 201):
                    dl = ur.json().get("browser_download_url", "")
                    if dl:
                        links.append(dl)
        except Exception as e:
            log(f"Asset exception: {e}")
        finally:
            if part != path and part.exists():
                part.unlink(missing_ok=True)
    return links


def main() -> int:
    media = find_media_files()
    if not media:
        log("No media files found – channel was offline during this window.")
        report = {
            "channel": CHANNEL_HANDLE,
            "status": "offline_no_media",
            "all_uploaded": True,
            "files": [],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / "upload_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 0

    video_id = "unknown"
    summary_path = OUT_DIR / "recording_summary.json"
    if summary_path.exists():
        try:
            video_id = json.loads(summary_path.read_text()).get("video_id", "unknown")
        except Exception:
            pass

    all_receipts: list[str] = []
    file_reports: list[dict] = []
    total_gb = 0.0

    for target in media:
        size_gb = target.stat().st_size / 1e9
        total_gb += size_gb
        log(f"Processing {target.name} ({size_gb:.2f} GB)")
        file_links: list[str] = []
        method = "none"

        # 1. PRIMARY – Google Drive via rclone
        drive_link = rclone_upload(target)
        if drive_link:
            file_links.append(f"Drive: {drive_link}")
            method = "google_drive"
        else:
            log("Drive unavailable/failed -> trying Gofile fallback")
            # 2. SECONDARY – Gofile
            gf_link = gofile_upload(target)
            if gf_link:
                file_links.append(f"Gofile: {gf_link}")
                method = "gofile"
            else:
                log("Gofile failed -> trying GitHub Releases fallback")
                # 3. TERTIARY – GitHub Releases
                rel_links = github_release_upload(target)
                if rel_links:
                    file_links.extend(rel_links)
                    method = "github_release"

        all_receipts.extend(file_links)
        file_reports.append({
            "file": target.name,
            "size_gb": round(size_gb, 3),
            "uploaded": bool(file_links),
            "method": method,
            "links": file_links,
        })

    all_ok = all(f["uploaded"] for f in file_reports)
    report = {
        "channel": CHANNEL_HANDLE,
        "video_id": video_id,
        "total_size_gb": round(total_gb, 3),
        "all_uploaded": all_ok,
        "files": file_reports,
        "links": all_receipts,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    report_path = OUT_DIR / "upload_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    Path("upload-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    log(f"Report saved -> {report_path}")

    if all_receipts:
        tg.upload_done(CHANNEL_HANDLE, video_id, total_gb, all_receipts)
        log("✅ All uploads finished")
        return 0 if all_ok else 1
    else:
        tg.shift_failed(CHANNEL_HANDLE, "All 3 upload tiers (Drive, Gofile, Releases) failed")
        log("❌ All upload methods failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
