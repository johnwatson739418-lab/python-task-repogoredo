#!/usr/bin/env python3
"""
Dispatch the next shift via GitHub workflow_dispatch.
"""

from __future__ import annotations

import os
import sys
import time
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "").strip()
SESSION_ID = os.getenv("SESSION_ID", "")
SESSION_START = os.getenv("SESSION_START", "")
NEXT_SHIFT_INDEX = os.getenv("NEXT_SHIFT_INDEX", "")
TOTAL_HOURS = os.getenv("TOTAL_HOURS", "6")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE", "")  # optional override


def log(msg: str) -> None:
    print(f"[chain] {msg}", flush=True)


def main() -> int:
    if not GITHUB_TOKEN or not GITHUB_REPOSITORY:
        log("Missing GITHUB_TOKEN or GITHUB_REPOSITORY – cannot chain")
        return 0

    if not NEXT_SHIFT_INDEX:
        log("No next shift – session complete")
        return 0

    owner, repo = GITHUB_REPOSITORY.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/record.yml/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    inputs = {
        "total_hours": str(TOTAL_HOURS),
        "session_start": SESSION_START,
        "session_id": SESSION_ID,
        "shift_index": str(NEXT_SHIFT_INDEX),
    }
    if CHANNEL_HANDLE:
        inputs["channel_handle"] = CHANNEL_HANDLE

    payload = {
        "ref": os.getenv("GITHUB_REF_NAME", "main"),
        "inputs": inputs,
    }

    for attempt in range(5):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            if r.status_code in (204, 200):
                log(f"✅ Dispatched next shift #{NEXT_SHIFT_INDEX}")
                return 0
            log(f"Dispatch failed ({r.status_code}): {r.text[:300]}")
        except Exception as e:
            log(f"Exception: {e}")
        time.sleep(3 * (attempt + 1))

    log("❌ Failed to dispatch next shift after retries")
    return 1


if __name__ == "__main__":
    sys.exit(main())
