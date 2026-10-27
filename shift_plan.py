#!/usr/bin/env python3
"""
Shift / session planner for GitHub Actions.
Calculates overlapping recording windows under the 6-hour job limit.
Outputs GitHub Actions outputs for the workflow.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Defaults
MAX_WINDOW_MINUTES = 300          # 5 hours safe
OVERLAP_MINUTES = 8               # small overlap between shifts
DEFAULT_TOTAL_HOURS = 6


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> int:
    total_hours = float(os.getenv("TOTAL_HOURS", str(DEFAULT_TOTAL_HOURS)))
    session_start_raw = os.getenv("SESSION_START", "").strip()
    session_id = os.getenv("SESSION_ID", "").strip() or now_utc().strftime("%Y%m%dT%H%M%SZ")
    shift_index = int(os.getenv("SHIFT_INDEX", "0"))

    session_start = parse_iso(session_start_raw) or now_utc()
    total_minutes = int(total_hours * 60)

    # For testing short runs (<= 120 min), split into exactly 2 shifts to test handover
    if total_minutes <= 120:
        overlap = min(2, max(1, total_minutes // 20))
        max_window = (total_minutes // 2) + overlap
    else:
        max_window = MAX_WINDOW_MINUTES
        overlap = OVERLAP_MINUTES

    # Build windows
    windows = []
    cursor = 0
    idx = 0
    while cursor < total_minutes:
        start_min = max(0, cursor - (overlap if idx > 0 else 0))
        end_min = min(total_minutes, start_min + max_window)
        if end_min <= start_min:
            break
        windows.append({
            "index": idx,
            "start_offset_min": start_min,
            "end_offset_min": end_min,
            "duration_min": end_min - start_min,
        })
        cursor = end_min
        idx += 1
        if idx > 24:  # safety
            break

    # Current window
    if shift_index >= len(windows):
        print(f"::error::shift_index {shift_index} out of range (only {len(windows)} windows)")
        # Still write minimal outputs so workflow can finish gracefully
        print(f"window_index={shift_index}")
        print(f"window_count={len(windows)}")
        print(f"has_next=false")
        _write_github_output({
            "session_id": session_id,
            "window_index": str(shift_index),
            "window_count": str(len(windows)),
            "has_next": "false",
            "window_deadline": "",
            "window_duration_min": "0",
        })
        return 0

    win = windows[shift_index]
    window_start = session_start + timedelta(minutes=win["start_offset_min"])
    window_end = session_start + timedelta(minutes=win["end_offset_min"])
    deadline = window_end.isoformat().replace("+00:00", "Z")

    has_next = "true" if (shift_index + 1) < len(windows) else "false"

    outputs = {
        "session_id": session_id,
        "session_start": session_start.isoformat().replace("+00:00", "Z"),
        "window_index": str(shift_index),
        "window_count": str(len(windows)),
        "window_start": window_start.isoformat().replace("+00:00", "Z"),
        "window_deadline": deadline,
        "window_duration_min": str(win["duration_min"]),
        "has_next": has_next,
        "next_shift_index": str(shift_index + 1) if has_next == "true" else "",
    }

    print("=== SHIFT PLAN ===")
    print(json.dumps(outputs, indent=2))
    print(f"Total windows planned: {len(windows)}")
    for w in windows:
        print(f"  [{w['index']}] {w['start_offset_min']:4d} -> {w['end_offset_min']:4d} min  ({w['duration_min']} min)")

    _write_github_output(outputs)
    return 0


def _write_github_output(data: dict) -> None:
    out_file = os.getenv("GITHUB_OUTPUT")
    if not out_file:
        return
    with open(out_file, "a", encoding="utf-8") as f:
        for k, v in data.items():
            # multiline safe
            if "\n" in str(v):
                f.write(f"{k}<<EOF\n{v}\nEOF\n")
            else:
                f.write(f"{k}={v}\n")


if __name__ == "__main__":
    sys.exit(main())
