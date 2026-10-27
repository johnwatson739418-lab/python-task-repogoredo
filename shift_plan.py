#!/usr/bin/env python3
"""
Shift / session planner for GitHub Actions.
Calculates overlapping recording windows under the 6-hour job limit.
Supports adaptive splitting for short 1-hour tests (2 x 30 min shifts) as well as multi-hour sessions.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone, timedelta

MAX_WINDOW_MINUTES = 240          # 4 hours per shift (leaves 2 hours for upload under 6h GitHub limit)
OVERLAP_MINUTES = 5               # overlap between shifts
DEFAULT_TOTAL_HOURS = 8


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
    total_minutes = max(4, int(round(total_hours * 60)))

    # Adaptive window size: if 0.5h..1.5h test, split into 2 shifts so handover is tested
    if 20 <= total_minutes <= 90:
        win_cap = max(10, total_minutes // 2)
        overlap = 1
    else:
        win_cap = MAX_WINDOW_MINUTES
        overlap = OVERLAP_MINUTES

    windows = []
    cursor = 0
    idx = 0
    while cursor < total_minutes:
        start_min = max(0, cursor - (overlap if idx > 0 else 0))
        end_min = min(total_minutes, cursor + win_cap)
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
        if idx > 24:
            break

    if shift_index >= len(windows):
        _write_github_output({
            "session_id": session_id,
            "session_start": session_start.isoformat().replace("+00:00", "Z"),
            "window_index": str(shift_index),
            "window_count": str(len(windows)),
            "has_next": "false",
            "next_shift_index": "",
            "window_deadline": "",
            "window_duration_min": "0",
        })
        return 0

    win = windows[shift_index]
    # Use actual current time + duration_min for deadline so queued shifts still record full duration
    window_start = now_utc()
    window_end = window_start + timedelta(minutes=win["duration_min"])
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
    _write_github_output(outputs)
    return 0


def _write_github_output(data: dict) -> None:
    out_file = os.getenv("GITHUB_OUTPUT")
    if not out_file:
        return
    with open(out_file, "a", encoding="utf-8") as f:
        for k, v in data.items():
            f.write(f"{k}={v}\n")


if __name__ == "__main__":
    sys.exit(main())
