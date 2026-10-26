"""Bounded overnight windows, scheduled relative to a manual start (UTC epochs)."""
import argparse
import json
import math
import os
import time

STEP = int(os.environ.get('SHIFT_STEP_SECONDS') or 2 * 3600)
WINDOW = int(os.environ.get('SHIFT_WINDOW_SECONDS') or 3 * 3600)


def plan(start, total_hours, now, index=0):
    if not 0 < total_hours <= 14 or not 0 <= index <= 6:
        raise ValueError('total_hours must be greater than 0 and at most 14; shift_index must be 0..6')
    if start > now + 60:
        raise ValueError('session_start cannot be in the future')
    deadline = start + round(total_hours * 3600)
    window_deadline = min(deadline, start + index * STEP + WINDOW)
    remaining = max(0, window_deadline - now)
    next_index = max(index + 1, math.floor((now - start) / STEP) + 1)
    next_at = start + next_index * STEP
    return {'active': remaining > 0, 'duration_hours': min(WINDOW, remaining) / 3600,
            'deadline': deadline, 'window_deadline': window_deadline,
            'next_at': next_at, 'next_index': next_index,
            'chain': remaining > 0 and next_at < deadline and window_deadline < deadline and next_index <= 6}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--duration-until', type=int)
    args = parser.parse_args()
    now = int(time.time())
    if args.duration_until is not None:
        seconds = min(WINDOW, max(0, args.duration_until - now))
        if seconds <= 0:
            raise SystemExit('Session deadline passed before recorder startup')
        print(seconds / 3600)
        return
    total = float(os.environ.get('TOTAL_HOURS') or '9')
    start = int(os.environ.get('SESSION_START') or now)
    index = int(os.environ.get('SHIFT_INDEX') or '0')
    session = os.environ.get('SESSION_ID') or os.environ.get('GITHUB_RUN_ID') or str(start)
    if not session.isdigit():
        raise ValueError('session ID must contain digits only')
    result = plan(start, total, now, index)
    result.update(session_start=start, total_hours=total, session_id=session)
    if os.environ.get('SHIFT_STEP_SECONDS'):
        result['shift_step_seconds'] = os.environ['SHIFT_STEP_SECONDS']
    if os.environ.get('SHIFT_WINDOW_SECONDS'):
        result['shift_window_seconds'] = os.environ['SHIFT_WINDOW_SECONDS']
    print(json.dumps(result))
    destination = os.environ.get('GITHUB_OUTPUT')
    if destination:
        with open(destination, 'a', encoding='utf-8') as output:
            for key, value in result.items():
                output.write(f'{key}={str(value).lower() if isinstance(value, bool) else value}\n')


if __name__ == '__main__':
    main()
