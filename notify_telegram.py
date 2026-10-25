"""Optional bounded text notification. Never log Telegram URLs or error bodies."""
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


def chunks(text, size=3500):
    for offset in range(0, len(text), size):
        yield text[offset:offset + size]


def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    chat = os.environ.get('TELEGRAM_CHAT_ID')
    if not token or not chat:
        print('Telegram not configured; skipping optional notification')
        return
    record_outcome = os.environ.get('RECORD_OUTCOME', 'unknown')
    lines = [f'YouTube recorder: {os.environ.get("CHANNEL_HANDLE", "unknown")}',
             f'Recording step: {record_outcome}']
    try:
        report = json.loads(Path('upload-report.json').read_text())
        lines.append('All uploads confirmed: ' + str(report.get('all_uploaded', False)))
        if not report.get('files'):
            lines.append('No recording files found.')
        for entry in report.get('files', []):
            lines.append(f'{entry["file"]}: {"uploaded" if entry["uploaded"] else "FAILED"}')
            lines += [r['url'] for r in entry.get('receipts', [])]
    except (OSError, ValueError):
        lines.append('Upload report unavailable; inspect workflow logs.')
    if os.environ.get('GITHUB_SERVER_URL') and os.environ.get('GITHUB_REPOSITORY'):
        lines.append(f'{os.environ["GITHUB_SERVER_URL"]}/{os.environ["GITHUB_REPOSITORY"]}/actions/runs/{os.environ.get("GITHUB_RUN_ID", "")}')
    end = time.monotonic() + 90
    for part in chunks('\n'.join(lines)):
        left = end - time.monotonic()
        if left <= 0:
            raise SystemExit('Telegram notification time budget exceeded')
        body = urllib.parse.urlencode({'chat_id': chat, 'text': part}).encode()
        request = urllib.request.Request(f'https://api.telegram.org/bot{token}/sendMessage', data=body)
        try:
            with urllib.request.urlopen(request, timeout=min(20, left)) as response:
                if not json.load(response).get('ok'):
                    raise SystemExit('Telegram rejected notification; response withheld')
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise SystemExit('Telegram notification failed; raw error withheld') from None
    print('Telegram text notification sent')


if __name__ == '__main__':
    main()
