"""Dispatch the next overlapping window once, with bounded retries. No credentials logged."""
import json
import os
import re
import time
import urllib.error
import urllib.request


def dispatch_body():
    inputs = {
        'total_hours': str(os.environ['TOTAL_HOURS']), 'release_backup': 'false',
        'session_start': str(os.environ['SESSION_START']),
        'session_id': str(os.environ['SESSION_ID']), 'shift_index': str(os.environ['NEXT_INDEX'])}
    if os.environ.get('SHIFT_STEP_SECONDS'):
        inputs['shift_step_seconds'] = str(os.environ['SHIFT_STEP_SECONDS'])
    if os.environ.get('SHIFT_WINDOW_SECONDS'):
        inputs['shift_window_seconds'] = str(os.environ['SHIFT_WINDOW_SECONDS'])
    return {'ref': os.environ['WORKFLOW_REF'], 'inputs': inputs}


def main():
    target = int(os.environ['NEXT_AT'])
    deadline = int(os.environ['DEADLINE'])
    repository = os.environ['GITHUB_REPOSITORY']
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repository):
        raise ValueError('Invalid repository')
    print(f'Next shift requested at UTC epoch {target}; session deadline {deadline}', flush=True)
    heartbeat = 0
    while time.time() < target:
        if time.time() - heartbeat >= 300:
            print(f'Next-shift dispatch in {max(0, int(target-time.time()))} seconds', flush=True)
            heartbeat = time.time()
        time.sleep(min(30, max(0, target-time.time())))
    if time.time() >= deadline:
        print('Session deadline reached; no further shift started', flush=True)
        return
    body = json.dumps(dispatch_body()).encode()
    url = f'https://api.github.com/repos/{repository}/actions/workflows/record.yml/dispatches'
    delays = [10, 20, 30, 45, 60, 90]
    for attempt in range(6):
        request = urllib.request.Request(url, data=body, method='POST', headers={
            'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
            'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json',
            'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                if response.status != 204:
                    raise RuntimeError(f'Unexpected dispatch HTTP {response.status}')
            print('Next shift accepted by GitHub; runner startup/capture still require verification', flush=True)
            return
        except urllib.error.HTTPError as error:
            if error.code not in {429, 500, 502, 503, 504}:
                raise SystemExit(f'Next-shift dispatch denied: HTTP {error.code}; check Actions write permission')
            print(f'Attempt {attempt+1}/6 dispatch HTTP error {error.code}, backing off...', flush=True)
        except (urllib.error.URLError, TimeoutError) as error:
            print(f'Attempt {attempt+1}/6 dispatch network error {error}, backing off...', flush=True)
        if attempt < 5:
            time.sleep(delays[attempt])
    raise SystemExit('Next-shift dispatch failed after six bounded attempts')


if __name__ == '__main__':
    main()
