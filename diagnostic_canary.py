"""One bounded, truthful live capture/Drive canary; report contains no credentials.

No source/client/account rotation. Only combined live HLS is supported by this
small test. A format mismatch fails rather than changing authentication strategy.
"""
from __future__ import annotations
import argparse
import configparser
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen
import uuid

DEADLINE = None


class Failure(Exception):
    pass

SECRET_NAMES = {'YT_COOKIES', 'RCLONE_CONFIG_TEXT', 'GH_TOKEN', 'GITHUB_TOKEN',
                'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'GOFILE_TOKEN'}
DENIALS = (
    ('AUTH_BLOCKED', ('cookies are no longer valid', 'not a bot', 'sign in',
                      'login required', 'members-only', 'private video', 'confirm your age')),
    ('RATE_LIMITED', ('too many requests', 'rate limit', 'http error 429')),
    ('ACCESS_DENIED', ('http error 403', '403 forbidden', 'access denied')),
)

def private_write(path, text):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
        handle.write(text)
    os.chmod(path, 0o600)


def child_env():
    env = os.environ.copy()
    for name in list(env):
        if name in SECRET_NAMES or name.startswith(('YT_COOKIES', 'TELEGRAM_', 'GOFILE_', 'RCLONE_')):
            env.pop(name, None)
    return env


def remaining(cap):
    value = cap if DEADLINE is None else min(cap, DEADLINE - time.monotonic())
    if value <= 0:
        raise Failure('TEST_DEADLINE_REACHED')
    return value


def run(command, timeout=45, file_size_limit=None):
    timeout = remaining(timeout)
    def limit_file():
        resource.setrlimit(resource.RLIMIT_FSIZE, (file_size_limit, file_size_limit))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               stdin=subprocess.DEVNULL, start_new_session=True,
                               env=child_env(), preexec_fn=limit_file if file_size_limit else None)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate(timeout=5)
        raise Failure('OPERATION_TIMEOUT') from None
    return process.returncode, stdout, stderr


def denial(raw):
    text = raw.decode('utf-8', 'replace').casefold()
    for status, phrases in DENIALS:
        if any(phrase in text for phrase in phrases):
            return status
    return None


def valid_selection(url, video, channel):
    if not re.fullmatch(r'[A-Za-z0-9_-]{11}', video or '') or not re.fullmatch(r'UC[A-Za-z0-9_-]{22}', channel or ''):
        return False
    try:
        parsed = urlparse(url)
        return (parsed.scheme == 'https' and parsed.hostname in ('youtube.com', 'www.youtube.com')
                and parsed.path == '/watch' and parse_qs(parsed.query) == {'v': [video]}
                and not parsed.fragment and not parsed.username and not parsed.password and not parsed.port)
    except ValueError:
        return False


def select_format(info, video, channel):
    if not isinstance(info, dict) or info.get('id') != video or info.get('channel_id') != channel:
        raise Failure('SOURCE_IDENTITY_MISMATCH')
    if info.get('is_live') is not True or info.get('live_status') != 'is_live':
        raise Failure('SOURCE_NOT_CONFIRMED_LIVE')
    formats = []
    for item in info.get('formats') or []:
        if not isinstance(item, dict):
            continue
        url = item.get('url')
        height = item.get('height')
        if (item.get('protocol') in ('m3u8', 'm3u8_native')
                and isinstance(url, str) and url.startswith('https://')
                and item.get('vcodec') not in (None, 'none') and item.get('acodec') not in (None, 'none')
                and isinstance(height, (int, float)) and 0 < height <= 1080):
            formats.append(item)
    if not formats:
        raise Failure('NO_PLAYABLE_COMBINED_HLS_FORMAT')
    return max(formats, key=lambda item: (item.get('height', 0), item.get('tbr') or 0))


def inspect_media(path):
    if not path.is_file() or not path.stat().st_size:
        raise Failure('ZERO_MEDIA')
    code, out, err = run(['ffprobe', '-v', 'error', '-show_entries',
                         'format=duration:stream=codec_type,codec_name,width,height',
                         '-of', 'json', str(path)], timeout=30)
    if code:
        raise Failure('MEDIA_PROBE_FAILED')
    try:
        data = json.loads(out)
        streams = data.get('streams') or []
        duration = float(data.get('format', {}).get('duration', 0))
        video = next(item for item in streams if item.get('codec_type') == 'video')
        audio = next(item for item in streams if item.get('codec_type') == 'audio')
        if duration < 60 or not video.get('width') or not video.get('height'):
            raise ValueError('insufficient_media')
    except (ValueError, TypeError, KeyError, StopIteration):
        raise Failure('MEDIA_AUDIO_VIDEO_OR_DURATION_INVALID') from None
    # A small bounded decode proves actual readable audio/video, not just a header.
    code, out, err = run(['ffmpeg', '-nostdin', '-v', 'error', '-i', str(path), '-t', '5',
                         '-map', '0:v:0', '-map', '0:a:0', '-f', 'null', '-'], timeout=30)
    if code:
        raise Failure('MEDIA_SAMPLE_DECODE_FAILED')
    return {'duration_seconds': round(duration, 3), 'width': video['width'],
            'height': video['height'], 'audio_present': True, 'video_present': True,
            'decode_scope': 'first_five_seconds', 'sample_decode_passed': True}


def md5_file(path):
    digest = hashlib.md5()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def remote_md5(remote):
    """rclone hash keys vary in casing; accept only one unambiguous MD5 value."""
    hashes = remote.get('Hashes') if isinstance(remote, dict) else None
    if not isinstance(hashes, dict):
        return None
    values = []
    for key, value in hashes.items():
        if isinstance(key, str) and key.casefold() == 'md5':
            if not isinstance(value, str) or not re.fullmatch(r'[0-9a-fA-F]{32}', value):
                return None
            values.append(value.lower())
    return values[0] if values and len(set(values)) == 1 else None


def drive(command, config, timeout=45):
    code, out, err = run(['rclone', '--config', str(config), '--retries', '1',
                          '--low-level-retries', '1', '--contimeout', '10s', '--timeout', '30s',
                          *command], timeout=timeout)
    if code:
        raise Failure('DRIVE_OPERATION_FAILED')
    return out


def owner_only(config, file_id):
    if not isinstance(file_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{5,200}', file_id):
        raise Failure('DRIVE_FILE_ID_MISSING')
    try:
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(config)
        token = json.loads(parser.get('drive', 'token'))['access_token']
        query = urlencode({'fields': 'permissions(type,role,deleted),nextPageToken',
                           'supportsAllDrives': 'true', 'pageSize': '100'})
        request = Request('https://www.googleapis.com/drive/v3/files/' + quote(file_id, safe='')
                          + '/permissions?' + query, headers={'Authorization': 'Bearer ' + token})
        with urlopen(request, timeout=remaining(20)) as response:
            data = json.load(response)
        perms = data.get('permissions') or []
        if (data.get('nextPageToken') or len(perms) != 1 or perms[0].get('type') != 'user'
                or perms[0].get('role') != 'owner' or perms[0].get('deleted')):
            raise Failure('DRIVE_OWNER_ONLY_PRIVACY_NOT_CONFIRMED')
    except Failure:
        raise
    except Exception:
        raise Failure('DRIVE_PRIVACY_CHECK_FAILED') from None


def drive_folder(config, path):
    drive(['mkdir', path], config)
    info = json.loads(drive(['lsjson', path, '--stat'], config))
    identity = info.get('ID')
    if not identity:
        if '/' in path:
            parent, name = path.rsplit('/', 1)
        else:
            parent, name = path.split(':', 1)
            parent += ':'
        rows = json.loads(drive(['lsjson', parent, '--dirs-only', '--max-depth', '1'], config))
        matches = [item for item in rows if item.get('Name') == name]
        if len(matches) == 1:
            identity = matches[0].get('ID')
    owner_only(config, identity)
    return identity


def execute(output):
    global DEADLINE
    DEADLINE = time.monotonic() + 540
    output.mkdir(parents=True, exist_ok=True)
    os.chmod(output, 0o700)
    report = {'status': 'FAIL', 'reason': 'NOT_STARTED', 'media_bytes': 0,
              'capture_test_passed': False, 'drive_test_passed': False,
              'large_6gb_tested': False, 'shift_handover_tested': False,
              'privacy': 'unverified', 'media_preserved_locally': False}
    raw_cookies = os.environ.pop('YT_COOKIES', '')
    raw_config = os.environ.pop('RCLONE_CONFIG_TEXT', '')
    video, channel, url = (os.environ.get(name, '') for name in
                            ('CANARY_VIDEO_ID', 'CANARY_CHANNEL_ID', 'CANARY_SOURCE_URL'))
    cleanup = os.environ.get('CANARY_CLEANUP', 'false').lower() == 'true'
    media = output / 'captured.ts'
    active = output / 'capture.part'
    try:
        if not valid_selection(url, video, channel):
            raise Failure('EXPLICIT_SOURCE_SELECTION_REQUIRED')
        if not raw_cookies.strip() or not raw_config.strip():
            raise Failure('SECRET_BINDING_MISSING')
        if shutil.disk_usage(output).free < 2 * 1024 ** 3:
            raise Failure('INSUFFICIENT_FREE_DISK')
        with tempfile.TemporaryDirectory(prefix='private-canary-') as temporary:
            private = Path(temporary)
            cookie = private / 'probe-cookies.txt'
            config = private / 'rclone.conf'
            private_write(cookie, raw_cookies.lstrip('\ufeff'))
            private_write(config, raw_config)
            raw_cookies = raw_config = ''
            command = [sys.executable, '-m', 'yt_dlp', '--no-config', '--no-cache-dir',
                       '--js-runtimes', 'node', '--socket-timeout', '10', '--retries', '0',
                       '--extractor-retries', '0', '--fragment-retries', '0', '--no-playlist',
                       '--skip-download', '--dump-single-json', '--cookies', str(cookie), url]
            code, out, err = run(command, timeout=60)
            reason = denial(err)
            if reason:
                raise Failure(reason)
            if code:
                raise Failure('METADATA_EXTRACTION_FAILED')
            try:
                info = json.loads(out)
            except (ValueError, TypeError):
                raise Failure('METADATA_INVALID_JSON') from None
            selected = select_format(info, video, channel)
            report.update(video_id=video, channel_id=channel,
                          selected_height=selected.get('height'), metadata_confirmed_live=True)
            # Before media capture, verify a unique destination has no shared access.
            folder = 'drive:RecorderCanary-' + uuid.uuid4().hex
            drive_folder(config, folder)
            report['privacy'] = 'owner_only_permissions_verified'
            ffmpeg = ['ffmpeg', '-nostdin', '-hide_banner', '-v', 'error', '-rw_timeout', '15000000']
            headers = selected.get('http_headers') or info.get('http_headers') or {}
            safe_headers = []
            for name in ('User-Agent', 'Referer', 'Origin'):
                value = headers.get(name)
                if isinstance(value, str) and '\r' not in value and '\n' not in value:
                    safe_headers.append(name + ': ' + value)
            if safe_headers:
                ffmpeg += ['-headers', '\r\n'.join(safe_headers) + '\r\n']
            ffmpeg += ['-i', selected['url'], '-t', '120', '-map', '0:v:0', '-map', '0:a:0',
                       '-c', 'copy', '-f', 'mpegts', '-y', str(active)]
            code, out, err = run(ffmpeg, timeout=180, file_size_limit=1024 ** 3)
            reason = denial(err)
            if reason:
                raise Failure(reason)
            if code:
                raise Failure('CAPTURE_FAILED')
            active.rename(media)
            os.chmod(media, 0o600)
            report['media_bytes'] = media.stat().st_size
            report['media'] = inspect_media(media)
            report['capture_test_passed'] = True
            digest = md5_file(media)
            destination = folder + '/captured.ts'
            drive(['copyto', str(media), destination], config, timeout=180)
            remote = json.loads(drive(['lsjson', destination, '--stat', '--hash'], config))
            identity = remote.get('ID')
            remote_hash = remote_md5(remote)
            if remote.get('Size') != report['media_bytes'] or remote_hash != digest:
                raise Failure('DRIVE_SIZE_OR_HASH_MISMATCH')
            owner_only(config, identity)
            report['drive_receipt'] = {'file_id': identity, 'size_bytes': remote['Size'],
                                       'md5': digest, 'size_match': True, 'md5_match': True,
                                       'privacy': 'owner_only_permissions_verified'}
            report['drive_test_passed'] = True
            report['drive_media_preserved'] = True
            if cleanup:
                drive(['deletefile', destination], config)
                drive(['rmdir', folder], config)
                report['drive_media_preserved'] = False
                report['drive_cleanup'] = 'completed'
            report.update(status='PASS', reason='LIVE_CAPTURE_AND_PRIVATE_DRIVE_VERIFIED')
    except Failure as exc:
        report['reason'] = str(exc)
    except Exception:
        report['reason'] = 'INTERNAL_TEST_ERROR'
    finally:
        raw_cookies = raw_config = ''
        report['media_preserved_locally'] = media.is_file()
        report['partial_media_preserved_locally'] = active.is_file()
        report['partial_media_bytes'] = active.stat().st_size if active.is_file() else 0
        if active.is_file():
            os.chmod(active, 0o600)
        private_write(output / 'canary-report.json', json.dumps(report, indent=2, allow_nan=False))
    return 0 if report['status'] == 'PASS' else 2

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='canary-result')
    args = parser.parse_args()
    raise SystemExit(execute(Path(args.output)))
