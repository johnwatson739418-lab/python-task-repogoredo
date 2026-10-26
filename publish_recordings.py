"""Bounded, checksummed uploads. No raw provider responses or credentials logged."""
import argparse
import configparser
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import threading
import time
import signal
from urllib.parse import quote, urlparse

RELEASE_LIMIT = 2_000_000_000  # Strictly below GitHub's 2 GiB ceiling.
GOFILE_ENDPOINT = 'https://upload.gofile.io/uploadfile'


class PublishError(Exception):
    pass


_provider_lock=threading.Lock()
_provider_processes=set()
_provider_cancelled=threading.Event()


def run_provider(command, *, capture_output=True, text=False, timeout=30, **kwargs):
    """Own provider process groups so shutdown cannot orphan a live upload."""
    if _provider_cancelled.is_set():raise PublishError('provider_shutdown_requested')
    process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,text=text,start_new_session=True,**kwargs)
    with _provider_lock:_provider_processes.add(process)
    try:
        if _provider_cancelled.is_set() and process.poll() is None:
            os.killpg(process.pid,signal.SIGKILL)
        try:
            stdout,stderr=process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            stdout,stderr=process.communicate(timeout=5)
            raise subprocess.TimeoutExpired(command,timeout,output=stdout,stderr=stderr)
        return subprocess.CompletedProcess(command,process.returncode,stdout,stderr)
    finally:
        with _provider_lock:_provider_processes.discard(process)


def cancel_provider_processes():
    _provider_cancelled.set()
    with _provider_lock:processes=list(_provider_processes)
    for process in processes:
        try:
            if process.poll() is None and os.getpgid(process.pid)==process.pid:
                os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass


def normalize_rclone_config(value):
    """Unwrap common copy/file encodings only when the resulting Drive INI validates.

    Credentials are never guessed or replaced. Parser exceptions deliberately
    stay private because they may contain token or client-secret input lines.
    """
    if not isinstance(value, str) or not value.strip():
        raise PublishError('drive_config_missing_or_invalid_ini')
    initial=value.lstrip('\ufeff').strip()
    candidates=[initial]
    try:
        decoded=json.loads(initial)
        if isinstance(decoded, str):candidates.append(decoded.lstrip('\ufeff').strip())
    except (ValueError, TypeError):pass
    for candidate in list(candidates):
        if '\\n' in candidate:
            candidates.append(candidate.replace('\\r\\n','\n').replace('\\n','\n'))
    for candidate in candidates:
        try:
            parser=configparser.ConfigParser(interpolation=None)
            parser.read_string(candidate)
            if not parser.has_section('drive') or parser.get('drive','type',fallback='').strip()!='drive':continue
            token=json.loads(parser.get('drive','token',fallback=''))
            if not isinstance(token,dict) or not isinstance(token.get('refresh_token'),str) or not token['refresh_token'].strip():continue
            return candidate.rstrip('\r\n')+'\n'
        except (configparser.Error, ValueError, TypeError, AttributeError):continue
    raise PublishError('drive_config_missing_or_invalid_ini')


def fingerprints(path, deadline=None):
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            if deadline is not None and time.monotonic() >= deadline:
                raise PublishError('upload_budget_exhausted')
            md5.update(data)
            sha256.update(data)
    return {'bytes': path.stat().st_size, 'md5': md5.hexdigest(), 'sha256': sha256.hexdigest()}


def safe_link(value):
    try:
        parsed = urlparse(value)
        return parsed.scheme == 'https' and parsed.hostname == 'gofile.io' and parsed.path.startswith('/d/')
    except (TypeError, ValueError):
        return False


def validate_gofile(response, metadata):
    info = response.get('data') or {}
    if response.get('status') != 'ok':
        raise PublishError('gofile_rejected')
    if info.get('size') != metadata['bytes'] or str(info.get('md5', '')).lower() != metadata['md5']:
        raise PublishError('gofile_checksum_or_size_mismatch')
    if not safe_link(info.get('downloadPage')):
        raise PublishError('gofile_missing_download_link')
    return info['downloadPage']


def media_ok(path, timeout=15):
    try:
        result = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'stream=codec_type',
                                 '-of', 'json', str(path)], capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0 and any(s.get('codec_type') == 'video'
                                              for s in json.loads(result.stdout).get('streams', []))
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return False


def select_media(root):
    # Include recovered nested chunks and short captures; never discard by size/name alone.
    return sorted((p for p in root.rglob('*') if p.is_file() and not p.is_symlink()
                  and p.suffix.lower() in {'.ts', '.part', '.mp4', '.mkv'}
                  and '.upload-work-' not in str(p) and p.stat().st_size > 0),
                  key=lambda p:(len(p.relative_to(root).parts),str(p)))


def split_for_release(source, destination, limit=RELEASE_LIMIT, timeout=300):
    """Keep source intact; adapt time segmentation until EVERY output fits the byte cap."""
    destination.mkdir(parents=True, exist_ok=True)
    if source.stat().st_size < limit:
        target = destination / 'part-00000.ts'
        os.link(source, target)
        return [target]
    if shutil.disk_usage(destination).free < source.stat().st_size * 1.1 + 64 * 1024 * 1024:
        raise PublishError('insufficient_disk_for_safe_split')
    started = time.monotonic()
    probe = subprocess.run(['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                            '-of', 'json', str(source)], capture_output=True, text=True, timeout=min(15, timeout))
    try:
        duration = float(json.loads(probe.stdout)['format']['duration'])
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError()
    except (ValueError, KeyError):
        raise PublishError('split_duration_unavailable') from None
    seconds = max(.25, min(5400, duration * limit / source.stat().st_size * .65))
    for attempt in range(12):
        remaining = timeout - (time.monotonic() - started)
        if remaining <= 1:
            raise PublishError('split_budget_exhausted')
        for old in destination.glob('part-*.ts'):
            old.unlink()
        process = subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-i', str(source),
                                  '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy',
                                  '-f', 'segment', '-segment_time', str(seconds),
                                  '-reset_timestamps', '1', str(destination / 'part-%05d.ts')],
                                 capture_output=True, timeout=remaining)
        parts = sorted(destination.glob('part-*.ts'))
        if process.returncode != 0 or not parts:
            raise PublishError('split_failed_source_preserved')
        if all(0 < p.stat().st_size < limit for p in parts):
            return parts
        if seconds <= .25:
            break  # A single keyframe group may exceed even a small test limit.
        seconds = max(.25, seconds / 2)
    raise PublishError('cannot_split_below_cap_source_preserved')


class Publisher:
    def __init__(self, root, report, budget=7200, release_fallback=False, endpoint=GOFILE_ENDPOINT):
        if not 10 <= budget <= 7200:
            raise ValueError('Upload budget must be 10..7200 seconds')
        self.root, self.report = Path(root), Path(report)
        self.end = time.monotonic() + budget
        self.release_fallback, self.endpoint = False, None
        self.token = ''
        self.folder = ''
        self.drive_config = os.environ.get('RCLONE_CONFIG', '')
        self.drive_prefix = os.environ.get('GDRIVE_REMOTE', 'drive:YouTubeRecordings')
        self.results = []
        self.session_url = None
        try:self.rolling=json.loads((self.root/'rolling-backup.json').read_text())
        except (OSError,ValueError):self.rolling={}
        self.category = 'recordings'
        try:
            self.prior = json.loads(self.report.read_text()).get('files',[])
        except (OSError,ValueError,TypeError):
            self.prior = []

    def remaining(self):
        value = self.end - time.monotonic()
        if value < 1:
            raise PublishError('upload_budget_exhausted')
        return value

    def save(self):
        self.report.parent.mkdir(parents=True, exist_ok=True)
        value = {'files': self.results, 'all_uploaded': bool(self.results) and
                 all(r.get('uploaded') for r in self.results), 'session_url':self.session_url,
                 'media_status':'media_present' if self.results else 'no_media',
                 'unique_uploaded_files':sum(bool(r.get('uploaded')) and not r.get('duplicate_of') for r in self.results),
                 'safety_backup':{k:self.rolling.get(k) for k in ('state','uploaded_clips','pending_clips','last_error')}}
        temporary = self.report.with_suffix('.tmp')
        temporary.write_text(json.dumps(value, indent=2))
        os.replace(temporary, self.report)

    def gofile(self, source, metadata):
        seconds = min(900, self.remaining())
        if seconds < 10:
            raise PublishError('gofile_upload_budget_exhausted')
        token = os.environ.get('GOFILE_TOKEN', '').strip()
        folder_id = os.environ.get('GOFILE_FOLDER_ID', '').strip()
        command = [
            'curl', '--silent', '--show-error', '--fail-with-body',
            '--connect-timeout', '20', '--max-time', str(int(seconds)),
            '--form', f'file=@{source}'
        ]
        if token:
            command += ['--form', f'token={token}']
        if folder_id:
            command += ['--form', f'folderId={folder_id}']
        command.append(GOFILE_ENDPOINT)
        try:
            result = run_provider(command, capture_output=True, text=True, timeout=min(seconds + 10, self.remaining()))
        except (subprocess.TimeoutExpired, OSError) as e:
            raise PublishError(f'gofile_transport_error: {e}')
        if result.returncode != 0:
            raise PublishError(f'gofile_curl_failed_rc_{result.returncode}')
        try:
            payload = json.loads(result.stdout or '{}')
        except json.JSONDecodeError:
            raise PublishError('gofile_invalid_json_response')
        if payload.get('status') != 'ok':
            raise PublishError(f'gofile_rejected_status_{payload.get("status")}')
        data = payload.get('data') or {}
        link = data.get('downloadPage') or data.get('link') or ''
        if not (isinstance(link, str) and link.startswith('https://')):
            raise PublishError('gofile_missing_download_link')
        file_id = data.get('fileId', '') or data.get('id', '')
        print(f'GOFILE_FALLBACK_SUCCESS: {source.name} -> {link}', flush=True)
        return {
            'provider': 'gofile',
            'url': link,
            'download_page': link,
            'file_id': file_id,
            'verification': 'status_ok_and_link',
            'bytes': metadata.get('bytes', source.stat().st_size)
        }

    def upload_candidate(self, source, metadata):
        errors = []
        # Primary: Google Drive
        try:
            return self.google_drive(source, metadata)
        except Exception as drive_err:
            errors.append(f"drive: {drive_err}")
            print(f"GOOGLE_DRIVE_FAILED for {source.name}: {drive_err}; falling back to Gofile...", flush=True)

        # Fallback: Gofile
        try:
            return self.gofile(source, metadata)
        except Exception as gofile_err:
            errors.append(f"gofile: {gofile_err}")
            print(f"GOFILE_FALLBACK_FAILED for {source.name}: {gofile_err}", flush=True)

        raise PublishError("all_upload_providers_failed: " + "; ".join(errors))

    def google_drive(self, source, metadata):
        if not self.drive_config or not Path(self.drive_config).is_file():
            raise PublishError('google_drive_not_configured')
        if not shutil.which('rclone'):
            raise PublishError('rclone_missing')
        seconds = min(5400, self.remaining())
        if seconds < 1:
            raise PublishError('drive_upload_budget_exhausted')
        owner_repo = os.environ.get('GITHUB_REPOSITORY', 'local').replace('/', '_')
        run = os.environ.get('GITHUB_RUN_ID', 'local')
        name = re.sub(r'[^A-Za-z0-9_.-]+', '_', source.name)
        session = os.environ.get('RECORDING_SESSION_ID',run)
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',session):
            raise PublishError('invalid_recording_session_id')
        window = os.environ.get('RECORDING_SHIFT_INDEX','0')
        if not window.isdigit():raise PublishError('invalid_recording_shift_index')
        session_path = self.drive_prefix.rstrip('/')+'/'+owner_repo+'/'+session
        remote = session_path+'/shift-'+window+'/'+self.category+'/'+metadata['sha256'][:20]+'-'+name
        base = ['rclone', '--config', self.drive_config]
        def remote_info(allow_missing=False):
            info = run_provider(base + ['lsjson', remote, '--stat', '--hash',
                                         '--retries', '1', '--low-level-retries', '1',
                                         '--contimeout', '10s', '--timeout', '20s'],
                                  capture_output=True, text=True, timeout=min(30, self.remaining()))
            if info.returncode:
                error = (info.stderr or '').lower()
                if allow_missing and info.returncode in (3, 4) and 'not found' in error:
                    return None
                raise PublishError('google_drive_metadata_failed')
            details = json.loads(info.stdout)
            if not isinstance(details, dict):
                raise PublishError('google_drive_invalid_metadata')
            return details

        def matches(details):
            if not isinstance(details, dict): return False
            raw_hashes = details.get('Hashes', {})
            if not isinstance(raw_hashes, dict): return False
            hashes = {str(k).lower(): str(v).lower() for k, v in raw_hashes.items()}
            return details.get('Size') == metadata['bytes'] and hashes.get('md5') == metadata['md5']

        details = remote_info(allow_missing=True)
        reconciled = matches(details)
        if details is not None and not reconciled:
            raise PublishError('google_drive_existing_object_mismatch_source_preserved')
        copied = None
        copy_timed_out = False
        if not reconciled:
            try:
                copied = run_provider(base + ['copyto', str(source), remote, '--checksum',
                                        '--retries', '1', '--low-level-retries', '1',
                                        '--contimeout', '15s', '--timeout', '60s',
                                        '--drive-chunk-size', '32M', '--transfers', '1', '--checkers', '1'],
                                capture_output=True, text=True, timeout=min(seconds, max(1, self.remaining()-35)))
            except subprocess.TimeoutExpired:
                copy_timed_out = True
            # A failed copy acknowledgment is ambiguous: reconcile rather than
            # blindly publishing again. No local source is ever removed here.
            details = remote_info()
            if not matches(details):
                raise PublishError('google_drive_checksum_or_size_mismatch')
        file_id = details.get('ID', '')
        link = 'https://drive.google.com/file/d/' + file_id + '/view' if re.fullmatch(r'[A-Za-z0-9_-]+', file_id) else remote
        return {'provider': 'google_drive', 'url': link, 'remote_path': remote,
                'session_path':session_path, 'file_id':file_id,
                'verification': 'remote_size_and_md5',
                'reconciled_existing': reconciled,
                'reconciled_after_ambiguous_ack': bool(copy_timed_out or (copied is not None and copied.returncode)),
                'sharing': 'permissions_not_modified; inherited_acl_not_verified'}

    def release(self, *args, **kwargs):
        raise PublishError('public_backup_disabled')

    def verified_prior(self, metadata):
        # A cached success is reused only after checking the remote Drive object again.
        for prior in self.prior:
            if not isinstance(prior,dict) or not prior.get('uploaded') or prior.get('sha256')!=metadata['sha256']:
                continue
            for receipt in prior.get('receipts',[]):
                if receipt.get('provider')!='google_drive' or not self.drive_config:
                    continue
                remote=receipt.get('remote_path','')
                if not remote.startswith(self.drive_prefix.rstrip('/')+'/'):
                    continue
                try:
                    info=run_provider(['rclone','--config',self.drive_config,'lsjson',remote,'--stat','--hash'],
                                        capture_output=True,text=True,timeout=min(30,self.remaining()))
                    data=json.loads(info.stdout)
                    hashes={k.lower():str(v).lower() for k,v in data.get('Hashes',{}).items()}
                    if info.returncode==0 and data.get('Size')==metadata['bytes'] and hashes.get('md5')==metadata['md5']:
                        return prior
                except (OSError,ValueError,subprocess.TimeoutExpired):
                    pass
        return None

    def resolve_session_url(self):
        if not self.drive_config:return
        safety_receipts=[c.get('receipt',{}) for c in self.rolling.get('clips',[]) if c.get('status')=='uploaded']
        attempted=set()
        for record in self.results + [{'receipts':safety_receipts}]:
            for receipt in record.get('receipts',[]):
                path=receipt.get('session_path')
                if receipt.get('provider')!='google_drive' or not path or path in attempted:continue
                attempted.add(path)
                try:
                    result=run_provider(['rclone','--config',self.drive_config,'lsjson',path,'--stat'],
                                          capture_output=True,text=True,timeout=min(20,self.remaining()))
                    data=json.loads(result.stdout);folder=data.get('ID','')
                    # Older rclone synthesizes --stat directory entries without IDs.
                    if not folder:
                        parent,name=path.rsplit('/',1)
                        listing=run_provider(['rclone','--config',self.drive_config,'lsjson',parent,'--dirs-only','--max-depth','1'],
                                               capture_output=True,text=True,timeout=min(20,self.remaining()))
                        entries=json.loads(listing.stdout)
                        data=next((entry for entry in entries if entry.get('Name')==name and entry.get('IsDir')), {}) if listing.returncode==0 else {}
                        folder=data.get('ID','')
                    if data.get('IsDir') and re.fullmatch(r'[A-Za-z0-9_-]+',folder):
                        self.session_url='https://drive.google.com/drive/folders/'+folder
                        return
                except (OSError,ValueError,PublishError,subprocess.TimeoutExpired):pass

    def run(self):
        candidates = select_media(self.root)
        self.save()
        seen = {}
        for path in candidates:
            record = {'file': str(path.relative_to(self.root)), 'uploaded': False, 'receipts': []}
            self.results.append(record)
            self.save()
            try:
                self.remaining()
                if not media_ok(path, timeout=min(15, self.remaining())):
                    raise PublishError('unreadable_video_preserved')
                metadata = fingerprints(path, self.end)
                self.remaining()
                record.update(metadata)
                previous=seen.get(metadata['sha256'])
                if previous and previous.get('uploaded'):
                    record.update(uploaded=True,receipts=previous['receipts'],duplicate_of=previous['file'])
                    self.save()
                    print('DEDUPLICATED '+record['file'],flush=True)
                    continue
                previous=self.verified_prior(metadata)
                if previous:
                    record.update(uploaded=True,receipts=previous['receipts'],resumed=True)
                    seen[metadata['sha256']]=record
                    self.save()
                    continue
                # Safe ASCII multipart filename; hard-link avoids another multi-GB copy.
                with tempfile.TemporaryDirectory(prefix='.upload-work-', dir=path.parent) as directory:
                    work = Path(directory)
                    staged = work / (metadata['sha256'][:20] + '.ts')
                    os.link(path, staged)
                    # Primary: Drive, Fallback: Gofile
                    receipts = [self.upload_candidate(path, metadata)]
                    record.update(uploaded=True, receipts=receipts)
                    seen[metadata["sha256"]]=record
                    self.save()
                # Keep source on disk even after upload; lifecycle cleanup is explicit.
                print('UPLOADED ' + record['file'], flush=True)
            except (PublishError, OSError, ValueError, subprocess.TimeoutExpired) as error:
                record['error'] = str(error) if isinstance(error, PublishError) else type(error).__name__
                print('UPLOAD_FAILED ' + record['file'] + ': ' + record['error'], flush=True)
            self.save()
        self.resolve_session_url()
        self.save()
        if not candidates:
            print('NO_RECORDING: no media files; this is not capture success', flush=True)
        return 0 if self.results and all(r['uploaded'] for r in self.results) else 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='streams')
    parser.add_argument('--report', default='upload-report.json')
    parser.add_argument('--budget', type=int, default=7200)
    parser.add_argument('--release-fallback', action='store_true')
    args = parser.parse_args()
    raise SystemExit(Publisher(args.root, args.report, args.budget, args.release_fallback).run())


if __name__ == '__main__':
    main()
