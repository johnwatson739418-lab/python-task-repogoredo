"""Run capture plus Drive safety clips; halt conservatively before local storage fills."""
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time

from rolling_backup import SafetyArchive
from publish_recordings import normalize_rclone_config, PublishError

GIB = 1024 ** 3


def pressure_reason(root, archive, *, local_limit=8*GIB, reserve=3*GIB):
    """Original captures are conservatively counted even after safety copies upload.

    This is a bounded local buffer, not a claim of durable storage or a perfect
    remote-coverage calculation. No source is removed to make room.
    """
    if archive.capture_pressure_reason:
        return archive.capture_pressure_reason
    try:
        if shutil.disk_usage(root).free < reserve:
            return 'capture_free_disk_reserve_reached'
        total = sum(p.stat().st_size for p in Path(root).rglob('*')
                    if p.is_file() and not p.is_symlink()
                    and '.upload-work-' not in str(p)
                    and p.suffix.lower() in {'.ts', '.mp4', '.mkv', '.part', '.webm'})
    except OSError:
        return 'capture_storage_measurement_failed'
    if total >= local_limit:
        return 'capture_local_buffer_limit_reached'
    return None


def save_guard_report(root, reason):
    path = Path(root)/'capture-guard.json'
    path.write_text(json.dumps({'halted': bool(reason), 'reason': reason,
                               'original_sources_deleted': False,
                               'unverified_files_deleted': False,
                               'local_storage_is_ephemeral': True}, indent=2))
    path.chmod(0o600)


def process_start_ticks(pid):
    try:
        # comm may contain spaces or parentheses; fields after its final ')'
        # begin at field3. Linux starttime is field22 (offset19 here).
        fields=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()
        return int(fields[19])
    except (OSError,ValueError,IndexError):return None


def kill_registered_groups(directory):
    """Kill only wrapper-owned registered groups whose PID identity still matches."""
    if not directory:return
    for path in Path(directory).glob('*.json'):
        try:
            info=path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:continue
            if info.st_size>4096:continue
            entry=json.loads(path.read_text())
            pid,ticks=entry.get('pid'),entry.get('proc_start_ticks')
            if type(pid)is not int or pid<=1 or type(ticks)is not int or ticks<=0:continue
            if entry.get('kind')not in {'metadata','capture'} or path.name!=f'{pid}-{ticks}.json':continue
            if process_start_ticks(pid)!=ticks or os.getpgid(pid)!=pid:continue
            os.killpg(pid,signal.SIGKILL)
        except (OSError,ValueError,TypeError,AttributeError):continue


def signal_capture_group(child, signum):
    try:
        if type(child.pid)is int and child.pid>1 and os.getpgid(child.pid)==child.pid:
            os.killpg(child.pid,signum)
        else:child.send_signal(signum)
    except ProcessLookupError:pass


def stop_child(child, registry=None):
    if child.poll() is not None:
        return
    signal_capture_group(child, signal.SIGINT)
    try:
        child.wait(timeout=30)
    except subprocess.TimeoutExpired:
        kill_registered_groups(registry)
        signal_capture_group(child, signal.SIGTERM)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            kill_registered_groups(registry)
            signal_capture_group(child, signal.SIGKILL)
            child.wait(timeout=5)


def main():
    os.umask(0o077)
    root = Path(os.environ.get('OUT_DIR', 'streams'))
    root.mkdir(parents=True, exist_ok=True)
    value = os.environ.pop('RCLONE_CONFIG_TEXT', '')
    done = threading.Event()
    with tempfile.TemporaryDirectory(prefix='private-capture-config-') as directory:
        registry=Path(directory)/'owned-processes'
        registry.mkdir(mode=0o700)
        config = os.environ.get('RCLONE_CONFIG')
        if value:
            try:value=normalize_rclone_config(value)
            except PublishError:
                save_guard_report(root, 'drive_config_missing_or_invalid_ini')
                return 2
            config = str(Path(directory)/'rclone.conf')
            Path(config).write_text(value)
            Path(config).chmod(0o600)
            value = ''
        if not config or not Path(config).is_file():
            save_guard_report(root, 'drive_config_missing_capture_not_started')
            return 2
        archive = SafetyArchive(root, config,
                                seconds=float(os.environ.get('SAFETY_CLIP_SECONDS', '300')),
                                overlap=float(os.environ.get('SAFETY_CLIP_OVERLAP', '15')),
                                max_pending_bytes=int(os.environ.get('SAFETY_PENDING_BYTES_LIMIT', str(GIB))))
        local_limit = int(os.environ.get('CAPTURE_LOCAL_BYTES_LIMIT', str(8*GIB)))
        reserve = int(os.environ.get('CAPTURE_FREE_RESERVE_BYTES', str(3*GIB)))
        if local_limit <= 0 or reserve <= 0:
            save_guard_report(root, 'invalid_storage_limits')
            return 2
        reason = pressure_reason(root, archive, local_limit=local_limit, reserve=reserve)
        if reason:
            save_guard_report(root, reason)
            return 3
        worker = threading.Thread(target=archive.run, args=(done,), daemon=True)
        worker.start()
        child_env = dict(os.environ)
        for key in ('GH_TOKEN', 'GITHUB_TOKEN', 'GOFILE_TOKEN', 'GOFILE_FOLDER_ID',
                    'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'RCLONE_CONFIG'):
            child_env.pop(key, None)
        child_env['RECORDER_PROCESS_REGISTRY']=str(registry)
        child = subprocess.Popen([sys.executable, 'recorder.py'], env=child_env, start_new_session=True)
        state = {'reason': None}

        def guard():
            while not done.wait(2):
                current = (state['reason'] or ('capture_backup_worker_stopped' if not worker.is_alive() else
                           pressure_reason(root, archive, local_limit=local_limit, reserve=reserve)))
                if current:
                    state['reason'] = current
                    save_guard_report(root, current)
                    stop_child(child, registry)
                    return

        monitor = threading.Thread(target=guard, daemon=True)
        monitor.start()

        def stop(signum, frame):
            state['reason'] = 'capture_cancelled'
            if child.poll() is None:
                signal_capture_group(child, signal.SIGINT)

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            returncode = child.wait()
        finally:
            done.set()
            monitor.join(timeout=50)
            # Recorder may have been hard-killed or exited while a registered
            # downloader still exists. Identity checks prevent PID-reuse kills.
            kill_registered_groups(registry)
            worker.join(timeout=540)
            archive.stop.set()
            worker.join(timeout=190)
            if worker.is_alive():
                state['reason']=state['reason'] or 'backup_shutdown_deadline_exceeded'
            save_guard_report(root, state['reason'])
        return 3 if state['reason'] else returncode


if __name__ == '__main__':
    raise SystemExit(main())
