"""Make a private Drive config for one Drive-only upload process lifetime."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from publish_recordings import normalize_rclone_config, PublishError


def confirmed_offline(root):
    root=Path(root)
    summaries=list(root.rglob('*_summary.json'))
    if not summaries:return False
    # Any media-like file, including an empty/incomplete one, forbids exemption.
    if any(p.is_file() and p.suffix.lower() in {'.ts','.mp4','.mkv','.part','.webm'} for p in root.rglob('*')):return False
    try:
        guard=root/'capture-guard.json'
        if guard.exists() and json.loads(guard.read_text()).get('halted') is not False:return False
        for path in summaries:
            item=json.loads(path.read_text(encoding='utf-8-sig'))
            counts=item.get('probe_observations')
            if not isinstance(counts,dict):return False
            if any(type(counts.get(k)) is not int or counts[k]<0 for k in ('live','offline','unknown')):return False
            if str(item.get('observation_status','')).casefold()!='offline':return False
            if str(item.get('status','')).casefold()!='offline':return False
            if counts['offline']<=0 or counts['unknown'] or counts['live']:return False
            if item.get('failure_reason') or item.get('auth_blocked') or item.get('fatal_errors') or item.get('fatal_errors_count'):return False
            if type(item.get('saved_file_count')) is not int or item['saved_file_count']!=0 or item.get('media_saved') is not False:return False
        return True
    except (OSError,ValueError,TypeError,AttributeError):return False


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser()
    parser.add_argument('--allow-confirmed-offline',action='store_true')
    parser.add_argument('--root',default='streams')
    parser.add_argument('--report',default='upload-report.json')
    parser.add_argument('--budget',type=int,default=7200)
    parser.add_argument('--release-fallback',action='store_true')
    args=parser.parse_args()
    if args.allow_confirmed_offline and confirmed_offline(args.root):
        report=Path(args.report);report.parent.mkdir(parents=True,exist_ok=True)
        value={'files':[],'all_uploaded':False,'publication_status':'not_applicable',
               'media_status':'confirmed_offline','offline_evidence_verified':True}
        temporary=report.with_suffix('.tmp');temporary.write_text(json.dumps(value,indent=2));temporary.chmod(0o600);os.replace(temporary,report)
        print('CONFIRMED_OFFLINE: publication not applicable; no media uploaded')
        return 0
    value = os.environ.pop('RCLONE_CONFIG_TEXT', '')
    child_env = os.environ.copy()
    for key in ('GH_TOKEN', 'GITHUB_TOKEN',
                'TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID', 'YT_COOKIES'):
        child_env.pop(key, None)
    with tempfile.TemporaryDirectory(prefix='private-upload-config-') as directory:
        if value:
            try:value=normalize_rclone_config(value)
            except PublishError:
                print('UPLOAD_FAILED: drive_config_missing_or_invalid_ini')
                return 2
            config = Path(directory)/'rclone.conf'
            config.write_text(value)
            config.chmod(0o600)
            value = ''
            child_env['RCLONE_CONFIG'] = str(config)
        command=[sys.executable,'publish_recordings.py','--root',args.root,'--report',args.report,'--budget',str(args.budget)]
        result = subprocess.run(command, env=child_env)
        return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
