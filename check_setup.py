"""Report secret binding presence and syntax only, never values or cookie hashes."""
import json
import os
import configparser
from pathlib import Path


def main():
    value = os.environ.pop('YT_COOKIES', '')
    rows = []
    for line in value.splitlines():
        if line.startswith('#HttpOnly_'):
            line = line[len('#HttpOnly_'):]
        if not line or line.startswith('#'):
            continue
        fields = line.split('\t')
        domain = fields[0].lstrip('.').lower()
        if len(fields) >= 7 and (domain == 'youtube.com' or domain.endswith('.youtube.com')) and fields[-1]:
            rows.append(fields)
    drive_text = os.environ.pop('RCLONE_CONFIG_TEXT', '')
    drive_valid = False
    own_client = False
    try:
        conf = configparser.ConfigParser(interpolation=None)
        conf.read_string(drive_text)
        token = json.loads(conf.get('drive', 'token', fallback='{}'))
        own_client = bool(conf.get('drive','client_id',fallback='').strip()) and bool(conf.get('drive','client_secret',fallback='').strip())
        drive_valid = conf.get('drive','type',fallback='') == 'drive' and bool(token.get('refresh_token')) and own_client
    except (configparser.Error, ValueError, AttributeError):
        pass
    drive_text = ''
    result = {'repository': os.environ.get('GITHUB_REPOSITORY'), 'cookie_binding_present': bool(value),
              'cookie_format_valid': bool(rows), 'youtube_authentication_tested': False,
              'drive_config_format_valid': drive_valid, 'google_drive_upload_tested': False,
              'drive_own_client_configured': own_client,
              'release_backup_default': True, 'overnight_workload_tested': False}
    value = ''; rows.clear()
    Path('setup-check.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result))
    if not result['cookie_format_valid']:
        raise SystemExit('YT_COOKIES is missing or not a recognizable YouTube cookie file; set the private repository secret.')
    if not drive_valid:
        raise SystemExit('Google Drive authorization with your own OAuth client is not configured; add private RCLONE_CONFIG secret. Shared client retirement affects 2026 setups.')


if __name__ == '__main__':
    main()
