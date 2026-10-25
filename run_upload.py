"""Write an optional private rclone config only for the upload process lifetime."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    value = os.environ.pop('RCLONE_CONFIG_TEXT', '')
    with tempfile.TemporaryDirectory(prefix='private-upload-config-') as d:
        if value:
            config = Path(d)/'rclone.conf'
            config.write_text(value); config.chmod(0o600); value = ''
            os.environ['RCLONE_CONFIG'] = str(config)
        result = subprocess.run([sys.executable, 'publish_recordings.py', *sys.argv[1:]])
        return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
