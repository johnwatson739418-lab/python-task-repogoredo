# God-level YouTube Live Recorder

Production-ready multi-shift YouTube live stream recorder for GitHub Actions.

## Features

- **Official upstream yt-dlp clients only**: `android_vr, android, ios, mweb, tv_embedded`
- **Cookies fully optional** – never hard-exits if missing
- **Upload order (locked)**:
  1. Google Drive via rclone (PRIMARY – permanent storage)
  2. Gofile (SECONDARY)
  3. GitHub Releases (TERTIARY, auto-split < 1.9 GB)
- **Manual channel override** – record any channel from a single repo via `workflow_dispatch`
- **Shift chaining** – long sessions automatically split under GitHub’s 6 h limit
- **Real-time Telegram** – LIVE detected, fallback mode, upload complete, failures
- Resilient HLS → `.ts` capture with stall detection + client rotation

## Quick Start (one repo)

1. Fork / push this code to a repository.
2. Add secrets (Settings → Secrets and variables → Actions):

| Secret | Required | Description |
|--------|----------|-------------|
| `RCLONE_CONFIG` | Recommended | Full rclone.conf content (auto-detects remote name e.g. `[drive]` or `[gdrive]`) |
| `RCLONE_REMOTE` | Optional | Default `drive:YouTubeRecordings` (auto-corrected if section name differs) |
| `GOFILE_TOKEN` | Optional | Gofile API token (guest also works) |
| `YT_COOKIES` | Optional | Netscape cookies.txt content |
| `TELEGRAM_BOT_TOKEN` | Optional | Bot token |
| `TELEGRAM_CHAT_ID` | Optional | Chat / group ID |

3. Edit `channels.json` **or** leave it and use the manual override.

4. Run workflow:
   - Actions → God-level YouTube Live Recorder → Run workflow
   - Optional: put `@SomeStreamer` in **channel_handle**
   - Set `total_hours` (e.g. `8` or `12`)

## Manual override (phone-friendly)

You can leave `channels.json` alone and just type any handle in the `channel_handle` input when starting the workflow. No need to edit 16 repos from your phone.

## Multi-repo strategy (optional)

Keep the same code in 16 repos and put one permanent channel in each `channels.json`.  
Or use one “master” repo and always pass `channel_handle` manually / via API.

## rclone setup (Google Drive primary)

```bash
rclone config
# create remote named "gdrive" (or whatever you put in RCLONE_REMOTE)
rclone about gdrive:
```

Copy the entire `~/.config/rclone/rclone.conf` into the `RCLONE_CONFIG` secret.

## Notes

- A 5-hour 720p/1080p stream commonly produces 6–12 GB depending on bitrate.
- Each shift is limited to ~5 hours for safety under the GitHub Actions job timeout.
- The recorder never fails the whole job just because cookies are missing.
- If Drive is down, it automatically falls back to Gofile, then GitHub Releases.

---

Built for reliability on datacenter IPs (GitHub Actions / Azure).
