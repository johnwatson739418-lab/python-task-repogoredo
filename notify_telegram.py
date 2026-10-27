#!/usr/bin/env python3
"""
Real-time Telegram notifications for God-level YouTube Live Recorder.
Events: live_detected, fallback_mode, upload_done, shift_failed, info
"""

from __future__ import annotations

import os
import time
import requests
from typing import Optional

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
ENABLED = bool(BOT_TOKEN and CHAT_ID)


def _send(text: str, parse_mode: str = "HTML", disable_preview: bool = True) -> bool:
    if not ENABLED:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text[:4000],
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_preview,
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                return True
            time.sleep(1.5 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return False


def live_detected(handle: str, video_id: str, client: str) -> None:
    msg = (
        f"🔴 <b>LIVE DETECTED</b>\n"
        f"Channel: <code>{handle}</code>\n"
        f"Video ID: <code>{video_id}</code>\n"
        f"Client: <code>{client}</code>\n"
        f"https://www.youtube.com/watch?v={video_id}"
    )
    _send(msg)


def fallback_mode(handle: str, reason: str) -> None:
    msg = (
        f"⚠️ <b>FALLBACK MODE</b>\n"
        f"Channel: <code>{handle}</code>\n"
        f"Reason: {reason}\n"
        f"Using pure mobile clients (no cookies)."
    )
    _send(msg)


def upload_done(handle: str, video_id: str, size_gb: float, links: list[str]) -> None:
    links_txt = "\n".join(f"• {u}" for u in links[:8]) if links else "• (no public links)"
    msg = (
        f"✅ <b>UPLOAD COMPLETE</b>\n"
        f"Channel: <code>{handle}</code>\n"
        f"Video: <code>{video_id}</code>\n"
        f"Size: <b>{size_gb:.2f} GB</b>\n\n"
        f"{links_txt}"
    )
    _send(msg)


def shift_failed(handle: str, reason: str, shift_index: int = 0) -> None:
    msg = (
        f"❌ <b>SHIFT FAILED</b>\n"
        f"Channel: <code>{handle}</code>\n"
        f"Shift: #{shift_index}\n"
        f"Reason: {reason}"
    )
    _send(msg)


def info(text: str) -> None:
    _send(f"ℹ️ {text}")


if __name__ == "__main__":
    info("Telegram notify module test OK")
