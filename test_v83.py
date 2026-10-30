import unittest
from unittest.mock import patch, MagicMock
import zipfile
import os
import json
import socket
import threading
import recorder


class TestRecorderV86Final(unittest.TestCase):
    def setUp(self):
        recorder.NON_LIVE_VOD_CACHE.clear()
        recorder.PROBE_STATS.update({"clean_offline": 0, "blocked": 0, "error": 0, "live_hits": 0})
        recorder.YTDLP_PROBE_COOLDOWN_UNTIL = 0.0
        recorder.TARGET["channel_id"] = "UCNty54b8Vzmk-9BRYfF0EVQ"
        recorder.SUMMARY["blocked_by_youtube"] = False

    def test_01_claude_125kb_offset_ytinitialdata(self):
        padding = "A" * 125_000
        large_idle_html = f'<html><head>{padding}</head><body><script>var ytInitialData = {{"externalId":"UCNty54b8Vzmk-9BRYfF0EVQ"}};</script></body></html>'
        state, details = recorder.inspect_youtube_html(large_idle_html)
        self.assertEqual(state, "NOT_LIVE")
        self.assertIsNone(details)

    def test_02_chatgpt_upcoming_wins_over_isLiveNow_false_and_endTimestamp(self):
        html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"UPCOMING123","channelId":"UCNty54b8Vzmk-9BRYfF0EVQ","isLive":false,"isUpcoming":true},'
            '"microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"isLiveNow":false,"endTimestamp":"2026-10-29T00:00:00Z"}}}'
            '};</script></html>'
        )
        state, details = recorder.inspect_youtube_html(html)
        self.assertEqual(state, "UPCOMING")
        self.assertFalse(details["ended"])

        with patch.object(recorder, "fetch_text", return_value=html), \
             patch.object(recorder, "run_ytdlp_json") as mock_ytdlp:
            res = recorder.verify_live_id("UPCOMING123", "rss_feed")
            self.assertIsNone(res)
            self.assertNotIn("UPCOMING123", recorder.NON_LIVE_VOD_CACHE)
            self.assertEqual(mock_ytdlp.call_count, 0)

    def test_03_chatgpt_isLive_true_not_overridden_by_endTimestamp_when_liveNow_missing(self):
        html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"LSClhQ75Ot8","channelId":"UCNty54b8Vzmk-9BRYfF0EVQ","isLive":true,"isUpcoming":false},'
            '"microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"endTimestamp":"2026-10-29T00:00:00Z"}}}'
            '};</script></html>'
        )
        state, details = recorder.inspect_youtube_html(html)
        self.assertEqual(state, "LIVE")
        self.assertFalse(details["ended"])

    def test_04_chatgpt_isLiveNow_true_wins_over_endTimestamp(self):
        html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"LSClhQ75Ot8","channelId":"UCNty54b8Vzmk-9BRYfF0EVQ","isLive":false},'
            '"microformat":{"playerMicroformatRenderer":{"liveBroadcastDetails":{"isLiveNow":true,"endTimestamp":"2026-10-29T00:00:00Z"}}}'
            '};</script></html>'
        )
        state, details = recorder.inspect_youtube_html(html)
        self.assertEqual(state, "LIVE")
        self.assertTrue(details["isLive"])
        self.assertFalse(details["ended"])

    def test_05_blocked_live_not_masked_by_home_not_live(self):
        bot_html = '<html><body>Sign in to confirm you\'re not a bot. g-recaptcha</body></html>'
        home_html = '<html><script>var ytInitialData = {"externalId":"UCNty54b8Vzmk-9BRYfF0EVQ"};</script></html>'

        def fake_fetch(url):
            return bot_html if url.endswith("/live") else home_html

        with patch.object(recorder, "fetch_text", side_effect=fake_fetch), \
             patch.object(recorder, "run_ytdlp_json", return_value=None):
            res = recorder.probe_fast()
            self.assertIsNone(res)
            self.assertEqual(recorder.PROBE_STATS["blocked"], 1)
            self.assertEqual(recorder.PROBE_STATS["clean_offline"], 0)
            self.assertTrue(recorder.SUMMARY["blocked_by_youtube"])

    def test_06_wrong_channel_idle_html_is_rejected_as_error(self):
        wrong_channel_html = '<html><script>var ytInitialData = {"externalId":"UC-JQRYHht4tLKpiChum6GnA"};</script></html>'
        state, _ = recorder.inspect_youtube_html(wrong_channel_html)
        self.assertEqual(state, "ERROR")

    def test_07_confirm_still_live_preserves_unknown(self):
        bot_html = '<html><body>Sign in to confirm you\'re not a bot. g-recaptcha</body></html>'
        with patch.object(recorder, "fetch_text", return_value=bot_html), \
             patch.object(recorder, "run_ytdlp_json", return_value=None):
            self.assertEqual(recorder.check_live_state("LSClhQ75Ot8"), "UNKNOWN")
            self.assertTrue(recorder.confirm_still_live("LSClhQ75Ot8"))

    def test_08_rss_vod_cached_and_zero_ytdlp(self):
        vod_html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"dQw4w9WgXcQ","channelId":"UCNty54b8Vzmk-9BRYfF0EVQ","isLive":false}'
            '};</script></html>'
        )
        with patch.object(recorder, "fetch_text", return_value=vod_html) as mock_fetch, \
             patch.object(recorder, "run_ytdlp_json") as mock_ytdlp:
            self.assertIsNone(recorder.verify_live_id("dQw4w9WgXcQ", "rss_feed"))
            self.assertIn("dQw4w9WgXcQ", recorder.NON_LIVE_VOD_CACHE)
            self.assertIsNone(recorder.verify_live_id("dQw4w9WgXcQ", "rss_feed"))
            self.assertEqual(mock_fetch.call_count, 1)
            self.assertEqual(mock_ytdlp.call_count, 0)

    def test_09_process_reaped_even_if_process_group_already_gone(self):
        class FakeProcess:
            pid = 43210
            waited = False
            def poll(self): return None
            def wait(self, timeout=None):
                self.waited = True
                return -15
        fake = FakeProcess()
        with patch("recorder.os.name", "posix"), patch("recorder.os.killpg", create=True, side_effect=ProcessLookupError):
            recorder.stop_proc(fake)
        self.assertTrue(fake.waited)

    def test_10_all_16_repos_mapped_and_no_artificial_auth_blockers(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cfg_file = os.path.join(base_dir, "streamers_16_config.json")
        with open(cfg_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        self.assertEqual(len(cfg), 16)
        yml_path = os.path.join(base_dir, ".github", "workflows", "record.yml")
        if not os.path.exists(yml_path):
            yml_path = os.path.join(base_dir, "record.yml")
        with open(yml_path, "r", encoding="utf-8") as f:
            yml = f.read()
        for item in cfg:
            self.assertIn(item["repo"], yml)
        self.assertNotIn("RECORDING_AUTHORIZED", yml)
        self.assertNotIn("PUBLIC_REDISTRIBUTION_AUTHORIZED", yml)

    def test_11_warp_proxy_socks5_handshake_verification(self):
        """warp_proxy_url() must require a real SOCKS5 \x05\x00 handshake, not just env var or dead TCP port."""
        os.environ["WARP_PROXY"] = "socks5://127.0.0.1:40000"
        # When port 40000 is not listening -> must return "" even though WARP_PROXY env is set
        self.assertEqual(recorder.warp_proxy_url(), "")

        # Mock a healthy SOCKS5 server returning \x05\x00
        mock_sock = MagicMock()
        mock_sock.__enter__.return_value = mock_sock
        mock_sock.recv.return_value = b"\x05\x00"
        with patch("recorder.socket.create_connection", return_value=mock_sock):
            self.assertEqual(recorder.warp_proxy_url(), "socks5://127.0.0.1:40000")

        # Mock a port that accepts TCP but returns garbage instead of SOCKS5 \x05\x00 -> must return ""
        mock_sock.recv.return_value = b"HTTP"
        with patch("recorder.socket.create_connection", return_value=mock_sock):
            self.assertEqual(recorder.warp_proxy_url(), "")

    def test_12_fetch_text_falls_back_to_warp_on_any_unknown_challenge_page(self):
        """fetch_text() uses inspect_youtube_html() so any unknown challenge page (ERROR or BLOCKED) triggers WARP fallback."""
        challenge_html = "<html><body>Please complete security challenge to continue</body></html>"
        valid_html = '<html><script>var ytInitialData = {"externalId":"UCNty54b8Vzmk-9BRYfF0EVQ"};</script></html>'

        def fake_curl(_bin, _url, use_proxy=False):
            return valid_html if use_proxy else challenge_html

        with patch.object(recorder, "_run_curl_fetch", side_effect=fake_curl) as mock_curl, \
             patch.object(recorder, "warp_proxy_url", return_value="socks5://127.0.0.1:40000"):
            out = recorder.fetch_text("https://www.youtube.com/@floxiclip/live")
            self.assertEqual(out, valid_html)
            self.assertEqual(mock_curl.call_count, 2)

    def test_13_check_live_state_strict_ended_vs_unknown(self):
        """check_live_state() returns ENDED only when videoId + channelId match and ended is True; probe failure is UNKNOWN."""
        # Probe failure / empty HTML -> UNKNOWN (never ENDED!)
        with patch.object(recorder, "fetch_text", return_value=""), \
             patch.object(recorder, "run_ytdlp_json", return_value=None):
            self.assertEqual(recorder.check_live_state("LSClhQ75Ot8"), "UNKNOWN")

        # Wrong channel ID -> UNKNOWN (never ENDED!)
        wrong_cid_html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"LSClhQ75Ot8","channelId":"UC-JQRYHht4tLKpiChum6GnA","isLive":false}'
            '};</script></html>'
        )
        with patch.object(recorder, "fetch_text", return_value=wrong_cid_html), \
             patch.object(recorder, "run_ytdlp_json", return_value=None):
            self.assertEqual(recorder.check_live_state("LSClhQ75Ot8"), "UNKNOWN")

        # Confirmed ended broadcast for exact videoId + channelId -> ENDED
        ended_html = (
            '<html><script>var ytInitialPlayerResponse = {'
            '"videoDetails":{"videoId":"LSClhQ75Ot8","channelId":"UCNty54b8Vzmk-9BRYfF0EVQ","isLive":false}'
            '};</script></html>'
        )
        with patch.object(recorder, "fetch_text", return_value=ended_html):
            self.assertEqual(recorder.check_live_state("LSClhQ75Ot8"), "ENDED")

    def test_14_rate_limit_vs_access_restricted_separation(self):
        """403 / content unavailable are classified as access_restricted, not rate_limited."""
        state_403 = {"botcheck": False, "no_formats": False, "rate_limited": False, "access_restricted": True, "not_live": False, "normal_exit": False}
        self.assertEqual(recorder.classify_output(state_403), "access_restricted")
        state_rl = {"botcheck": False, "no_formats": False, "rate_limited": True, "access_restricted": False, "not_live": False, "normal_exit": False}
        self.assertEqual(recorder.classify_output(state_rl), "rate_limited")

    def test_15_unverified_not_claimed_as_verified_offline(self):
        """Test 13/15 audit requirement: clean_offline=0, blocked=0, error=0 must be UNVERIFIED via actual production function."""
        summary_unverified = {
            "probe_stats": {"clean_offline": 0, "blocked": 0, "error": 0},
            "blocked_by_youtube": False,
            "downloader_rate_limited": False,
            "final_size_mb": 0.0,
            "chunk_count": 0,
        }
        status = recorder.classify_run_status(summary_unverified, media_candidates=[])
        self.assertEqual(status, "UNVERIFIED")
        self.assertNotEqual(status, "NO_LIVE_CONFIRMED")

        summary_confirmed_offline = {
            "probe_stats": {"clean_offline": 3, "blocked": 0, "error": 0},
            "blocked_by_youtube": False,
            "downloader_rate_limited": False,
            "final_size_mb": 0.0,
            "chunk_count": 0,
        }
        status_offline = recorder.classify_run_status(summary_confirmed_offline, media_candidates=[])
        self.assertEqual(status_offline, "NO_LIVE_CONFIRMED")

    def test_16_record_pass_requires_non_empty_media_file(self):
        """Test 14/16 audit requirement: test actual RECORD_PASS implementation with real temporary files."""
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            # Case 1: Empty folder
            valid = recorder.validate_finalized_recordings(tmpdir)
            self.assertEqual(valid, [])
            status = recorder.classify_run_status({"chunk_count": 0, "final_size_mb": 0.0}, tmpdir)
            self.assertNotEqual(status, "RECORD_PASS")

            # Case 2: Only .part file exists (e.g. 2 MB unfinished download)
            part_file = os.path.join(tmpdir, "test_stream.mp4.part")
            with open(part_file, "wb") as f:
                f.write(b"0" * 2_000_000)
            valid = recorder.validate_finalized_recordings(tmpdir)
            self.assertEqual(valid, [], ".part files must not count as finalized recordings")
            status = recorder.classify_run_status({"chunk_count": 1, "final_size_mb": 2.0}, tmpdir)
            self.assertNotEqual(status, "RECORD_PASS", "Cannot claim RECORD_PASS with only .part file")

            # Case 3: Empty finalized file (0 bytes)
            empty_mp4 = os.path.join(tmpdir, "empty_stream.mp4")
            with open(empty_mp4, "wb") as f:
                pass
            valid = recorder.validate_finalized_recordings(tmpdir)
            self.assertEqual(valid, [], "0-byte file must not count as valid recording")
            status = recorder.classify_run_status({"chunk_count": 1, "final_size_mb": 0.0}, tmpdir)
            self.assertNotEqual(status, "RECORD_PASS", "Cannot claim RECORD_PASS with empty file")

            # Case 4: Finalized, non-empty media file (>= MIN_SAVE_BYTES)
            valid_mp4 = os.path.join(tmpdir, "valid_stream.mp4")
            with open(valid_mp4, "wb") as f:
                f.write(b"0" * 1_500_000)
            valid = recorder.validate_finalized_recordings(tmpdir)
            self.assertEqual(len(valid), 1)
            self.assertEqual(valid[0][0], valid_mp4)
            self.assertEqual(valid[0][1], 1_500_000)

            summary_success = {
                "chunk_count": 1,
                "final_size_mb": 1.43,
                "probe_stats": {"clean_offline": 0, "blocked": 0, "error": 0}
            }
            status = recorder.classify_run_status(summary_success, tmpdir)
            self.assertEqual(status, "RECORD_PASS", "Must claim RECORD_PASS with finalized, non-empty media file")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestRecorderV86Final)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    assert result.wasSuccessful(), "Unit tests failed"

    zip_path = "Recorder-v8.6-Final.zip"
    yml_src = "record.yml" if os.path.exists("record.yml") else ".github/workflows/record.yml"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("recorder.py", "recorder.py")
        zf.write(yml_src, ".github/workflows/record.yml")
        zf.write("streamers_16_config.json", "streamers_16_config.json")
        zf.write("test_v83.py", "test_v83.py")
    print("Packaged Recorder-v8.6-Final.zip:", os.path.getsize(zip_path), "bytes")
