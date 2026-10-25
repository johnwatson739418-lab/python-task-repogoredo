"""Reproduce denied-video retries from the uploaded overnight logs, without YouTube calls."""
import ast
import io
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock


def load(*names, **namespace):
    tree = ast.parse(Path('recorder.py').read_text())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<recorder-functions>', 'exec'), namespace)
    return namespace


class DeniedVideoRegression(unittest.TestCase):
    def test_denied_video_stops_before_other_clients(self):
        for reason in ('botcheck', 'rate_limited', 'unavailable'):
            attempt = Mock(return_value=(False, reason, None))
            ns = load('record_until_stop', time=time, STOP_REQUESTED=False,
                      DOWNLOAD_CLIENTS=['web_safari', None, 'android'],
                      run_download_attempt=attempt)
            result = ns['record_until_stop']({}, time.time()+60, None)
            self.assertEqual(result, (False, reason, None))
            self.assertEqual(attempt.call_count, 1)

    def test_repeated_denials_back_off_and_waits_are_bounded(self):
        ns = load('recovery_delay', FAILURE_COOLDOWN_SECONDS=15)
        for reason, expected in [('unavailable', [600,1200,2400,3600,3600]),
                                 ('rate_limited', [300,600,1200,2400,3600])]:
            active = {}
            self.assertEqual([ns['recovery_delay'](reason, active) for _ in range(5)], expected)
        self.assertEqual(ns['recovery_delay']('short_exit', {}), 30)

    def test_private_video_takes_precedence_over_no_formats(self):
        ns = load('classify_output')
        self.assertEqual(ns['classify_output']({'unavailable':True,'no_formats':True}), 'unavailable')
        self.assertEqual(ns['classify_output']({'rate_limited':True,'no_formats':True}), 'rate_limited')

    def test_real_denial_messages_are_classified_before_retry(self):
        tree = ast.parse(Path('recorder.py').read_text())
        markers = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name) and node.targets[0].id.endswith('_MARKERS'):
                markers[node.targets[0].id] = ast.literal_eval(node.value)
        ns = load('output_reader','classify_output', normalize_message=lambda text:text.casefold(), log_raw=lambda *_:None, **markers)
        for message in ('Private video.', 'This video has been removed by the uploader.', 'Sign in to confirm your age.'):
            flags = dict.fromkeys(('botcheck','rate_limited','no_formats','not_live','normal_exit','unavailable'),False)
            ns['output_reader'](io.StringIO('ERROR: [youtube] fixture: '+message+'\n'), flags)
            self.assertEqual(ns['classify_output'](flags),'unavailable')

    def test_private_video_is_not_short_exit_and_partial_media_is_kept(self):
        for has_media in (False, True):
            with tempfile.TemporaryDirectory() as d:
                path = str(Path(d)/'chunk.ts')
                if has_media: Path(path).write_bytes(b'x'*2000)
                state = {'botcheck':False,'rate_limited':False,'unavailable':True,
                         'not_live':False,'normal_exit':False,'no_formats':False}
                process = Mock(); process.poll.return_value = 0
                append = Mock()
                ns = load('run_download_attempt', 'classify_output',
                          time=time, os=os, threading=threading,
                          STOP_REQUESTED=False, CURRENT_PROC_LOCK=threading.Lock(),
                          DOWNLOAD_ACTIVE=threading.Event(),
                          build_chunk_path=lambda *_: path, make_download_cmd=lambda *_: [],
                          client_label=lambda c:c, hms=lambda _: '', log=lambda *_:None,
                          subprocess=Mock(Popen=Mock(return_value=process)),
                          output_reader=lambda stream, output: output.update(state),
                          finalize_download_path=lambda _:path if has_media else None,
                          SHORT_EXIT_PROTECTION_SECONDS=90, MIN_SAVE_BYTES=1880,
                          save_active_state=lambda _:None, append_chunk=append)
                active = {'chunk_index':1}
                ok, reason, _ = ns['run_download_attempt'](active,time.time()+60,'web_safari',None)
                self.assertFalse(ok); self.assertEqual(reason,'unavailable')
                self.assertEqual(append.call_count,int(has_media))
                if has_media: self.assertTrue(Path(path).exists())


if __name__ == '__main__': unittest.main()
