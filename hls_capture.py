"""Extract an authorized HLS URL with yt-dlp; read it with bounded Streamlink recovery.

No proxy rotation, challenge bypass, cookie export, or response-body logging.
"""
import argparse
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
import http.cookiejar
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from probe_health import classify_message

MESSAGES={'botcheck':"Sign in to confirm you're not a bot",'rate_limited':'Too many requests',
          'age_verification':'Sign in to confirm your age','private_video':'Private video',
          'removed_video':'Video has been removed by the uploader','access_denied':'HTTP Error 403',
          'invalid_cookies':'The provided YouTube account cookies are no longer valid',
          'not_live':'The channel is not currently live','ended':'This live event has ended',
          'network_error':'connection failed','no_formats':'No video formats found','extraction_failed':'download extraction failed'}


def describe(reason):
    print('ERROR: '+MESSAGES.get(reason,MESSAGES['extraction_failed']),flush=True)


def extract(args):
    if args.metadata:return json.loads(Path(args.metadata).read_text())
    command=[sys.executable,'-m','yt_dlp','--no-config','--no-playlist',
             '--skip-download','--extractor-retries','1','--socket-timeout','15','-J','-f',args.format]
    if shutil.which('node'):command+=['--js-runtimes','node']
    if args.client!='default':command+=['--extractor-args','youtube:player_client='+args.client]
    if args.cookies:command+=['--cookies',args.cookies]
    result=subprocess.run(command+[args.url],capture_output=True,text=True,timeout=45)
    message=result.stderr.casefold()
    if any(x in message for x in ('cookies are no longer valid','cookies have expired','cookies are invalid')):
        describe('invalid_cookies');return None
    if result.returncode:
        describe(classify_message(result.stderr));return None
    return json.loads(result.stdout)


def retry_after(value):
    try:return max(0,float(value))
    except (TypeError,ValueError):
        try:return max(0,(parsedate_to_datetime(value)-datetime.now(timezone.utc)).total_seconds())
        except (TypeError,ValueError,OverflowError):return None


def configure_session(session,metadata,cookies):
    session.set_option('stream-timeout',90)
    session.set_option('stream-segment-timeout',12)
    session.set_option('stream-segment-attempts',2)
    session.set_option('stream-segment-threads',1)
    session.set_option('hls-playlist-reload-attempts',1)
    session.set_option('hls-live-edge',3)
    session.set_option('hls-start-offset',60)
    session.set_option('hls-segment-stream-data',True)
    session.http.headers.update(metadata.get('http_headers') or {})
    if cookies and Path(cookies).is_file():
        jar=http.cookiejar.MozillaCookieJar(cookies);jar.load(ignore_discard=True,ignore_expires=False)
        session.http.cookies.update(jar)
    original=session.http.request
    state={'denial':None,'retries':0}
    def request(method,url,*a,**kw):
        # Streamlink's default retries include 403. Keep auth denials out of the retry loop.
        kw['retries']=0;deadline=time.monotonic()+60;attempt=0
        while True:
            if state['denial']:raise OSError('authorized_stream_access_denied')
            try:return original(method,url,*a,**kw)
            except Exception as error:
                underlying=getattr(error,'err',error);response=getattr(underlying,'response',None)
                status=getattr(response,'status_code',None)
                if status in (401,403,429):
                    state['denial']='rate_limited' if status==429 else 'access_denied'
                    raise
                # Network failures and temporary server failures only; never ignore TLS errors.
                import requests
                transient=(status in (408,500,502,503,504) or isinstance(underlying,(requests.exceptions.ConnectionError,requests.exceptions.Timeout)))
                if isinstance(underlying,requests.exceptions.SSLError):transient=False
                if not transient:raise
                delay=retry_after(response.headers.get('Retry-After')) if response is not None else None
                if delay is None:delay=min(5,1+attempt)
                if time.monotonic()+delay>=deadline:raise
                state['retries']+=1;attempt+=1;time.sleep(delay)
    session.http.request=request
    return state


def fallback_capture(metadata,args):
    # Keep the normal downloader for combined formats that are not HLS.
    with tempfile.TemporaryDirectory(prefix='private-media-info-') as d:
        info=Path(d)/'selected.json';info.write_text(json.dumps(metadata));info.chmod(0o600)
        command=[sys.executable,'-m','yt_dlp','--no-config','--no-part','--no-progress','--quiet',
                 '--no-warnings','--retries','5','--fragment-retries','10',
                 '--load-info-json',str(info),'-o',args.output]
        if args.cookies:command+=['--cookies',args.cookies]
        with tempfile.TemporaryFile() as errors:
            child=subprocess.Popen(command,stdout=subprocess.DEVNULL,stderr=errors)
            def stop(signum,frame):
                if child.poll() is None:child.send_signal(signal.SIGINT)
            signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
            status=child.wait()
            if status:
                errors.seek(0);describe(classify_message(errors.read().decode(errors='replace')))
            return status


def capture(args):
    metadata=extract(args)
    if not metadata:return 2
    if metadata.get('is_live') is not True and metadata.get('live_status') != 'is_live':
        describe('ended' if metadata.get('live_status') in ('was_live','post_live') else 'not_live');return 2
    url=metadata.get('url','')
    if not url.startswith(('https://','http://')):
        describe('extraction_failed');return 2
    # A combined audio/video HLS format is selected. Other formats retain yt-dlp's normal downloader.
    if 'm3u8' not in str(metadata.get('protocol','')):
        return fallback_capture(metadata,args)
    from streamlink import Streamlink
    from streamlink.stream.hls import HLSStream
    logging.getLogger('streamlink').setLevel(logging.CRITICAL)
    session=Streamlink();state=configure_session(session,metadata,args.cookies)
    output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
    stream=None;stopping=False
    def stop(signum,frame):
        nonlocal stopping
        stopping=True
        # Let the current read finish; the parent has a bounded escalation timeout.
        if stream is not None:
            try:stream.close()
            except Exception:pass
    signal.signal(signal.SIGINT,stop);signal.signal(signal.SIGTERM,stop)
    try:
        print('chosen='+str(metadata.get('format_id','HLS'))+' | '+str(metadata.get('width','?'))+'x'+str(metadata.get('height','?'))+' | transport=streamlink_hls',flush=True)
        stream=HLSStream(session,url).open()
        with output.open('wb',buffering=0) as target:
            while not stopping:
                block=stream.read(188*256)
                if not block:break
                target.write(block)
        if state['denial']:
            describe(state['denial']);return 2
        print('transport_complete | transient_retries='+str(state['retries']),flush=True)
        return 0
    except Exception as error:
        if not stopping:describe(state['denial'] or classify_message(str(error)))
        return 0 if stopping else 2
    finally:
        if stream is not None:
            try:stream.close()
            except Exception:pass
        session.http.close()


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--url',default='');parser.add_argument('--metadata')
    parser.add_argument('--output',required=True);parser.add_argument('--format',default='best');parser.add_argument('--client',default='web_safari');parser.add_argument('--cookies')
    args=parser.parse_args()
    try:return capture(args)
    except (OSError,ValueError,subprocess.TimeoutExpired):describe('extraction_failed');return 2

if __name__=='__main__':raise SystemExit(main())
