"""Create immutable, overlapping safety clips while the original capture keeps running.

The original files are never deleted here. Only checksum-verified temporary clips
are removed. Final publication still uploads the main recording independently.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time

from publish_recordings import Publisher, PublishError, fingerprints, run_provider, cancel_provider_processes


def atomic_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value,indent=2)); os.replace(temporary,path)


def probe(path):
    result = subprocess.run(['ffprobe','-v','error','-show_entries',
                             'format=duration:stream=codec_type','-of','json',str(path)],
                            capture_output=True,text=True,timeout=15)
    if result.returncode:
        raise ValueError('media_probe_failed')
    data=json.loads(result.stdout); duration=float(data['format']['duration'])
    if not math.isfinite(duration) or duration<=0 or not any(s.get('codec_type')=='video' for s in data.get('streams',[])):
        raise ValueError('media_not_ready')
    return duration


def make_clip(source, output, start, duration):
    output=Path(output);temp=output.with_suffix('.partial.ts')
    result=subprocess.run(['ffmpeg','-nostdin','-hide_banner','-v','error','-y',
                           '-i',str(source),'-ss',str(start),'-t',str(duration),
                           '-map','0:v:0','-map','0:a:0?','-c','copy',
                           '-avoid_negative_ts','make_zero','-mpegts_flags','+resend_headers',
                           '-f','mpegts',str(temp)],capture_output=True,timeout=90)
    try:
        actual=probe(temp)
        if result.returncode or actual < max(1,duration-3):
            raise ValueError('clip_incomplete_source_kept')
        os.replace(temp,output)
        return actual
    finally:
        temp.unlink(missing_ok=True)


class SafetyArchive:
    def __init__(self, root, config=None, seconds=300, overlap=15, max_pending_bytes=1024**3, uploader=None):
        if not 5<=seconds<=900 or not 0<=overlap<seconds/2:
            raise ValueError('invalid_safety_clip_window')
        self.root=Path(root);self.work=self.root/'.upload-work-safety'
        self.work.mkdir(parents=True,exist_ok=True)
        self.report=self.root/'rolling-backup.json'
        self.seconds,self.overlap,self.max_pending=seconds,overlap,max_pending_bytes
        self.config=config;self.stop=threading.Event();self.lock=threading.Lock()
        self.sources={};self.clips=[];self.last_error=None;self.upload=uploader or self.drive_upload
        self.capture_pressure_reason = None
        self.upload_worker = None
        try:
            previous=json.loads(self.report.read_text())
            for clip in previous.get('clips',[]):
                name=clip.get('local','')
                if not name or Path(name).name!=name:continue
                if clip.get('status')=='uploaded' or (self.work/name).is_file():
                    if clip.get('status')!='uploaded':clip.update(status='pending',retry_at=0)
                    self.clips.append(clip)
        except (OSError,ValueError,TypeError,AttributeError):pass
        self.capture_finished=False
        self.state='not_configured' if not config and uploader is None else 'running'

    def save(self):
        with self.lock:
            atomic_json(self.report,{'version':1,'state':self.state,'clip_seconds':self.seconds,
                                    'overlap_seconds':self.overlap,'original_sources_deleted':False,
                                    'last_error':self.last_error,'clips':self.clips,
                                    'capture_pressure_reason':self.capture_pressure_reason,
                                    'uploaded_clips':sum(c['status']=='uploaded' for c in self.clips),
                                    'pending_clips':sum(c['status']!='uploaded' for c in self.clips)})

    def discover(self):
        for path in sorted(self.root.rglob('*.ts')):
            if '.upload-work-' in str(path) or path.is_symlink():continue
            try:stat=path.stat()
            except OSError:continue
            if stat.st_size<1880:continue
            key=str(stat.st_dev)+':'+str(stat.st_ino)
            # Follow a single native chunk after the recorder renames it to its final file.
            if '_chunks' not in path.parts and key not in self.sources:continue
            if key not in self.sources:
                relative=str(path.relative_to(self.root))
                covered=0.0
                for clip in sorted((c for c in self.clips if c.get('source')==relative),key=lambda c:c['start_seconds']):
                    if clip['start_seconds']>covered+.01:break
                    covered=max(covered,clip['start_seconds']+clip['requested_duration'])
                self.sources[key]={'path':path,'name':relative,'cursor':max(0,covered-self.overlap),
                                   'id':hashlib.sha256(relative.encode()).hexdigest()[:16]}
            self.sources[key]['path']=path

    def stage(self, final=False):
        stage_deadline=time.monotonic()+120
        self.discover()
        for entry in list(self.sources.values()):
            source=entry['path']
            try:duration=probe(source)
            except (OSError,ValueError,KeyError,subprocess.TimeoutExpired):continue
            while duration-entry['cursor'] >= (2 if final else self.seconds+10):
                if time.monotonic() >= stage_deadline or self.stop.is_set():return
                pending=0
                for pending_path in self.work.glob('*.ts'):
                    try:pending+=pending_path.stat().st_size
                    except FileNotFoundError:pass
                if pending >= self.max_pending or shutil.disk_usage(self.root).free<128*1024**2:
                    self.capture_pressure_reason='safety_backlog_limit_reached'
                    self.last_error='safety_buffer_full_original_capture_halt_requested';self.save();return
                start=entry['cursor'];wanted=min(self.seconds,duration-start)
                path=self.work/(entry['id']+'-'+str(round(start*1000))+'.ts')
                try:actual=make_clip(source,path,start,wanted)
                except (OSError,ValueError,subprocess.TimeoutExpired):break
                with self.lock:
                    self.clips.append({'source':entry['name'],'start_seconds':round(start,3),
                                       'requested_duration':round(wanted,3),'duration':round(actual,3),
                                       'local':path.name,'status':'pending','attempts':0,'retry_at':0})
                entry['cursor']=start+wanted-self.overlap
                self.save()
                if wanted<self.seconds:break

    def drive_upload(self,path,metadata):
        publisher=Publisher(self.work,self.work/'unused-report.json',budget=90)
        publisher.category='safety'
        publisher.drive_config=self.config or ''
        return publisher.google_drive(path,metadata)

    def upload_index(self, receipt):
        if not self.config or receipt.get('provider')!='google_drive':return
        remote=receipt['remote_path'].rsplit('/',1)[0]+'/backup-index.json'
        snapshot=self.work/'index-snapshot.json'
        with self.lock:snapshot.write_bytes(self.report.read_bytes())
        metadata=fingerprints(snapshot)
        base=['rclone','--config',self.config]
        copied=run_provider(base+['copyto',str(snapshot),remote,'--checksum','--contimeout','10s','--timeout','20s','--retries','1'],capture_output=True,timeout=45)
        if copied.returncode:raise ValueError('safety_index_copy_failed')
        result=run_provider(base+['lsjson',remote,'--stat','--hash'],capture_output=True,text=True,timeout=20)
        data=json.loads(result.stdout)
        if result.returncode or data.get('Size')!=metadata['bytes'] or data.get('Hashes',{}).get('md5')!=metadata['md5']:
            raise ValueError('safety_index_verification_failed')

    def upload_pending(self):
        while not self.stop.is_set():
            with self.lock:
                clip=next((c for c in self.clips if c['status']=='pending' and c['retry_at']<=time.monotonic()),None)
                if clip:clip['status']='uploading'
            if clip is None:
                if self.capture_finished:return
                self.stop.wait(.25);continue
            path=self.work/clip['local']
            try:
                metadata=fingerprints(path, deadline=time.monotonic()+30)
                receipt=self.upload(path,metadata)
                if not isinstance(receipt,dict) or not receipt.get('verification'):
                    raise ValueError('safety_receipt_unverified')
                with self.lock:
                    clip.update(status='uploaded',receipt=receipt,bytes=metadata['bytes'],sha256=metadata['sha256'])
                path.unlink(missing_ok=True)
                self.save()
                try:
                    if not self.stop.is_set():self.upload_index(receipt)
                except (OSError,ValueError,subprocess.TimeoutExpired):self.last_error='safety_index_upload_pending'
            except (OSError,ValueError,PublishError,subprocess.TimeoutExpired):
                with self.lock:
                    clip['attempts']+=1;clip['status']='pending'
                    clip['retry_at']=time.monotonic()+min(300,30*2**min(clip['attempts']-1,4))
                    self.last_error='safety_upload_failed_original_and_clip_kept'
                # A failed provider must not cause a tight loop across all queued files.
                self.stop.wait(min(30,clip['retry_at']-time.monotonic()))
            self.save()

    def run(self, capture_done):
        self.save()
        if self.state=='not_configured':return
        worker=threading.Thread(target=self.upload_pending,daemon=True)
        self.upload_worker=worker
        worker.start()
        try:
            while not capture_done.is_set() and not self.stop.is_set():
                self.stage();capture_done.wait(10)
            self.stage(final=True)
            self.capture_finished=True
            worker.join(timeout=120)
        finally:
            # A current Drive operation has a90s budget, followed by at most
            #65s of index I/O (and30s hash budget before it). Do not delete the
            # private configuration while the owned uploader can still use it.
            self.stop.set()
            cancel_provider_processes()
            worker.join(timeout=40)
            if worker.is_alive():
                self.last_error='safety_uploader_shutdown_deadline_exceeded'
            self.state=('no_safety_media' if not self.clips else ('completed' if all(c['status']=='uploaded' for c in self.clips) else 'pending_backup_clips'))
            self.save()
