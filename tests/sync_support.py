"""Synthetic provider and contract HTTP peer; never reads installed harnesses."""
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import threading
import urllib.parse


class FakeTokens:
    def __init__(self): self.revoked = False
    def access_token(self, force_refresh=False): return 'fixture-access'
    def revoke_locally(self): self.revoked = True


class FakeProvider:
    def __init__(self, path, *, mode='append'):
        self.path, self.mode = path, mode
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def raw_files(self, provider, native_id):
        data = self.path.read_bytes()
        if self.mode == 'rewrite':
            try: json.loads(data)
            except ValueError:
                from csl_sync_state import SyncError
                raise SyncError('source_incomplete')
            length = len(data)
        else: length = data.rfind(b'\n') + 1
        return [{'file_key':'main','path':str(self.path),'relative_path':'fake-session.jsonl',
                 'mode':self.mode,'complete_length':length,'size':len(data),
                 'sha256':hashlib.sha256(data[:length]).hexdigest(),'role':'transcript'}]

    def list_sessions(self, provider):
        if provider != 'codex': return []
        f = self.raw_files(provider,'fake-session')
        return [{'provider':provider,'native_id':'fake-session','title':'Synthetic','cwd':'/fake/project',
                 'repository':None,'created_at':self.updated_at,'updated_at':self.updated_at,'model':None,
                 'record_count':len(self.records()),'byte_count':f[0]['complete_length'],
                 'parent':None,'origin':'native','state':'active'}]

    def records(self):
        data = self.path.read_bytes()
        if self.mode == 'rewrite':
            values=json.loads(data)['messages']; offsets=[0]*len(values)
        else:
            values=data[:data.rfind(b'\n')+1].splitlines(keepends=True);offsets=[];offset=0
            for value in values: offsets.append(offset);offset+=len(value)
            values=[v.decode().rstrip('\n') for v in values]
        return [{'seq':i+1,'time':None,'role':'user','kind':'text','text':value[:16000],'tool':None,'call_id':None,
                 'raw_offset':{'file_key':'main','offset':offsets[i]}} for i,value in enumerate(values)]

    def view_records(self, provider, native_id, *, position=None, limit=100):
        records=self.records();start=position['n'] if position else 0
        raw=self.raw_files(provider,native_id)[0]
        source={'file_key':raw['file_key'],'length':raw['complete_length'],'sha256':raw['sha256']}
        prior=hashlib.sha256(json.dumps(records[:start],sort_keys=True).encode()).hexdigest()
        if position and (start>len(records) or prior!=position['h']):
            return {'records':[],'position':position,'reset_required':True,'complete':False,'source_file':source}
        page=records[start:start+limit];end=start+len(page)
        return {'records':page,'position':{'n':end,'h':hashlib.sha256(json.dumps(records[:end],sort_keys=True).encode()).hexdigest()},
                'reset_required':False,'complete':end==len(records),'source_file':source}

    def package_descriptor(self, provider, native_id):
        return {'available':True,'descriptor_version':1,'reason':None,
                'manifest':{'format':'codex-session-librarian','version':3,'mode':'raw','created_at':'fixture',
                            'sessions':[{'id':native_id,'provider':provider}]},
                'members':[{'file_key':'main','member':'sessions/fake-session/main.jsonl'}]}


class Peer:
    def __init__(self):
        self.sessions={};self.files={};self.views={};self.writes=[];self.calls=[]
        self.snapshots={};self.operations={};self.upload_requests=[];self.drop_metadata_once=False
        self.error=None;self.redirect_url=None;self.drop_once=False;self.drop_view_once=False;self.first_chunk=None;self.release=None
        peer=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def answer(self,status,value):
                body=json.dumps({**value,'server_time':'2026-11-14T00:00:00Z'}).encode()
                self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers()
                try:self.wfile.write(body)
                except (BrokenPipeError,ConnectionResetError):pass
            def fail(self,status,code):self.answer(status,{'error':{'code':code,'message':'PRIVATE_TRANSCRIPT_SENTINEL','retryable':status>=500,'recovery':'none','retry_after_seconds':0}})
            def do_GET(self):self.handle_request()
            def do_PUT(self):self.handle_request()
            def do_POST(self):self.handle_request()
            def handle_request(self):
                uri=urllib.parse.urlsplit(self.path);path=uri.path;query=urllib.parse.parse_qs(uri.query)
                raw=self.rfile.read(int(self.headers.get('Content-Length','0')));peer.calls.append((self.command,path))
                if peer.redirect_url:
                    self.send_response(302);self.send_header('Location',peer.redirect_url);self.send_header('Content-Length','0');self.end_headers();return
                if peer.error:return self.fail(*peer.error)
                if self.headers.get('Authorization')!='Bearer fixture-access':return self.fail(401,'session_revoked')
                if path=='/limits':return self.answer(200,{'max_file_bytes':2*1024**3,'max_account_bytes':20*1024**3})
                if path.startswith('/devices/'):return self.answer(200,{'registered':True})
                parts=path.strip('/').split('/');key='/'.join(parts[:3])
                if len(parts)==3 and self.command=='PUT':
                    body=json.loads(raw)
                    snapshot_key=(key,body['snapshot_id'])
                    fingerprint=json.dumps(body,sort_keys=True,separators=(',',':'))
                    if snapshot_key in peer.snapshots and peer.snapshots[snapshot_key]!=fingerprint:
                        return self.fail(409,'idempotency_conflict')
                    peer.snapshots[snapshot_key]=fingerprint;peer.sessions[key]=body
                    if peer.drop_metadata_once:
                        peer.drop_metadata_once=False;self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                    return self.answer(200,{'id':'opaque-session'})
                if len(parts)==4 and parts[-1]=='files':
                    items=[{'file_key':f,'revision':r,'length':len(data),'sha256':hashlib.sha256(data).hexdigest()}
                           for (s,f,r),data in peer.files.items() if s==key]
                    start=int(query.get('cursor',['0'])[0]);page=items[start:start+2]
                    return self.answer(200,{'items':page,'next_cursor':str(start+2) if start+2<len(items) else None})
                if len(parts)==6 and parts[-1]=='content':
                    f=parts[-2];rev=query['revision'][0];filekey=(key,f,rev);data=peer.files.setdefault(filekey,bytearray())
                    start=int(self.headers['X-Start']);end=int(self.headers['X-End'])
                    if end-start!=len(raw) or self.headers['X-Content-SHA256']!=hashlib.sha256(raw).hexdigest():return self.fail(422,'invalid_input')
                    operation=(key,f,rev,query['mode'][0],start,end,self.headers.get('X-Prefix-SHA256',''),self.headers['X-Content-SHA256'])
                    idem=self.headers['Idempotency-Key']
                    peer.upload_requests.append((idem,operation))
                    if idem in peer.operations and peer.operations[idem]!=operation:return self.fail(409,'idempotency_conflict')
                    peer.operations[idem]=operation
                    if start and self.headers.get('X-Prefix-SHA256')!=hashlib.sha256(data[:start]).hexdigest():return self.fail(409,'prefix_mismatch')
                    if start==len(data):data.extend(raw);peer.writes.append((filekey,start,end))
                    elif not (end<=len(data) and data[start:end]==raw):return self.fail(409,'revision_conflict')
                    if peer.first_chunk is not None and not peer.first_chunk.is_set():
                        peer.first_chunk.set();peer.release.wait(10)
                    if peer.drop_once:
                        peer.drop_once=False;self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                    return self.answer(200,{'file_key':f,'revision':rev,'length':len(data),'sha256':hashlib.sha256(data).hexdigest()})
                if len(parts)==4 and parts[-1]=='view':
                    body=json.loads(raw);view=peer.views.setdefault((key,body['generation']),[])
                    for row in body['records']:
                        seq=row['seq']
                        if seq==len(view)+1:view.append(row)
                        elif seq<1 or seq>len(view) or view[seq-1]!=row:return self.fail(409,'revision_conflict')
                    if peer.drop_view_once:
                        peer.drop_view_once=False;self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                    return self.answer(200,{'last_seq':len(view)})
                return self.fail(404,'not_found')
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.01),daemon=True);self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
    def close(self):
        if self.release:self.release.set()
        self.server.shutdown();self.server.server_close();self.thread.join(2)
