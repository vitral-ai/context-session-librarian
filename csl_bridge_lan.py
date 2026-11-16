"""Private session LAN pairing: authenticated discovery, pinned TLS, local-only grants.

No Codex auth, native server, model, public relay or operating-system service setup.
"""
import base64
import hashlib
import hmac
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import socketserver
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import uuid

sys.path.insert(0,str(Path(__file__).resolve().parent))

from csl_bridge_store import BridgeError, MAX_WIRE, Store, decode, encode, name, private_directory, validate, identifier

class LANRejected(BridgeError):pass

DISCOVERY_PORT = 47639
INVITATION_SECONDS = 300
COMMUNICATION_OPS = ['followup','send_message','status','interrupt','receipt','watch','agents']


def save(path, data):
    path=Path(path);path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
    fd,temp=tempfile.mkstemp(prefix='.lan-',dir=path.parent)
    try:
        with os.fdopen(fd,'w') as out:out.write(encode(data))
        os.replace(temp,path)
    finally:
        if os.path.exists(temp):os.unlink(temp)


def private_json(path):
    path=Path(path);info=path.lstat()
    if path.is_symlink() or not path.is_file() or info.st_uid!=os.getuid() or info.st_mode&0o077:
        raise BridgeError('LAN credentials must be private owned files')
    return decode(path.read_bytes())


def code_bytes(code):
    try:
        value=base64.b32decode(code.strip().replace('-','').upper()+'======')
        if len(value)!=16:raise ValueError()
        return value
    except (ValueError,TypeError):raise BridgeError('Use the complete 26-character pairing code') from None


def mac(secret, value):return hmac.new(secret,encode(value).encode(),hashlib.sha256).hexdigest()


def endpoint(value):
    if not isinstance(value,dict) or set(value)!={'host','port','fingerprint'}:raise BridgeError('Invalid LAN endpoint')
    try:
        if not isinstance(value['host'],str) or len(value['host'])>15:raise ValueError()
        ip=ipaddress.IPv4Address(value['host'])
        if not (ip.is_private or ip.is_loopback) or ip.is_unspecified or ip.is_multicast or ip.is_reserved or str(ip)=='255.255.255.255':raise ValueError()
    except (OSError,ValueError,TypeError):raise BridgeError('Select an explicit reachable IPv4 interface') from None
    if type(value['port']) is not int or not 1<=value['port']<=65535:raise BridgeError('Invalid LAN port')
    digest=value['fingerprint']
    if not isinstance(digest,str) or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest):raise BridgeError('Invalid TLS pin')
    return value


def token_value(value):
    if not isinstance(value,str) or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
        raise BridgeError('Invalid private peer token')
    return value


def validate_config(config):
    if isinstance(config,dict) and config.get('transport')=='relay':
        from csl_bridge_relay_client import validate_peer_config
        return validate_peer_config(config)
    if not isinstance(config,dict) or config.get('transport')!='lan' or set(config)!={'transport','endpoint','token_file','allow_agents','allow_ops'}:raise BridgeError('Invalid LAN peer profile')
    endpoint(config['endpoint'])
    if not config['allow_agents'] or any(name(a)!=a for a in config['allow_agents']):raise BridgeError('Select registered local agents')
    if set(config['allow_ops'])!=set(COMMUNICATION_OPS):raise BridgeError('LAN scope is communication only')
    token=private_json(config['token_file'])['token']
    token_value(token)
    return config


def request(remote, payload, timeout=90):
    remote=endpoint(remote)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.check_hostname=False;context.verify_mode=ssl.CERT_NONE
    context.minimum_version=ssl.TLSVersion.TLSv1_2
    with socket.create_connection((remote['host'],remote['port']),timeout=timeout) as raw:
        with context.wrap_socket(raw,server_hostname=None) as stream:
            if not hmac.compare_digest(hashlib.sha256(stream.getpeercert(binary_form=True)).hexdigest(),remote['fingerprint']):
                raise BridgeError('LAN TLS identity changed; no credentials or task were sent')
            stream.sendall((encode(payload)+'\n').encode())
            with stream.makefile('rb') as file:data=file.readline(MAX_WIRE+1)
    if not data.endswith(b'\n'):raise BridgeError('LAN response lost; outcome uncertain; inspect receipt')
    value=decode(data)
    if not isinstance(value,dict) or set(value)!={'ok','data'} or value['ok'] is not True:raise LANRejected('LAN peer rejected request; inspect local state')
    return value['data']


def exchange(config, identity, body):
    validate_config(config);validate(body)
    try:return request(config['endpoint'],{'op':'call','token':private_json(config['token_file'])['token'],'request':body})
    except (OSError,ValueError,ssl.SSLError):raise BridgeError('LAN outcome uncertain; inspect receipt before retry') from None


def register_peer(home, peer, config):
    """Explicit local pairing action. This operation is never accepted from a peer."""
    from csl_bridge_transport import local_call
    name(peer);validate_config(config)
    try:
        return local_call(home,{'local_op':'pair_peer','peer':peer,'config':config})
    except BridgeError:
        # Offline fallback requires the same exclusive lock as normal configuration.
        import fcntl
        store=Store(home)
        lock=os.fdopen(os.open(store.home/'connector.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600),'a')
        try:
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise BridgeError('Existing connector does not support pairing; update it under its owner, never start a second writer') from None
            if peer==store.setting('identity') or not set(config['allow_agents'])<=set(store.agents()):raise BridgeError('Invalid local pairing identity/scope')
            previous=store.peers().get(peer)
            if previous is not None and previous!=config and previous.get('transport') not in ('lan','relay'):raise BridgeError('Existing non-LAN peer profile is preserved')
            with store.transaction() as db:db.execute('INSERT OR REPLACE INTO peers VALUES(?,?)',(peer,encode(config)))
        finally:lock.close();store.close()
        return {'paired':peer,'scope':'communication'}


class Session:
    def __init__(self, state, home, bind='127.0.0.1', port=0, discovery_port=DISCOVERY_PORT):
        self.state=private_directory(state,create=True);self.home=private_directory(home)
        if bind in ('0.0.0.0','255.255.255.255'):raise BridgeError('Select a concrete local interface; no wildcard listener')
        socket.inet_aton(bind)
        store=Store(home)
        try:self.identity=store.setting('identity');self.agents=list(store.agents());self.thread_id=next(iter(store.agents().values()),None)
        finally:store.close()
        if len(self.agents)!=1:raise BridgeError('Select one existing local conversation for this pairing session')
        import fcntl
        self.owner_lock=os.fdopen(os.open(self.state/'session.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600),'a')
        try:fcntl.flock(self.owner_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise BridgeError('A LAN session already owns this private state') from None
        if (self.state/'session.json').exists():
            old=private_json(self.state/'session.json')
            if old.get('bridge_home')!=str(self.home) or old.get('thread_id')!=self.thread_id:
                raise BridgeError('This LAN state belongs to another conversation; preserve it and select its original session')
        self.lock=threading.RLock();self.stop=threading.Event();self.threads=[];self.discovery_window=[]
        self.invitation=private_json(self.state/'invitation.json') if (self.state/'invitation.json').exists() else None
        self.registry=private_json(self.state/'registry.json') if (self.state/'registry.json').exists() else {}
        self.peer_agents=private_json(self.state/'peers.json') if (self.state/'peers.json').exists() else {}
        self.receipts=private_json(self.state/'pair-receipts.json') if (self.state/'pair-receipts.json').exists() else {}
        cert=self.state/'certificate.pem';key=self.state/'certificate-key.pem'
        if not cert.exists():
            executable=shutil.which('openssl',path='/usr/bin:/bin')
            if not executable:raise BridgeError('This OS needs its existing openssl tool to create a local TLS identity')
            old=os.umask(0o077)
            try:
                subprocess.run([executable,'req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','3650','-subj','/CN=CSL-session'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=15)
            finally:os.umask(old)
        for path in (cert,key):
            info=path.lstat()
            if path.is_symlink() or info.st_uid!=os.getuid() or info.st_mode&0o077:raise BridgeError('TLS identity files must be private')
        self.fingerprint=hashlib.sha256(ssl.PEM_cert_to_DER_cert(cert.read_text())).hexdigest()
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2;context.load_cert_chain(cert,key)
        session=self
        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                self.connection.settimeout(65)
                try:
                    raw=self.rfile.readline(MAX_WIRE+1)
                    if not raw.endswith(b'\n'):raise BridgeError('Incomplete LAN request')
                    answer={'ok':True,'data':session.incoming(decode(raw),self.client_address[0])}
                except (BridgeError,OSError,ValueError,KeyError,TypeError):answer={'ok':False,'data':{'error':'LAN request rejected'}}
                try:self.wfile.write((encode(answer)+'\n').encode())
                except OSError:pass
        class Server(socketserver.ThreadingTCPServer):
            daemon_threads=True
            allow_reuse_address=True
            slots=threading.BoundedSemaphore(16)
            def process_request(self, request, client_address):
                if not self.slots.acquire(blocking=False):request.close();return
                try:super().process_request(request,client_address)
                except BaseException:self.slots.release();raise
            def process_request_thread(self, request, client_address):
                try:super().process_request_thread(request,client_address)
                finally:self.slots.release()
            def get_request(self):
                raw,address=super().get_request();raw.settimeout(10)
                try:return context.wrap_socket(raw,server_side=True),address
                except Exception:raw.close();raise
        if port==0 and (self.state/'session.json').exists():
            previous=private_json(self.state/'session.json').get('endpoint',{})
            if previous.get('host')==bind:port=endpoint(previous)['port']
        self.server=Server((bind,port),Handler)
        self.remote={'host':bind,'port':self.server.server_address[1],'fingerprint':self.fingerprint}
        self.discovery_port=discovery_port;self.udp=None

    def validate_endpoint(self, value):return endpoint(value)

    def peer_config(self, remote, token_path):
        return {'transport':'lan','endpoint':remote,'token_file':str(token_path),'allow_agents':self.agents,'allow_ops':COMMUNICATION_OPS}

    def invite(self):
        with self.lock:
            code=base64.b32encode(secrets.token_bytes(16)).decode().rstrip('=')
            self.invitation={'code':code,'expires':time.time()+INVITATION_SECONDS,'used':False,'failures':0}
            save(self.state/'invitation.json',self.invitation)
            payload={'endpoint':self.remote,'proof':mac(code_bytes(code),self.remote),'code':code}
            link='cslpair:'+base64.urlsafe_b64encode(encode(payload).encode()).decode().rstrip('=')
            return {'code':code,'link':link,'expires_at':self.invitation['expires'],'scope':'Tasks, messages, progress, status and results; no shell, history or approvals'}

    def discovery_reply(self, query):
        with self.lock:
            invite=self.invitation
            now=time.monotonic();self.discovery_window=[x for x in self.discovery_window if now-x<1]
            if len(self.discovery_window)>=8:return None
            if not invite or invite['used'] or time.time()>invite['expires'] or invite['failures']>=6:return None
            if not isinstance(query,dict) or set(query)!={'nonce','proof'} or not isinstance(query['nonce'],str) or len(query['nonce'])!=32:return None
            secret=code_bytes(invite['code'])
            if not hmac.compare_digest(query['proof'],mac(secret,{'nonce':query['nonce']})):return None
            self.discovery_window.append(now)
            body={'nonce':query['nonce'],'endpoint':self.remote}
            return {**body,'proof':mac(secret,body)}

    def incoming(self, value, source_address=None):
        from csl_bridge_transport import local_call
        if not isinstance(value,dict):raise BridgeError('Invalid LAN frame')
        if value=={'op':'health'}:return {'identity':self.identity,'scope':'session_endpoint_only'}
        if value.get('op')=='pair_receipt' and set(value)=={'op','pair_id','proof'}:
            record=self.receipts.get(identifier(value['pair_id']))
            if not record:return {'state':'not_found','note':'No receipt observed; this is not permission to replay pairing.'}
            if not hmac.compare_digest(value['proof'],mac(bytes.fromhex(record['proof_key']),{'pair_id':value['pair_id'],'endpoint':self.remote})):
                raise BridgeError('Invalid pairing receipt proof')
            return {'state':'accepted','result':record['result']}
        if value.get('op')=='call' and set(value)=={'op','token','request'}:
            token=value['token']
            if not isinstance(token,str) or len(token)!=64:raise BridgeError('Invalid credential')
            peer=self.registry.get(hashlib.sha256(token.encode()).hexdigest())
            if not peer:raise BridgeError('Unpaired or revoked peer')
            return local_call(self.home,{'local_op':'_incoming','trusted_peer':peer,'request':validate(value['request'])})
        if value.get('op')!='pair' or set(value)!={'op','offer','proof'}:raise BridgeError('Pairing required')
        with self.lock:
            invite=self.invitation
            if not invite or invite['used'] or time.time()>invite['expires'] or invite['failures']>=6:raise BridgeError('Invitation expired or consumed')
            offer=value['offer'];secret=code_bytes(invite['code'])
            if not hmac.compare_digest(value['proof'],mac(secret,{'offer':offer,'endpoint':self.remote})):
                invite['failures']+=1;save(self.state/'invitation.json',invite);raise BridgeError('Invalid pairing code')
            if set(offer)!={'identity','endpoint','token','agent','pair_id'}:raise BridgeError('Invalid communication offer')
            peer=name(offer['identity']);name(offer['agent']);self.validate_endpoint(offer['endpoint']);token_value(offer['token']);identifier(offer['pair_id'])
            if source_address is not None and offer['endpoint'].get('host')!=source_address:
                raise BridgeError('Return endpoint must belong to the connected LAN peer')
            if peer==self.identity:raise BridgeError('Pair with a different bridge identity')
            if peer not in self.registry.values() and len(self.registry)>=32:raise BridgeError('This session supports at most32 communication peers')
            token=secrets.token_hex(32);token_path=self.state/('to-'+peer+'.json')
            save(token_path,{'token':offer['token']})
            config=self.peer_config(offer['endpoint'],token_path)
            register_peer(self.home,peer,config)
            self.registry={key:owner for key,owner in self.registry.items() if owner!=peer}
            self.registry[hashlib.sha256(token.encode()).hexdigest()]=peer;save(self.state/'registry.json',self.registry)
            self.peer_agents[peer]=offer['agent'];save(self.state/'peers.json',self.peer_agents)
            invite['used']=True;save(self.state/'invitation.json',invite)
            result={'identity':self.identity,'agent':self.agents[0],'endpoint':self.remote,'token':token}
            self.receipts[offer['pair_id']]={'proof_key':offer['token'],'result':result}
            self.receipts=dict(list(self.receipts.items())[-32:]);save(self.state/'pair-receipts.json',self.receipts)
            return result

    def start(self, discovery=True):
        thread=threading.Thread(target=self.server.serve_forever,daemon=True);thread.start();self.threads.append(thread)
        if discovery:
            self.udp=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            self.udp.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
            self.udp.bind(('',self.discovery_port));self.discovery_port=self.udp.getsockname()[1];self.udp.settimeout(.5)
            def listen():
                while not self.stop.is_set():
                    try:
                        raw,address=self.udp.recvfrom(4096);reply=self.discovery_reply(decode(raw))
                        if reply:self.udp.sendto(encode(reply).encode(),address)
                    except (OSError,ValueError,TypeError,BridgeError):pass
            thread=threading.Thread(target=listen,daemon=True);thread.start();self.threads.append(thread)
        session=self
        class Control(socketserver.StreamRequestHandler):
            def handle(self):
                self.connection.settimeout(5)
                try:
                    value=decode(self.rfile.readline(4096))
                    if value=={'op':'invite'}:data=session.invite()
                    elif value.get('op')=='connect' and set(value)=={'op','code','link'}:
                        remote=link_endpoint(value['link'],value['code']) if value['link'] else discover(value['code'])
                        data=pair(session,value['code'],remote)
                    elif value=={'op':'reconcile'}:data=reconcile_pair(session)
                    elif value=={'op':'status'}:data={'identity':session.identity,'bridge_home':str(session.home),'endpoint':session.remote,'paired_count':len(session.registry),'peers':[{'identity':p,'agent':a} for p,a in session.peer_agents.items()],'running':True}
                    elif value=={'op':'stop'}:session.stop.set();data={'stopping':True,'tokens_revoked_when_stopped':True}
                    else:raise BridgeError('Invalid local session control')
                    self.wfile.write((encode({'ok':True,'data':data})+'\n').encode())
                except (BridgeError,OSError,ValueError):pass
        control_path=self.state/'session-control.sock'
        if control_path.exists():
            import stat
            info=control_path.lstat()
            if info.st_uid!=os.getuid() or not stat.S_ISSOCK(info.st_mode):raise BridgeError('Unexpected session control path')
            control_path.unlink()
        self.control=socketserver.ThreadingUnixStreamServer(str(control_path),Control)
        self.control.daemon_threads=True;os.chmod(control_path,0o600)
        thread=threading.Thread(target=self.control.serve_forever,daemon=True);thread.start();self.threads.append(thread)
        save(self.state/'session.json',{'pid':os.getpid(),'endpoint':self.remote,'identity':self.identity,'agent':self.agents[0],'bridge_home':str(self.home),'thread_id':self.thread_id})

    def close(self, revoke=True):
        self.stop.set()
        if self.udp:self.udp.close()
        self.server.shutdown();self.server.server_close()
        if hasattr(self,'control'):self.control.shutdown();self.control.server_close()
        if revoke:
            save(self.state/'pair-attempt.json',{'state':'revoked'})
            self.registry={};save(self.state/'registry.json',{});self.invitation=None
            self.receipts={};save(self.state/'pair-receipts.json',{})
            self.peer_agents={};save(self.state/'peers.json',{})
            for path in self.state.glob('to-*.json'):
                if path.is_symlink():raise BridgeError('Unexpected private credential path')
                save(path,{'token':''})
            save(self.state/'invitation.json',{'used':True,'expires':0,'failures':6,'code':''})
        for thread in self.threads:thread.join(1)
        self.owner_lock.close()


def discover(code, target='255.255.255.255', port=DISCOVERY_PORT, timeout=3):
    secret=code_bytes(code);nonce=secrets.token_hex(16);query={'nonce':nonce,'proof':mac(secret,{'nonce':nonce})}
    with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as udp:
        udp.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1);udp.settimeout(timeout);udp.sendto(encode(query).encode(),(target,port))
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            raw,address=udp.recvfrom(4096);reply=decode(raw)
            body={'nonce':reply.get('nonce'),'endpoint':reply.get('endpoint')}
            if body['nonce']==nonce and isinstance(reply.get('proof'),str) and hmac.compare_digest(reply['proof'],mac(secret,body)):
                selected=endpoint(body['endpoint'])
                if selected['host']!=address[0]:raise BridgeError('Discovery endpoint does not match responding peer')
                return selected
    raise BridgeError('Peer not discovered; use its authenticated invitation link or check OS local-network permission')


def link_endpoint(link, code):
    try:
        if not link.startswith('cslpair:'):raise ValueError()
        data=decode(base64.urlsafe_b64decode(link[8:]+'='*(-len(link[8:])%4)))
        if not hmac.compare_digest(data['proof'],mac(code_bytes(code),data['endpoint'])):raise ValueError()
        return endpoint(data['endpoint'])
    except (ValueError,KeyError,TypeError):raise BridgeError('Invitation link does not match the code') from None


def pair(session, code, remote):
    secret=code_bytes(code);remote=endpoint(remote);token=secrets.token_hex(32)
    attempt_path=session.state/'pair-attempt.json'
    if attempt_path.exists() and private_json(attempt_path).get('state')=='uncertain':
        raise BridgeError('Previous pairing outcome is uncertain; reconcile it before creating another grant')
    offer={'identity':session.identity,'agent':session.agents[0],'endpoint':session.remote,'token':token,'pair_id':str(uuid.uuid4())}
    attempt={'state':'uncertain','offer':offer,'remote':remote}
    save(attempt_path,attempt)
    try:result=request(remote,{'op':'pair','offer':offer,'proof':mac(secret,{'offer':offer,'endpoint':remote})},timeout=15)
    except LANRejected:
        save(attempt_path,{'state':'rejected','pair_id':offer['pair_id']})
        return {'state':'rejected','pair_id':offer['pair_id'],'note':'Peer rejected this pairing. Check the current code and expiry; no automatic grant replay.'}
    except (BridgeError,OSError,ValueError):
        return {'state':'uncertain','pair_id':offer['pair_id'],'note':'Pairing may have committed on the peer. Reconcile; do not create another grant.'}
    try:return apply_pair_result(session,attempt,result)
    except (BridgeError,OSError,ValueError):return {'state':'uncertain','pair_id':offer['pair_id'],'note':'Peer may be paired, but local setup is incomplete. Review local profiles and reconcile; do not replay.'}


def apply_pair_result(session, attempt, result):
    remote=attempt['remote'];token=attempt['offer']['token']
    if not isinstance(result,dict) or set(result)!={'identity','agent','endpoint','token'}:raise BridgeError('Invalid pairing receipt')
    peer=name(result['identity']);name(result['agent']);token_value(result['token'])
    if result['endpoint']!=remote:raise BridgeError('Paired peer identity changed')
    path=session.state/('to-'+peer+'.json');save(path,{'token':result['token']})
    config=session.peer_config(remote,path)
    register_peer(session.home,peer,config)
    session.registry={key:owner for key,owner in session.registry.items() if owner!=peer}
    session.registry[hashlib.sha256(token.encode()).hexdigest()]=peer;save(session.state/'registry.json',session.registry)
    session.peer_agents[peer]=result['agent'];save(session.state/'peers.json',session.peer_agents)
    save(session.state/'pair-attempt.json',{'state':'paired','pair_id':attempt['offer']['pair_id'],'peer':peer})
    return {'state':'paired','paired':peer,'remote_agent':result['agent'],'scope':'communication','endpoint':remote,'model_started':False}


def reconcile_pair(session):
    attempt=private_json(session.state/'pair-attempt.json')
    if attempt.get('state')!='uncertain':return attempt
    pair_id=attempt['offer']['pair_id'];remote=attempt['remote']
    reply=request(remote,{'op':'pair_receipt','pair_id':pair_id,'proof':mac(bytes.fromhex(attempt['offer']['token']),{'pair_id':pair_id,'endpoint':remote})},timeout=15)
    if reply.get('state')!='accepted':return {'state':'uncertain','pair_id':pair_id,'note':'No receipt observed; do not create a replacement grant.'}
    try:return apply_pair_result(session,attempt,reply['result'])
    except (BridgeError,OSError,ValueError):return {'state':'uncertain','pair_id':pair_id,'note':'Peer accepted this pairing; local profile needs review. Non-LAN profiles are never overwritten automatically.'}


def control(state, op):
    state=private_directory(state)
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as stream:
        stream.settimeout(20 if op=='reconcile' else 5);stream.connect(str(state/'session-control.sock'))
        stream.sendall((encode({'op':op})+'\n').encode())
        with stream.makefile('rb') as file:answer=decode(file.readline(MAX_WIRE+1))
    if answer.get('ok') is not True:raise BridgeError('Local LAN session unavailable')
    return answer['data']


def network_choices():
    """Read OS interface/default-route metadata only; never send discovery traffic."""
    import re
    addresses={};preferred=None
    executable=shutil.which('ip',path='/sbin:/usr/sbin:/usr/bin:/bin')
    if executable:
        result=subprocess.run([executable,'-j','address','show'],capture_output=True,text=True,timeout=3)
        if result.returncode==0:
            addresses={row['ifname']:[a['local'] for a in row.get('addr_info',[]) if a.get('family')=='inet' and not a['local'].startswith(('127.','169.254.'))] for row in json.loads(result.stdout)}
        route=subprocess.run([executable,'-j','route','show','default'],capture_output=True,text=True,timeout=3)
        if route.returncode==0:
            routes=json.loads(route.stdout)
            if routes:
                metric=min(r.get('metric',0) for r in routes)
                selected={r.get('dev') for r in routes if r.get('metric',0)==metric}
                if len(selected)==1:preferred=next(iter(selected))
    else:
        executable=shutil.which('ifconfig',path='/sbin:/usr/sbin:/usr/bin:/bin')
        if executable:
            result=subprocess.run([executable],capture_output=True,text=True,timeout=3)
            current=None
            for line in result.stdout.splitlines():
                if line and not line[0].isspace():current=line.split(':',1)[0];addresses.setdefault(current,[])
                found=re.search(r'\binet (\d+\.\d+\.\d+\.\d+)',line)
                if found and current and not found[1].startswith(('127.','169.254.')):addresses[current].append(found[1])
        route=shutil.which('route',path='/sbin:/usr/sbin:/usr/bin:/bin')
        if route:
            result=subprocess.run([route,'-n','get','default'],capture_output=True,text=True,timeout=3)
            found=re.search(r'interface:\s+(\S+)',result.stdout)
            if found:preferred=found[1]
    addresses={key:value for key,value in addresses.items() if value}
    return addresses,preferred


def interfaces():
    choices,_=network_choices()
    return sorted({value for values in choices.values() for value in values})


def select_interface(selection=None):
    choices,preferred=network_choices()
    if selection:
        if selection in choices:
            if len(choices[selection])==1:return choices[selection][0]
        else:
            # Explicit IP is still useful for loopback laboratories and OS aliases.
            try:socket.inet_aton(selection);return selection
            except OSError:pass
    elif preferred in choices and len(choices[preferred])==1:return choices[preferred][0]
    elif len(choices)==1 and len(next(iter(choices.values())))==1:return next(iter(choices.values()))[0]
    raise BridgeError('Choose --interface NAME from: '+', '.join(sorted(choices))+'; OS default route is ambiguous. No traffic or network changes were made.')


def ensure_session(state, home, bind=None):
    state=private_directory(state,create=True)
    try:existing=control(state,'status')
    except (OSError,BridgeError):existing=None
    if existing:
        if existing.get('bridge_home')!=str(Path(home).absolute()):raise BridgeError('Existing LAN session belongs to another bridge home')
        return {**existing,'started_by_caller':False}
    bind=select_interface(bind)
    subprocess.Popen([sys.executable,'-I','-S','-B',str(Path(__file__).resolve()),'serve','--state',str(state),'--home',str(home),'--bind',bind],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    deadline=time.monotonic()+15
    while time.monotonic()<deadline:
        try:return {**control(state,'status'),'started_by_caller':True}
        except (OSError,BridgeError):time.sleep(.1)
    raise BridgeError('LAN session could not start; check interface, OS local-network permission or occupied discovery port')


def connect_code(state, home, code, bind=None, link=None):
    session_status=ensure_session(state,home,bind)
    # Pairing must run inside the resident session to preserve its private registry.
    state=private_directory(state)
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as stream:
        stream.settimeout(25);stream.connect(str(state/'session-control.sock'))
        stream.sendall((encode({'op':'connect','code':code,'link':link})+'\n').encode())
        with stream.makefile('rb') as file:answer=decode(file.readline(MAX_WIRE+1))
    if answer.get('ok') is not True:raise BridgeError('Pairing did not complete; check code, expiry, permission and peer state before retrying')
    return {**answer['data'],'lan_session_owned':session_status['started_by_caller']}


def serve_main():
    import argparse
    parser=argparse.ArgumentParser(description='Session-owned LAN channel; no global service or model')
    parser.add_argument('action',choices=['serve']);parser.add_argument('--state',required=True);parser.add_argument('--home',required=True);parser.add_argument('--bind',required=True)
    args=parser.parse_args();session=Session(args.state,args.home,args.bind)
    for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,lambda *_:session.stop.set())
    try:
        session.start()
        while not session.stop.wait(.5):pass
    finally:session.close()



def invitation_input(text):
    text=text.strip()
    if text.startswith('cslpair:'):
        try:
            payload=decode(base64.urlsafe_b64decode(text[8:]+'='*(-len(text[8:])%4)))
            if set(payload)!={'endpoint','proof','code'}:raise ValueError()
            code_bytes(payload['code']);link_endpoint(text,payload['code'])
            return payload['code'],text
        except (ValueError,TypeError,KeyError):raise BridgeError('Invalid private invitation link') from None
    code_bytes(text)
    return text,None

if __name__=='__main__':serve_main()
