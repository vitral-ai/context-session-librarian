"""Session-owned outgoing relay client; application TLS stays end-to-end."""
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import socket
import ssl
import threading
import time
import uuid

import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))

from csl_bridge_store import BridgeError, Store, decode, encode, name, validate
from csl_bridge_lan import Session, save, private_json, code_bytes, mac, token_value, apply_pair_result, register_peer, COMMUNICATION_OPS
from csl_bridge_relay import rpc, open_tunnel, InnerTLS, read_line, send_line, hex_value, validate_profile, RelayFailure


def relay_endpoint(value):
    if not isinstance(value,dict) or set(value)!={'route','fingerprint'}:raise BridgeError('Invalid opaque peer route')
    hex_value(value['route']);hex_value(value['fingerprint']);return value


def client_context():
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.check_hostname=False;context.verify_mode=ssl.CERT_NONE
    context.minimum_version=ssl.TLSVersion.TLSv1_2;return context


def peer_request(profile,remote,payload,timeout=90):
    relay_endpoint(remote)
    outer=open_tunnel(profile,{'op':'connect','route':remote['route']},timeout)
    if outer is None:raise BridgeError('Peer relay route unavailable; inspect receipt')
    inner=None
    try:
        inner=InnerTLS(outer,client_context(),pin=remote['fingerprint'])
        send_line(inner,payload);response=read_line(inner,65536)
        if not isinstance(response,dict) or set(response)!={'ok','data'} or response['ok'] is not True:raise BridgeError('Peer rejected request; inspect receipt')
        return response['data']
    finally:
        if inner:inner.close()
        else:outer.close()


def validate_peer_config(config):
    if not {'transport','endpoint','token_file','relay_profile','allow_agents','allow_ops'} <= set(config) or set(config)-{'transport','endpoint','token_file','relay_profile','allow_agents','allow_ops','allow_sources','send_session'} or config['transport']!='relay':raise BridgeError('Invalid relay peer profile')
    if 'allow_sources' in config and (not isinstance(config['allow_sources'],list) or any(name(a)!=a for a in config['allow_sources'])):raise BridgeError('Invalid paired source scope')
    if 'send_session' in config and type(config['send_session']) is not bool:raise BridgeError('Invalid sender session option')
    relay_endpoint(config['endpoint']);validate_profile(private_json(config['relay_profile']));token_value(private_json(config['token_file'])['token'])
    if not config['allow_agents'] or any(name(a)!=a for a in config['allow_agents']):raise BridgeError('Invalid local relay agent scope')
    if set(config['allow_ops'])!=set(COMMUNICATION_OPS):raise BridgeError('Only communication scope is supported')
    return config


def exchange(config,identity,body):
    validate_peer_config(config);validate(body)
    try:return peer_request(private_json(config['relay_profile']),config['endpoint'],{'op':'call','token':private_json(config['token_file'])['token'],'request':body})
    except (OSError,ssl.SSLError,ValueError):raise BridgeError('Relay outcome uncertain; inspect same-ID receipt before retry') from None


class RelaySession(Session):
    def __init__(self,state,home,profile_path,heartbeat_interval=None):
        self.profile_path=Path(profile_path).absolute();self.profile=validate_profile(private_json(self.profile_path))
        super().__init__(state,home,bind='127.0.0.1',discovery_port=0)
        # No LAN listener starts. Reuse only the private peer TLS identity and grant logic.
        self.server.server_close()
        binding=self.state/'relay-binding.json'
        expected={'bridge_home':str(self.home),'thread_id':self.thread_id}
        if binding.exists() and private_json(binding)!=expected:raise BridgeError('Relay state belongs to another existing conversation')
        save(binding,expected)
        self.server_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);self.server_context.minimum_version=ssl.TLSVersion.TLSv1_2
        self.server_context.load_cert_chain(self.state/'certificate.pem',self.state/'certificate-key.pem')
        route_path=self.state/'relay-route.json'
        if route_path.exists():self.route=private_json(route_path)
        else:self.route={'route':secrets.token_hex(32),'lease':secrets.token_hex(32)};save(route_path,self.route)
        hex_value(self.route['route']);hex_value(self.route['lease'])
        self.remote={'route':self.route['route'],'fingerprint':self.fingerprint}
        self.workers=[];self.peer_slots=threading.BoundedSemaphore(8)
        self.needs_reverification=threading.Event();self.status_lock=threading.RLock();self.registered=threading.Event();self.heartbeat_wakeup=threading.Event();self.heartbeat_interval=heartbeat_interval
        self.relay_status={'phase':'disconnected','peer_ready':False,'last_error':None,'heartbeat_last_success':None,'accept_last_success':None,'loop_observed_at':time.time()}

    def validate_endpoint(self,value):return relay_endpoint(value)

    def peer_config(self,remote,token_path,source_agent):
        return {'transport':'relay','endpoint':relay_endpoint(remote),'token_file':str(token_path),'relay_profile':str(self.profile_path),'allow_agents':self.agents,'allow_ops':COMMUNICATION_OPS,'allow_sources':[name(source_agent)]}

    def publish(self):
        rpc(self.profile,{'op':'register',**self.route})
        invitation=self.invite();code=code_bytes(invitation['code'])
        locator=hashlib.sha256(b'CSL-relay-invitation-v1'+code).hexdigest()
        descriptor={'endpoint':self.remote,'identity':self.identity,'nonce':secrets.token_hex(16),'expires_at':invitation['expires_at']}
        signed={'descriptor':descriptor,'proof':mac(code,descriptor)}
        rpc(self.profile,{'op':'publish',**self.route,'locator':locator,'descriptor':signed})
        # Route and code stay separate from ordinary logs/status; code only returned to local UI.
        return {'code':invitation['code'],'expires_at':invitation['expires_at'],'scope':'communication','mode':'outgoing-relay','note':'Share this code manually. One use, five minutes; no QR. Relay pairing is not model activity.'}

    def connect_code(self,code):
        secret=code_bytes(code);path=self.state/'pair-attempt.json'
        if path.exists() and private_json(path).get('state')=='uncertain':raise BridgeError('Reconcile the existing pairing receipt; do not create another grant')
        locator=hashlib.sha256(b'CSL-relay-invitation-v1'+secret).hexdigest()
        signed=rpc(self.profile,{'op':'lookup','locator':locator})
        if not isinstance(signed,dict) or set(signed)!={'descriptor','proof'}:raise BridgeError('Invalid rendezvous descriptor')
        descriptor=signed['descriptor']
        if not isinstance(descriptor,dict) or set(descriptor)!={'endpoint','identity','nonce','expires_at'}:raise BridgeError('Invalid peer invitation')
        if not isinstance(signed['proof'],str) or not hmac.compare_digest(signed['proof'],mac(secret,descriptor)):raise BridgeError('Invitation authentication failed; no peer credentials sent')
        if not isinstance(descriptor['expires_at'],(float,int)) or not time.time()<descriptor['expires_at']<=time.time()+305:raise BridgeError('Invitation expired')
        peer=name(descriptor['identity']);remote=relay_endpoint(descriptor['endpoint'])
        rpc(self.profile,{'op':'register',**self.route})
        offer={'identity':self.identity,'agent':self.agents[0],'endpoint':self.remote,'token':secrets.token_hex(32),'pair_id':str(uuid.uuid4())}
        attempt={'state':'uncertain','remote':remote,'offer':offer,'expected_identity':peer};save(path,attempt)
        try:
            result=peer_request(self.profile,remote,{'op':'pair','offer':offer,'proof':mac(secret,{'offer':offer,'endpoint':remote})},timeout=30)
            if result.get('identity')!=peer:raise BridgeError('Peer identity differs from authenticated invitation')
            return apply_pair_result(self,attempt,result)
        except (BridgeError,OSError,ValueError,ssl.SSLError):
            return {'state':'uncertain','pair_id':offer['pair_id'],'note':'Pairing may have committed. Reconcile this receipt; no automatic grant replay.'}

    def reconcile_pair(self):
        attempt=private_json(self.state/'pair-attempt.json')
        if attempt.get('state')!='uncertain':return attempt
        pair_id=attempt['offer']['pair_id'];remote=relay_endpoint(attempt['remote'])
        result=peer_request(self.profile,remote,{'op':'pair_receipt','pair_id':pair_id,'proof':mac(bytes.fromhex(attempt['offer']['token']),{'pair_id':pair_id,'endpoint':remote})},timeout=30)
        if result.get('state')!='accepted':return {'state':'uncertain','pair_id':pair_id,'note':'No receipt observed. Do not re-create a grant.'}
        if result['result'].get('identity')!=attempt['expected_identity']:raise BridgeError('Peer identity differs from invitation')
        return apply_pair_result(self,attempt,result['result'])

    def handle_peer(self,outer):
        inner=None
        try:
            inner=InnerTLS(outer,self.server_context,server_side=True)
            value=read_line(inner,65536)
            result=self.incoming(value)
            send_line(inner,{'ok':True,'data':result})
            self.observe(last_peer_response=time.time())
        except (BridgeError,OSError,ValueError,TypeError,KeyError,ssl.SSLError):
            if inner:
                try:send_line(inner,{'ok':False,'data':{'error':'Peer request rejected'}})
                except (BridgeError,OSError,ssl.SSLError):pass
        finally:
            if inner:inner.close()
            else:outer.close()
            self.peer_slots.release()

    def error_reason(self,exc):
        if isinstance(exc,RelayFailure):return exc.reason
        if isinstance(exc,TimeoutError):return 'transport_timeout'
        if isinstance(exc,ssl.SSLError):return 'tls_error'
        if isinstance(exc,OSError):return 'transport_io'
        return 'protocol_error'

    def heartbeat_loop(self):
        retry=1
        while not self.stop.is_set():
            method='renew' if self.registered.is_set() else 'register'
            self.observe(heartbeat_stage=method,heartbeat_last_attempt=time.time())
            try:
                reply=rpc(self.profile,{'op':method,**self.route},timeout=10)
                was_registered=self.registered.is_set()
                self.registered.set()
                values={'heartbeat_stage':'waiting','heartbeat_last_success':time.time(),'heartbeat_last_error':None,'phase':'relay_connected_peer_unverified','last_error':None}
                if not was_registered or self.needs_reverification.is_set():
                    values['connected_at']=time.time();self.needs_reverification.clear()
                self.observe(**values)
                retry=1
                lease=reply.get('lease_seconds',self.relay_status.get('lease_seconds',180))
                if type(lease) not in (int,float) or not .03<=lease<=3600:raise BridgeError('Invalid relay lease budget')
                self.observe(lease_seconds=lease)
                interval=self.heartbeat_interval if self.heartbeat_interval is not None else min(45,lease/3)
                self.heartbeat_wakeup.wait(interval);self.heartbeat_wakeup.clear()
            except (BridgeError,OSError,ValueError,TypeError,KeyError,ssl.SSLError) as exc:
                self.registered.clear()
                self.observe(phase='reconnecting',heartbeat_stage='backoff',heartbeat_last_error=self.error_reason(exc),last_error=self.error_reason(exc),peer_ready=False)
                self.heartbeat_wakeup.wait(retry);self.heartbeat_wakeup.clear();retry=min(retry*2,30)

    def run(self):
        heartbeat=threading.Thread(target=self.heartbeat_loop,daemon=True);heartbeat.start()
        retry=1
        try:
            while not self.stop.is_set():
                if not self.registered.wait(.2):continue
                try:
                    self.observe(accept_stage='waiting',accept_last_attempt=time.time())
                    outer=open_tunnel(self.profile,{'op':'accept',**self.route},timeout=35)
                    self.observe(accept_stage='returned',accept_last_success=time.time(),accept_last_error=None)
                    if outer:
                        if not self.peer_slots.acquire(blocking=False):
                            self.observe(accept_last_error='local_peer_capacity');outer.close();continue
                        thread=threading.Thread(target=self.handle_peer,args=(outer,),daemon=True);thread.start();self.workers.append(thread)
                        self.workers=[t for t in self.workers if t.is_alive()]
                    retry=1
                except (BridgeError,OSError,ValueError,TypeError,KeyError,ssl.SSLError) as exc:
                    self.needs_reverification.set()
                    self.observe(phase='reconnecting',accept_stage='backoff',accept_last_error=self.error_reason(exc),last_error=self.error_reason(exc),peer_ready=False)
                    if isinstance(exc,RelayFailure) and exc.reason in ('route_missing','route_revoked','route_auth_failed'):self.registered.clear()
                    self.heartbeat_wakeup.set()
                    if self.stop.wait(retry):break
                    retry=min(retry*2,30)
        finally:
            self.stop.set();self.heartbeat_wakeup.set();heartbeat.join(11)
            self.observe(phase='stopped',peer_ready=False,accept_stage='stopped',heartbeat_stage='stopped')

    def observe(self,**values):
        with self.status_lock:
            self.relay_status.update(values,loop_observed_at=time.time())
            save(self.state/'relay-status.json',dict(self.relay_status))

    def publish_status(self):self.observe()

    def revoke_credentials(self):
        with self.lock:
            self.registry={};self.receipts={};self.peer_agents={}
            save(self.state/'registry.json',{});save(self.state/'pair-receipts.json',{});save(self.state/'peers.json',{})
            save(self.state/'invitation.json',{'used':True,'expires':0,'failures':6,'code':''})
            save(self.state/'pair-attempt.json',{'state':'revoked'})
            self.invitation=None
            for path in self.state.glob('to-*.json'):save(path,{'token':''})

    def close(self,revoke=True):
        self.power.close()
        self.stop.set();self.heartbeat_wakeup.set()
        if revoke:
            self.revoke_credentials()
            try:rpc(self.profile,{'op':'revoke',**self.route},timeout=5)
            except (BridgeError,OSError,ValueError):pass
        for thread in self.workers:thread.join(.1)
        self.owner_lock.close()


def session_control(state,operation,**values):
    from csl_bridge_store import private_directory
    state=private_directory(state)
    with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as stream:
        stream.settimeout(100 if operation in ('connect','reconcile') else 35)
        stream.connect(str(state/'relay-control.sock'));send_line(stream,{'op':operation,**values});result=read_line(stream,65536)
    if result.get('ok') is not True:raise BridgeError('Relay session operation not confirmed; preserve pairing/task IDs and inspect state')
    return result['data']


def start_control(session):
    import socketserver
    import stat
    path=session.state/'relay-control.sock'
    if path.exists():
        info=path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid!=os.getuid():raise BridgeError('Unexpected relay control path')
        path.unlink()
    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(5)
            try:
                value=decode(self.rfile.readline(4097))
                if value=={'op':'share'}:data=session.publish()
                elif isinstance(value,dict) and set(value)=={'op','code'} and value['op']=='connect':data=session.connect_code(value['code'])
                elif value=={'op':'reconcile'}:data=session.reconcile_pair()
                elif isinstance(value,dict) and value.get('op')=='keep_awake' and set(value)=={'op','enabled'}:data=session.power.set(value['enabled'])
                elif value=={'op':'stop'}:
                    session.power.close()
                    session.revoke_credentials()
                    session.stop.set()
                    try:rpc(session.profile,{'op':'revoke',**session.route},timeout=5)
                    except (BridgeError,OSError,ValueError):pass
                    data={'stopping':True,'revocation':'local grants revoked; relay revoke best effort; accepted work is not cancelled'}
                elif value=={'op':'status'}:
                    data={'running':True,'identity':session.identity,'bridge_home':str(session.home),'mode':'relay','power':session.power.status(),**session.relay_status,
                          'peers':[{'identity':p,'agent':a} for p,a in session.peer_agents.items()]}
                else:raise BridgeError('Invalid local relay operation')
                self.wfile.write((encode({'ok':True,'data':data})+'\n').encode())
            except (BridgeError,OSError,ValueError,TypeError,KeyError):
                try:self.wfile.write(b'{"ok":false,"data":{"error":"Session request rejected; inspect receipt before retry"}}\n')
                except OSError:pass
    server=socketserver.ThreadingUnixStreamServer(str(path),Handler);server.daemon_threads=True;os.chmod(path,0o600)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    return server,thread


def ensure_session(state,home,profile_path):
    import subprocess
    import sys
    from csl_bridge_store import private_directory
    state=private_directory(state,create=True);validate_profile(private_json(profile_path))
    try:current=session_control(state,'status')
    except (BridgeError,OSError):current=None
    if current:
        if current['bridge_home']!=str(Path(home).absolute()):raise BridgeError('Relay session belongs to another conversation')
        return {**current,'started_by_caller':False}
    subprocess.Popen([sys.executable,'-I','-S','-B',str(Path(__file__).resolve()),'serve','--state',str(state),'--home',str(home),'--profile',str(Path(profile_path).absolute())],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    end=time.monotonic()+15
    while time.monotonic()<end:
        try:return {**session_control(state,'status'),'started_by_caller':True}
        except (BridgeError,OSError):time.sleep(.1)
    raise BridgeError('Relay client could not start. Check provisioned profile, enrollment access and existing session owner')


def main():
    import argparse
    import signal
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['serve']);parser.add_argument('--state',required=True);parser.add_argument('--home',required=True);parser.add_argument('--profile',required=True)
    args=parser.parse_args();session=RelaySession(args.state,args.home,args.profile)
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:(session.power.close(),session.stop.set()))
    server,thread=start_control(session)
    try:session.run()
    finally:server.shutdown();server.server_close();thread.join(2);session.close()

if __name__=='__main__':main()
