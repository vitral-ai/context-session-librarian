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
from csl_bridge_relay import rpc, open_tunnel, InnerTLS, read_line, send_line, hex_value, validate_profile


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
    if set(config)!={'transport','endpoint','token_file','relay_profile','allow_agents','allow_ops'} or config['transport']!='relay':raise BridgeError('Invalid relay peer profile')
    relay_endpoint(config['endpoint']);validate_profile(private_json(config['relay_profile']));token_value(private_json(config['token_file'])['token'])
    if not config['allow_agents'] or any(name(a)!=a for a in config['allow_agents']):raise BridgeError('Invalid local relay agent scope')
    if set(config['allow_ops'])!=set(COMMUNICATION_OPS):raise BridgeError('Only communication scope is supported')
    return config


def exchange(config,identity,body):
    validate_peer_config(config);validate(body)
    try:return peer_request(private_json(config['relay_profile']),config['endpoint'],{'op':'call','token':private_json(config['token_file'])['token'],'request':body})
    except (OSError,ssl.SSLError,ValueError):raise BridgeError('Relay outcome uncertain; inspect same-ID receipt before retry') from None


class RelaySession(Session):
    def __init__(self,state,home,profile_path):
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
        self.relay_status={'phase':'disconnected','peer_ready':False,'last_error':None}

    def validate_endpoint(self,value):return relay_endpoint(value)

    def peer_config(self,remote,token_path):
        return {'transport':'relay','endpoint':relay_endpoint(remote),'token_file':str(token_path),'relay_profile':str(self.profile_path),'allow_agents':self.agents,'allow_ops':COMMUNICATION_OPS}

    def publish(self):
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
            self.relay_status.update(last_peer_response=time.time())
        except (BridgeError,OSError,ValueError,TypeError,KeyError,ssl.SSLError):
            if inner:
                try:send_line(inner,{'ok':False,'data':{'error':'Peer request rejected'}})
                except (BridgeError,OSError,ssl.SSLError):pass
        finally:
            if inner:inner.close()
            else:outer.close()
            self.peer_slots.release()

    def run(self):
        retry=1
        while not self.stop.is_set():
            try:
                rpc(self.profile,{'op':'register',**self.route})
                self.relay_status.update(phase='relay_connected_peer_unverified',last_error=None,connected_at=time.time())
                self.publish_status()
                while not self.stop.is_set():
                    rpc(self.profile,{'op':'renew',**self.route})
                    outer=open_tunnel(self.profile,{'op':'accept',**self.route},timeout=35)
                    if outer:
                        if not self.peer_slots.acquire(blocking=False):outer.close();continue
                        thread=threading.Thread(target=self.handle_peer,args=(outer,),daemon=True);thread.start();self.workers.append(thread)
                        self.workers=[t for t in self.workers if t.is_alive()]
                    retry=1
            except (BridgeError,OSError,ValueError,ssl.SSLError):
                self.relay_status.update(phase='reconnecting',last_error='relay_unavailable',peer_ready=False);self.publish_status()
                if self.stop.wait(retry):break
                retry=min(retry*2,30)
        self.relay_status.update(phase='stopped',peer_ready=False);self.publish_status()

    def publish_status(self):save(self.state/'relay-status.json',self.relay_status)

    def close(self,revoke=True):
        self.stop.set()
        if revoke:
            try:rpc(self.profile,{'op':'revoke',**self.route},timeout=5)
            except (BridgeError,OSError,ValueError):pass
            self.registry={};self.receipts={};self.peer_agents={}
            save(self.state/'registry.json',{});save(self.state/'pair-receipts.json',{});save(self.state/'peers.json',{})
            save(self.state/'invitation.json',{'used':True,'expires':0,'failures':6,'code':''})
            save(self.state/'pair-attempt.json',{'state':'revoked'})
            for path in self.state.glob('to-*.json'):save(path,{'token':''})
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
                elif value=={'op':'stop'}:session.stop.set();data={'stopping':True,'revocation':'future calls only; accepted work is not cancelled'}
                elif value=={'op':'status'}:
                    data={'running':True,'identity':session.identity,'bridge_home':str(session.home),'mode':'relay',**session.relay_status,
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
    for sig in (signal.SIGINT,signal.SIGTERM):signal.signal(sig,lambda *_:session.stop.set())
    server,thread=start_control(session)
    try:session.run()
    finally:server.shutdown();server.server_close();thread.join(2);session.close()

if __name__=='__main__':main()
