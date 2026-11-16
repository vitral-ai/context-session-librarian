"""Bounded opaque TCP relay over authenticated TLS; no model, shell or Codex auth.

The relay forwards inner TLS bytes and sees routing metadata, IPs and lengths.
It never receives invitation codes, peer private keys, or application credentials.
"""
import argparse
from collections import defaultdict, deque
import hashlib
import hmac
import os
from pathlib import Path
import secrets
import socket
import socketserver
import ssl
import sys
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parent))
from csl_bridge_store import BridgeError, decode, encode

CONTROL_LIMIT=4096


def read_line(stream, limit=CONTROL_LIMIT):
    data=bytearray()
    while len(data)<=limit:
        byte=stream.recv(1)
        if not byte:raise BridgeError('Relay control ended')
        data.extend(byte)
        if byte==b'\n':return decode(bytes(data))
    raise BridgeError('Relay control exceeds limit')


def send_line(stream,value):stream.sendall((encode(value)+'\n').encode())


def hex_value(value,size=64):
    if not isinstance(value,str) or len(value)!=size or any(c not in '0123456789abcdef' for c in value):raise BridgeError('Invalid relay identifier')
    return value


class Limits:
    def __init__(self,routes=32,connections=16,lease=180,invite=300,wait=25,idle=60,lifetime=180,bytes=8*1024*1024,rate=60):
        self.routes=routes;self.connections=connections;self.lease=lease;self.invite=invite;self.wait=wait
        self.idle=idle;self.lifetime=lifetime;self.bytes=bytes;self.rate=rate


class Broker:
    def __init__(self,limits=None,clock=time.monotonic,enrollment=None):
        self.enrollment=enrollment
        self.limits=limits or Limits();self.clock=clock;self.lock=threading.RLock()
        self.routes={};self.invitations={};self.rate=defaultdict(deque);self.global_rate=deque();self.active=0

    def cleanup(self):
        now=self.clock()
        for key,row in list(self.routes.items()):
            if row['expires']<now:
                row['revoked']=True;row['condition'].notify_all();del self.routes[key]
        for key,row in list(self.invitations.items()):
            if row['expires']<now:del self.invitations[key]
        while self.global_rate and now-self.global_rate[0]>=60:self.global_rate.popleft()
        for key,values in list(self.rate.items()):
            while values and now-values[0]>=60:values.popleft()
            if not values:del self.rate[key]

    def admit(self,address):
        with self.lock:
            self.cleanup()
            if self.active>=self.limits.connections:raise BridgeError('Relay capacity reached')
            if address not in self.rate and len(self.rate)>=1024:raise BridgeError('Relay rate table capacity reached')
            if len(self.global_rate)>=self.limits.rate*4:raise BridgeError('Relay global rate limit')
            values=self.rate[address]
            if len(values)>=self.limits.rate:raise BridgeError('Relay rate limit')
            values.append(self.clock());self.global_rate.append(self.clock());self.active+=1

    def release(self):
        with self.lock:self.active-=1

    def authenticated(self,route,lease):
        hex_value(route);hex_value(lease)
        row=self.routes.get(route)
        if not row or not hmac.compare_digest(row['lease'],lease):raise BridgeError('Relay route unavailable')
        row['expires']=self.clock()+self.limits.lease
        return row

    def control(self,value):
        with self.lock:
            self.cleanup()
            if not isinstance(value,dict):raise BridgeError('Invalid relay frame')
            op=value.get('op')
            if op=='register' and set(value)=={'op','route','lease'}:
                route=hex_value(value['route']);lease=hex_value(value['lease'])
                if route in self.routes:
                    self.authenticated(route,lease)
                else:
                    if len(self.routes)>=self.limits.routes:raise BridgeError('Relay route capacity reached')
                    self.routes[route]={'lease':lease,'expires':self.clock()+self.limits.lease,'pending':deque(),'condition':threading.Condition(self.lock),'revoked':False,'accepting':False}
                return {'registered':route,'lease_seconds':self.limits.lease}
            if op in ('renew','revoke') and set(value)=={'op','route','lease'}:
                row=self.authenticated(value['route'],value['lease'])
                if op=='revoke':
                    row['revoked']=True;row['condition'].notify_all();del self.routes[value['route']]
                    for key,entry in list(self.invitations.items()):
                        if entry['route']==value['route']:del self.invitations[key]
                return {'state':op}
            if op=='publish' and set(value)=={'op','route','lease','locator','descriptor'}:
                self.authenticated(value['route'],value['lease']);locator=hex_value(value['locator'])
                if len(encode(value['descriptor']).encode())>2048:raise BridgeError('Invitation metadata too large')
                if len(self.invitations)>=self.limits.routes*2 and locator not in self.invitations:raise BridgeError('Invitation capacity reached')
                self.invitations[locator]={'route':value['route'],'descriptor':value['descriptor'],'expires':self.clock()+self.limits.invite}
                return {'published':True}
            if op=='lookup' and set(value)=={'op','locator'}:
                row=self.invitations.get(hex_value(value['locator']))
                if not row:raise BridgeError('Invitation unavailable')
                return row['descriptor']
            raise BridgeError('Invalid relay operation')

    def tunnel(self,stream,value):
        if not isinstance(value,dict):raise BridgeError('Invalid tunnel')
        op=value.get('op')
        with self.lock:
            self.cleanup()
            if op=='accept' and set(value)=={'op','route','lease'}:
                row=self.authenticated(value['route'],value['lease'])
                if row['accepting']:raise BridgeError('Route already has an accept waiter')
                row['accepting']=True;deadline=self.clock()+self.limits.wait
                try:
                    while not row['pending'] and not row['revoked']:
                        left=deadline-self.clock()
                        if left<=0:send_line(stream,{'ok':True,'data':{'waiting':True}});return
                        row['condition'].wait(left)
                    if row['revoked']:raise BridgeError('Route revoked')
                    pending=row['pending'].popleft();pending['server']=stream
                    pending['ready'].set()
                finally:row['accepting']=False
            elif op=='connect' and set(value)=={'op','route'}:
                row=self.routes.get(hex_value(value['route']))
                if not row or len(row['pending'])>=8:raise BridgeError('Route unavailable or busy')
                pending={'client':stream,'server':None,'ready':threading.Event(),'done':threading.Event()}
                row['pending'].append(pending);row['condition'].notify_all()
            else:raise BridgeError('Invalid tunnel operation')
        if op=='accept':
            pending['done'].wait(self.limits.lifetime+5);return
        if not pending['ready'].wait(self.limits.wait):
            with self.lock:
                if pending in row['pending']:row['pending'].remove(pending)
            raise BridgeError('Peer is not connected; no application request was sent')
        other=pending['server']
        try:
            send_line(stream,{'ok':True,'data':{'tunnel':True}});send_line(other,{'ok':True,'data':{'tunnel':True}})
            self.forward(stream,other)
        finally:pending['done'].set()

    def forward(self,left,right):
        stop=threading.Event();lock=threading.Lock();started=self.clock();last=[started];count=[0]
        def copy(source,target):
            source.settimeout(min(self.limits.idle,1))
            try:
                while not stop.is_set():
                    now=self.clock()
                    if now-started>self.limits.lifetime or now-last[0]>self.limits.idle:break
                    try:data=source.recv(16384)
                    except socket.timeout:continue
                    if not data:break
                    with lock:
                        count[0]+=len(data)
                        if count[0]>self.limits.bytes:break
                        last[0]=self.clock()
                    target.sendall(data)
            except (OSError,ssl.SSLError):pass
            finally:
                stop.set()
                try:target.shutdown(socket.SHUT_WR)
                except OSError:pass
        worker=threading.Thread(target=copy,args=(left,right),daemon=True);worker.start();copy(right,left);worker.join(2)


class RelayServer(socketserver.ThreadingTCPServer):
    daemon_threads=True;allow_reuse_address=True
    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):request.close();return
        try:super().process_request(request,client_address)
        except BaseException:self.slots.release();raise

    def process_request_thread(self, request, client_address):
        try:super().process_request_thread(request,client_address)
        finally:self.slots.release()

    def __init__(self,address,context,broker=None):
        self.context=context;self.broker=broker or Broker()
        owner=self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                stream=None;admitted=False
                try:
                    owner.broker.admit(self.client_address[0]);admitted=True
                    self.request.settimeout(5)
                    stream=owner.context.wrap_socket(self.request,server_side=True)
                    envelope=read_line(stream)
                    if not isinstance(envelope,dict) or set(envelope)!={'enrollment','request'}:raise BridgeError('Enrollment required')
                    if owner.broker.enrollment is None or not hmac.compare_digest(hex_value(envelope['enrollment']),owner.broker.enrollment):raise BridgeError('Enrollment rejected')
                    value=envelope['request']
                    if isinstance(value,dict) and value.get('op') in ('accept','connect'):owner.broker.tunnel(stream,value)
                    else:send_line(stream,{'ok':True,'data':owner.broker.control(value)})
                except (BridgeError,OSError,ValueError,TypeError,KeyError):
                    if stream:
                        try:send_line(stream,{'ok':False,'data':{'error':'Relay request rejected'}})
                        except OSError:pass
                finally:
                    if stream:stream.close()
                    if admitted:owner.broker.release()
        self.slots=threading.BoundedSemaphore(self.broker.limits.connections)
        super().__init__(address,Handler)


def validate_profile(profile):
    if not isinstance(profile,dict) or set(profile)!={'host','port','fingerprint','enrollment_file'}:raise BridgeError('Use the operator-provisioned relay profile')
    host=profile['host'];port=profile['port']
    if not isinstance(host,str) or not host or len(host)>253 or any(c.isspace() for c in host):raise BridgeError('Invalid relay host')
    if type(port) is not int or not 1<=port<=65535:raise BridgeError('Invalid relay port')
    hex_value(profile['fingerprint']);enrollment_value(profile);return profile


def enrollment_value(profile):
    path=Path(profile['enrollment_file']);info=path.lstat()
    if path.is_symlink() or not path.is_file() or info.st_uid!=os.getuid() or info.st_mode&0o077:raise BridgeError('Relay enrollment must be a private owned file')
    return hex_value(path.read_text().strip())


def envelope(profile,value):return {'enrollment':enrollment_value(profile),'request':value}


def dial(profile, timeout=90):
    validate_profile(profile)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.check_hostname=False;context.verify_mode=ssl.CERT_NONE;context.minimum_version=ssl.TLSVersion.TLSv1_2
    raw=socket.create_connection((profile['host'],profile['port']),timeout=timeout)
    try:
        stream=context.wrap_socket(raw,server_hostname=profile['host'])
        if not hmac.compare_digest(hashlib.sha256(stream.getpeercert(binary_form=True)).hexdigest(),profile['fingerprint']):
            stream.close();raise BridgeError('Relay TLS identity differs from provisioned pin')
        return stream
    except BaseException:raw.close();raise


def rpc(profile,value, timeout=30):
    with dial(profile,timeout) as stream:
        send_line(stream,envelope(profile,value));answer=read_line(stream)
    if answer.get('ok') is not True:raise BridgeError('Relay refused operation; preserve IDs and inspect state')
    return answer['data']


def open_tunnel(profile,value,timeout=90):
    stream=dial(profile,timeout)
    try:
        send_line(stream,envelope(profile,value));answer=read_line(stream)
        if answer.get('ok') is not True:raise BridgeError('Relay peer unavailable; outcome uncertain')
        if not answer['data'].get('tunnel'):stream.close();return None
        return stream
    except BaseException:stream.close();raise


class InnerTLS:
    """TLS over an arbitrary duplex stream, including an outer TLS connection."""
    def __init__(self,transport,context,server_side=False,pin=None):
        self.transport=transport;self.input=ssl.MemoryBIO();self.output=ssl.MemoryBIO()
        self.tls=context.wrap_bio(self.input,self.output,server_side=server_side,server_hostname=None)
        self.closed=False
        self.drive(self.tls.do_handshake)
        if pin and not hmac.compare_digest(hashlib.sha256(self.tls.getpeercert(binary_form=True)).hexdigest(),hex_value(pin)):
            self.close();raise BridgeError('Peer TLS pin mismatch; no application payload sent')

    def flush(self):
        while self.output.pending:self.transport.sendall(self.output.read(16384))

    def drive(self,function,*args):
        while True:
            try:
                value=function(*args);self.flush();return value
            except ssl.SSLWantWriteError:self.flush()
            except ssl.SSLWantReadError:
                self.flush();data=self.transport.recv(16384)
                if not data:
                    self.input.write_eof()
                    try:return function(*args)
                    except (ssl.SSLError,ssl.SSLWantReadError):raise BridgeError('Peer TLS disconnected; outcome uncertain') from None
                self.input.write(data)

    def sendall(self,data):
        offset=0
        while offset<len(data):offset+=self.drive(self.tls.write,data[offset:offset+16384])

    def recv(self,size):return self.drive(self.tls.read,size)

    def close(self):
        if self.closed:return
        self.closed=True
        try:self.tls.unwrap();self.flush()
        except (ssl.SSLError,OSError):pass
        self.transport.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['serve']);parser.add_argument('--bind',required=True);parser.add_argument('--port',required=True,type=int)
    parser.add_argument('--certificate',required=True,type=Path);parser.add_argument('--private-key',required=True,type=Path)
    parser.add_argument('--enrollment-file',required=True,type=Path,help='Private256bithex enrollment file; never a command argument token')
    parser.add_argument('--max-routes',type=int,default=32);parser.add_argument('--max-connections',type=int,default=16)
    parser.add_argument('--idle-seconds',type=int,default=60);parser.add_argument('--tunnel-seconds',type=int,default=180);parser.add_argument('--tunnel-bytes',type=int,default=8*1024*1024)
    args=parser.parse_args()
    if not 1<=args.idle_seconds<=300 or not 1<=args.tunnel_seconds<=600 or not 1024<=args.tunnel_bytes<=64*1024*1024:parser.error('Choose bounded idle, lifetime and byte limits')
    if not 1<=args.max_routes<=256 or not 2<=args.max_connections<=1024:parser.error('Choose bounded quotas')
    for path in (args.certificate,args.private_key):
        info=path.lstat()
        if path.is_symlink() or info.st_uid!=os.getuid() or info.st_mode&0o077:parser.error('TLS files must be private and owned by the service user')
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2;context.load_cert_chain(args.certificate,args.private_key)
    server=RelayServer((args.bind,args.port),context,Broker(Limits(routes=args.max_routes,connections=args.max_connections,idle=args.idle_seconds,lifetime=args.tunnel_seconds,bytes=args.tunnel_bytes),enrollment=enrollment_value({'enrollment_file':str(args.enrollment_file)})))
    try:server.serve_forever()
    except KeyboardInterrupt:pass
    finally:server.server_close()

if __name__=='__main__':main()
