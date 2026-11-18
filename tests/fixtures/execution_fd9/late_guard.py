"""Real fork/socket/effect probe; source path is explicit for old/new comparison."""
import hashlib
import json
import os
from pathlib import Path
import select
import selectors
import signal
import socket
import struct
import sys

sys.path.insert(0,sys.argv[1])
import csl_execution as e
from csl_backup_core import canonical
from csl_execution_transport import Channel

root=Path(sys.argv[2]); operation,loss,clock_result=sys.argv[3:]
boot,now=e.linux_clock()
req=e.request(canonical(dict(schema_version=1,operation='import',execution_id='re_'+'1'*64,
    import_key='ri_'+'2'*64,provider='codex',source_id='synthetic',package_size=7,
    package_sha256=hashlib.sha256(b'package').hexdigest(),owner='1'*24,host_id='ses_fixture',
    assignment_id='33333333-3333-4333-8333-333333333333',generation='1',boot_id=boot,
    cwd=str(root),cwd_device=str(root.stat().st_dev),cwd_inode=str(root.stat().st_ino),
    authority=dict(boot_id=boot,start_boottime_ns=str(now),end_boottime_ns=str(now+2_000_000_000)))))
(root/'package').write_bytes(b'package')
package=os.open(root/'package',os.O_RDONLY)
parent,child=socket.socketpair()
ready_r,ready_w=os.pipe();release_r,release_w=os.pipe();result_r,result_w=os.pipe()

def frame(v):
    raw=canonical(v)
    return struct.pack('!I',len(raw))+raw

def exact(sock,n):
    raw=b''
    while len(raw)<n:
        part=sock.recv(n-len(raw))
        if not part:raise EOFError()
        raw+=part
    return raw

pid=os.fork()
if pid==0:
    parent.close();os.close(ready_r);os.close(release_w);os.close(result_r)
    record={};creates=0
    try:
        signal.alarm(4)
        with Channel(child.detach(),req,parent_pid=os.getppid(),parent_uid=os.getuid()) as ch:
            ch.admit();paused=False
            if operation in ('receive','receive-partial'):os.write(ready_w,b'A')
            def clock():
                global paused
                names=[];frames={};f=sys._getframe(1)
                while f:
                    names.append(f.f_code.co_name);frames.setdefault(f.f_code.co_name,f);f=f.f_back
                # Final original-clock sample after exact OK, not a send/receive
                # or wait sample. Works on both original and corrected source.
                target=(ch.seq==(2 if operation=='capture' else 1) and '_check' in names and not any(n in names for n in ('_receive','_send','_wait')))
                if operation=='send':target='_send' in names and '_wait' not in names
                elif operation=='receive':target='_receive' in frames and 'value' in frames['_receive'].f_locals
                elif operation=='receive-partial':target='_receive' in frames and len(frames['_receive'].f_locals.get('raw',b''))==4 and frames['_receive'].f_locals.get('need',4)>4
                elif operation=='wait':target='_wait' in frames and 'ready' in frames['_wait'].f_locals
                if target and not paused:
                    paused=True;os.write(ready_w,b'R');os.read(release_r,1)
                    if clock_result=='expired':return boot,ch.end
                    if clock_result=='failed':raise e.ExecutionError('clock_unavailable')
                return e.linux_clock()
            ch.clock=clock
            original=os.memfd_create
            def counted(*args,**kwargs):
                global creates
                creates+=1
                return original(*args,**kwargs)
            os.memfd_create=counted
            try:
                if operation=='result':ch.write_result(result_w,b'positive-result')
                elif operation=='guard':
                    ch.check('capture');os.write(result_w,b'guarded-effect')
                elif operation=='send':ch.check('capture')
                elif operation in ('receive','receive-partial'):
                    ch._receive();os.write(result_w,b'accepted-frame')
                elif operation=='wait':
                    ch._wait(result_w,selectors.EVENT_WRITE);os.write(result_w,b'ready-effect')
                else:
                    captured=e.capture(package,req,ch.check,1024);captured.close()
                record={'error':'NO_REFUSAL','type':'none'}
            except BaseException as ex:
                record={'error':str(ex),'type':type(ex).__name__}
    finally:
        record['memfd_creates']=creates
        (root/'report.json').write_text(json.dumps(record))
        for fd in (ready_w,release_r,result_w,package):os.close(fd)
        os._exit(0)
child.close();os.close(ready_w);os.close(release_r);os.close(result_w);os.close(package)
parent.settimeout(2);reaped=False
try:
    parent.sendall(frame(dict(type='hello',request_sha256=req.commitment,authority=req.value['authority'])))
    if operation in ('receive','receive-partial'):
        assert select.select([ready_r],[],[],2)[0] and os.read(ready_r,1)==b'A'
        if operation=='receive-partial':parent.sendall(frame(dict(type='deny',reason='assignment_revoked'))[:4])
        else:parent.sendall(frame(dict(type='ok',seq='1',authority=req.value['authority'])))
    elif operation not in ('send','wait'):
        for unused in range(2 if operation=='capture' else 1):
            message=json.loads(exact(parent,struct.unpack('!I',exact(parent,4))[0]))
            parent.sendall(frame(dict(type='ok',seq=message['seq'],authority=req.value['authority'])))
    assert select.select([ready_r],[],[],2)[0] and os.read(ready_r,1)==b'R'
    if operation=='receive-partial':parent.sendall(frame(dict(type='deny',reason='assignment_revoked'))[4:])
    elif loss=='deny':parent.sendall(frame(dict(type='deny',reason='assignment_revoked')))
    elif loss=='partial':parent.sendall(b'\0')
    else:parent.shutdown(socket.SHUT_WR)
    os.write(release_w,b'G')
    waited,status=os.waitpid(pid,0);reaped=True
    assert waited==pid and os.waitstatus_to_exitcode(status)==0
    data=os.read(result_r,1024)
    control_reset=False
    try:control=parent.recv(2048)
    except ConnectionResetError:
        control=b'';control_reset=True
    record=json.loads((root/'report.json').read_text())
    print(json.dumps(dict(operation=operation,loss=loss,emitted_hex=data.hex(),control_hex=control.hex(),control_reset=control_reset,child=record,reaped=True)))
finally:
    parent.close()
    for fd in (ready_r,release_w,result_r):os.close(fd)
    if not reaped:
        try:os.kill(pid,signal.SIGKILL)
        except ProcessLookupError:pass
        os.waitpid(pid,0)
