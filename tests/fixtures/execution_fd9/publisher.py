"""Synthetic actual private publisher, driven only by the Go FD9 test parent."""
import base64
import os
from pathlib import Path
import sys

sys.path.insert(0,sys.argv[1])
from csl_backup_core import canonical
import csl_execution as execution
from csl_execution_transport import Channel

req=execution.request(base64.b64decode(sys.argv[4],validate=True))
root=Path(sys.argv[2])
try:
    actual=[]
    for entry in os.listdir('/proc/self/fd'):
        fd=int(entry)
        if fd>=3:
            try:os.fstat(fd)
            except OSError:continue
            actual.append(fd)
    assert sorted(actual)==list(range(3,10)), actual
    # Measure inherited endpoints BEFORE loading extension-backed stdlib modules;
    # this host's ctypes loader retains its own libffi FD, not an inherited peer.
    import csl_import_execution as publisher
    with Channel(9,req,parent_pid=os.getppid(),parent_uid=os.getuid()) as channel:
        channel.admit()
        req=channel.read_request(3)
        captured=execution.capture(5,req,channel.check,1024*1024)
        try:
            roots={'journal':publisher.identity(6),'home':publisher.identity(7)}
            def cut(point):
                if sys.argv[3]=='death-publisher-effect' and point=='metadata_publishing_renamed':
                    (root/'ready').write_text(str(os.getpid()))
                    channel.check('effect')
                    (root/'effect').write_text('must not exist')
            result=publisher.publish(6,7,req,captured,channel.check,roots=roots,fault=cut)
            assert publisher.lookup(6,7,req,channel.check,roots=roots)['receipt']==result
            raw=canonical(result)
            channel.write_result(4,raw)
            (root/'report.json').write_bytes(raw)
        finally:captured.close()
finally:
    for fd in range(3,9):os.close(fd)
