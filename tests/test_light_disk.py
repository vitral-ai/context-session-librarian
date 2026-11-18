"""Disk-backed duplex replay; real framing/SQLite, no SSH or native model calls."""
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
import uuid
import csl_light as light
from csl_bridge_store import Store


class DiskReplayTests(unittest.TestCase):
    def test_150_large_frames_each_way_with_reconnect_on_disk(self):
        parent = os.environ.get('CSL_DISK_TEST_DIR', '/var/tmp')
        if not Path(parent).is_dir():
            parent = tempfile.gettempdir()
        with tempfile.TemporaryDirectory(prefix='csl-light-disk-',dir=parent) as directory:
            root=Path(directory).resolve();nodes=[];stopped=threading.Event()
            active=[];lock=threading.Lock();connections=[];forced=threading.Event()
            def make(name,peer,agent):
                home=root/name/'bridge';store=Store(home,create=True)
                with store.transaction()as db:
                    db.execute('INSERT INTO agents VALUES(?,?)',(agent,str(uuid.uuid4())))
                    db.execute('INSERT INTO config VALUES(?,?)',('runtime_socket','null'))
                store.close()
                node=light.Node({'node':name,'home':str(root/name/'light'),
                    'default_agent':agent,'bridges':{name:{'home':str(home),'agents':[agent]}},
                    'links':{peer:{'agents':[]}}})
                node.schedule=lambda *args:None  # Durable ACK only, like unresolved forwards.
                nodes.append(node);return node
            a=make('env-a','hub','worker');b=make('hub','env-a','coordinator')
            for node,peer,source,target in ((a,'hub','worker','coordinator'),(b,'env-a','coordinator','worker')):
                for _ in range(150):
                    request={'version':1,'op':'mail','message_version':1,'action':'send',
                        'id':str(uuid.uuid4()),'source':source,'target':target,
                        'envelope':{'kind':'message','body':'😀'*12000,'in_reply_to':None,
                         'grant_ref':None,'created_at':'2026-11-17T00:00:00Z','expires_at':None}}
                    node.journal.put('out',peer,request['id'],{'type':'request','id':request['id'],
                        'request':request,'origin':'synthetic','destination':peer})
            def received(node):
                with node.journal.db()as db:
                    return db.execute("SELECT count(*) FROM rpc WHERE direction='in'").fetchone()[0]
            def connect_loop():
                while not stopped.is_set():
                    left,right=socket.socketpair();items=[]
                    for node,peer,sock in ((a,'hub',left),(b,'env-a',right)):
                        reader=sock.makefile('rb');writer=sock.makefile('wb',buffering=0)
                        channel=light.Channel(node,peer,reader,writer,
                            lambda sock=sock:sock.shutdown(socket.SHUT_RDWR))
                        thread=threading.Thread(target=channel.run,daemon=True)
                        items.append((channel,thread,sock,reader,writer))
                    connections.append(time.monotonic())
                    with lock:active[:]=items
                    for _,thread,*_ in items:thread.start()
                    while not stopped.wait(.02) and not any(c.closed for c,*_ in items):
                        if not forced.is_set() and min(received(a),received(b))>=20:
                            forced.set();break
                    for channel,*_ in items:channel.close()
                    for _,thread,sock,reader,writer in items:
                        thread.join(2);reader.close();writer.close();sock.close()
                    stopped.wait(.05)
            connector=threading.Thread(target=connect_loop,daemon=True);connector.start()
            try:
                until=time.monotonic()+35
                while time.monotonic()<until:
                    if forced.is_set() and received(a)==received(b)==150:
                        break
                    time.sleep(.05)
                self.assertEqual((received(a),received(b)),(150,150))
                self.assertTrue(forced.is_set())
                self.assertGreaterEqual(len(connections),2)
                self.assertLessEqual(len(connections),3)
            finally:
                stopped.set()
                with lock:
                    for channel,*_ in active:channel.close()
                connector.join(5)
                for node in nodes:node.pool.shutdown(wait=True)


if __name__=='__main__':unittest.main()
