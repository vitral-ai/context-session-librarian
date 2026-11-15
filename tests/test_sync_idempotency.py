"""Strict registry identities under metadata changes, lost replies and restarts."""
import unittest
import uuid
from unittest.mock import patch

from csl_sync_engine import digest
from csl_sync_http import Registry
from csl_sync_state import SyncError
import test_sync as fixtures


class IdempotencyTests(unittest.TestCase):
    setUp=fixtures.SyncTests.setUp
    tearDown=fixtures.SyncTests.tearDown
    run_pass=fixtures.SyncTests.run_pass
    data=fixtures.SyncTests.data
    def test_legacy_inventory_only_id_reproduces_registry_metadata_conflict(self):
        self.run_pass()
        body=dict(self.peer.sessions['sessions/codex/fake-session'])
        inventory={k:body[k] for k in ('files','view','package')}
        body['snapshot_id']=str(uuid.uuid5(uuid.NAMESPACE_URL,
            self.state.get('device_id')+'/codex/fake-session/'+digest(inventory)))
        self.registry.call('PUT','/sessions/codex/fake-session',body=body,key=digest(body))
        body['state']='idle'
        with self.assertRaises(SyncError) as conflict:
            self.registry.call('PUT','/sessions/codex/fake-session',body=body,key=digest(body))
        self.assertEqual(conflict.exception.code,'idempotency_conflict')

    def test_metadata_only_change_gets_new_snapshot_without_new_raw_revision(self):
        self.run_pass()
        prior=dict(self.peer.sessions['sessions/codex/fake-session']);writes=len(self.peer.writes)
        listing=self.provider.list_sessions
        def idle(provider):
            rows=listing(provider)
            for row in rows:row['state']='idle'
            return rows
        with patch.object(self.provider,'list_sessions',side_effect=idle):
            self.assertEqual(self.run_pass()['synced'],1)
            current=self.peer.sessions['sessions/codex/fake-session']
            self.assertNotEqual(current['snapshot_id'],prior['snapshot_id'])
            self.assertEqual(current['files'],prior['files']);self.assertEqual(current['view'],prior['view'])
            self.assertEqual(current['state'],'idle');self.assertEqual(len(self.peer.writes),writes)
            self.run_pass();self.assertEqual(self.peer.sessions['sessions/codex/fake-session']['snapshot_id'],current['snapshot_id'])

    def test_lost_metadata_response_reuses_identical_complete_request(self):
        self.peer.drop_metadata_once=True
        self.assertEqual(self.run_pass()['synced'],1)
        self.assertEqual(len(self.peer.snapshots),1)
        self.assertEqual(self.run_pass()['synced'],1)
        self.assertEqual(len(self.peer.snapshots),1)

    def test_lost_chunk_response_then_append_never_reuses_key_for_different_bytes(self):
        self.source.write_bytes(b'aaaa\n')
        self.registry.http.attempts=1;self.peer.drop_once=True
        self.assertEqual(self.run_pass()['pending'],1)
        self.source.write_bytes(b'aaaa\nbbbb\n')
        self.assertEqual(self.run_pass()['synced'],1)
        self.assertEqual(self.data(),self.source.read_bytes())
        by_key={}
        for key,operation in self.peer.upload_requests:
            if key in by_key:self.assertEqual(by_key[key],operation)
            by_key[key]=operation

    def test_lost_chunk_reply_then_rewrite_changes_revision_before_reupload(self):
        self.source.write_bytes(b'aaaa\n')
        self.registry.http.attempts=1;self.peer.drop_once=True
        self.assertEqual(self.run_pass()['pending'],1)
        self.source.write_bytes(b'xxxx\n')
        self.assertEqual(self.run_pass()['pending'],1)
        self.assertEqual(self.state.session('codex','fake-session')['last_error'],'remote_prefix_mismatch')
        self.assertEqual(self.run_pass()['synced'],1)
        self.assertEqual(self.data(),b'xxxx\n')
        self.assertTrue(all(len({op for k,op in self.peer.upload_requests if k==key})==1 for key,_ in self.peer.upload_requests))

    def test_json_wire_order_is_canonical_for_content_addressed_requests(self):
        class Capture:
            def __init__(self):self.bodies=[]
            def request(self,url,method,**kwargs):self.bodies.append(kwargs['data']);return {}
        capture=Capture();client=Registry(self.peer.url,self.tokens,http=capture)
        first={'package':{'manifest':{'mode':'raw','version':3}},'state':'idle'}
        second={'state':'idle','package':{'manifest':{'version':3,'mode':'raw'}}}
        self.assertEqual(digest(first),digest(second))
        client.call('PUT','/sessions/codex/fake-session',body=first,key=digest(first))
        client.call('PUT','/sessions/codex/fake-session',body=second,key=digest(second))
        self.assertEqual(capture.bodies[0],capture.bodies[1])
