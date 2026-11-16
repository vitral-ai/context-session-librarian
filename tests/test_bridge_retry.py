"""Explicit local same-ID retry with actual IPC + fabricated native runtime."""
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from csl_bridge_store import Store,BridgeError,encode
from csl_bridge_engine import Connector
from csl_bridge_transport import exchange
from test_bridge import FakeRuntime


class RetryTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.connectors=[]
  for identity,peer in (('alice','bob'),('bob','alice')):
   home=self.root/identity;store=Store(home,create=True);thread=str(uuid.uuid4())
   with store.transaction() as db:
    db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode(identity)),('runtime_socket',encode(None))])
    db.execute('INSERT INTO agents VALUES(?,?)',('worker',thread))
    db.execute('INSERT INTO peers VALUES(?,?)',(peer,encode({'transport':'receive-only','allow_agents':['worker'],'allow_sources':['worker'],'allow_ops':['followup','send_message','receipt','status']})))
   store.close();connector=Connector(home);connector.start();self.connectors.append(connector)
  self.owner,self.receiver=self.connectors;self.receiver.client_only=False;self.receiver.runtime=FakeRuntime(self.receiver.agents['worker'])
  self.owner.peers['bob']={'transport':'lab','home':str(self.receiver.store.home)}
  self.body={'version':1,'op':'followup','id':str(uuid.uuid4()),'source':'worker','target':'worker','text':'Original synthetic instruction.'}
  self.operation={'local_op':'call','peer':'bob','request':self.body}
 def tearDown(self):
  for connector in self.connectors:connector.close()
  self.temp.cleanup()
 def uncertain(self):
  with patch('csl_bridge_engine.exchange',side_effect=BridgeError('Synthetic disconnected relay')):self.owner.local(self.operation)
 def retry(self,**kwargs):
  digest=hashlib.sha256(encode(self.body).encode()).hexdigest()
  return self.owner.local({'local_op':'retry_operation','operation_id':self.body['id'],'expected_hash':digest,'confirmed':True,**kwargs})
 def starts(self):return len([x for x in self.receiver.runtime.calls if x[0]=='turn/start'])
 def test_authenticated_not_found_allows_exact_original_once_then_converges(self):
  self.uncertain();self.assertEqual(self.starts(),0)
  result=self.retry();self.assertEqual(result['state'],'delivered');self.assertTrue(result['resent'])
  self.assertEqual(result['request_id'],self.body['id']);self.assertEqual(self.starts(),1)
  repeated=self.retry();self.assertEqual(repeated['turn_id'],result['turn_id']);self.assertEqual(self.starts(),1)
 def test_original_inflight_arrives_after_not_found_receiver_deduplicates(self):
  self.uncertain();seen=[]
  def race(config,identity,body):
   result=exchange(config,identity,body);seen.append(body['op'])
   if body['op']=='receipt':
    self.assertEqual(result['state'],'not_found')
    self.receiver.incoming('alice',self.body)  # original delayed frame won the race
   return result
  with patch('csl_bridge_engine.exchange',side_effect=race):result=self.retry()
  self.assertEqual(seen,['receipt','followup']);self.assertEqual(result['state'],'delivered');self.assertEqual(self.starts(),1)
  self.assertEqual(self.owner.local(self.operation)['turn_id'],result['turn_id'])
 def test_existing_delivered_receipt_recovers_without_reinvocation(self):
  self.uncertain();original=self.receiver.incoming('alice',self.body)
  result=self.retry();self.assertFalse(result['resent']);self.assertTrue(result['recovered_from_receipt'])
  self.assertEqual(result['turn_id'],original['turn_id']);self.assertEqual(self.starts(),1)
 def test_uncertain_or_invoking_receiver_and_receipt_failure_never_replay(self):
  self.uncertain()
  for response in ({'state':'uncertain','result':{'state':'uncertain','request_id':self.body['id']}},{'state':'invoking','result':None}):
   with patch('csl_bridge_engine.exchange',return_value=response) as call:
    self.assertFalse(self.retry()['resent']);self.assertEqual(call.call_count,1)
  with patch('csl_bridge_engine.exchange',side_effect=BridgeError('Receipt unavailable')) as call:
   self.assertFalse(self.retry()['resent']);self.assertEqual(call.call_count,1)
  self.assertEqual(self.starts(),0)
 def test_hash_change_or_missing_confirmation_rejects_before_network(self):
  self.uncertain()
  with patch('csl_bridge_engine.exchange') as call:
   with self.assertRaises(BridgeError):self.retry(expected_hash='0'*64)
   with self.assertRaises(BridgeError):self.retry(confirmed=False)
  call.assert_not_called()
  with self.owner.store.transaction() as db:
   db.execute('UPDATE operations SET request=? WHERE id=?',(encode({**self.body,'text':'Changed instruction'}),self.body['id']))
  with patch('csl_bridge_engine.exchange') as call:
   with self.assertRaises(BridgeError):self.retry()
  call.assert_not_called();self.assertEqual(self.starts(),0)

if __name__=='__main__':unittest.main()
