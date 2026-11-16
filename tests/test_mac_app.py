"""Local backend scope, existing-session and configuration preservation tests."""
import importlib.util
import json
from pathlib import Path
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('csl_app_service',ROOT/'macos/CSLBridge/app_service.py')
app=importlib.util.module_from_spec(spec);sys.modules[spec.name]=app;spec.loader.exec_module(app)
class AppTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
  self.service=app.Service(self.root/'state',self.root/'bridge',self.root/'codex')
 def tearDown(self):self.temp.cleanup()
 def test_profile_uses_current_bundled_interpreter_and_only_communication_grant(self):
  import tomllib
  p=tomllib.loads(self.service.mcp_profile())['mcp_servers']['csl_bridge']
  self.assertEqual(p['command'],sys.executable)
  from csl_bridge_mcp import MCP_TOOL_TIMEOUT, MCP_HOST_MARGIN
  from csl_bridge_transport import CALL_TIMEOUT
  self.assertEqual(p['tool_timeout_sec'],MCP_TOOL_TIMEOUT)
  self.assertEqual(p['tool_timeout_sec']-CALL_TIMEOUT,MCP_HOST_MARGIN)
  self.assertEqual(p['startup_timeout_sec'],10)
  self.assertEqual(set(p['enabled_tools']),{'send_progress','wait_agent','list_agents','agent_status','operation_receipt'})
  self.assertEqual(p['tools'],{'send_progress':{'approval_mode':'approve'}})
  self.assertEqual(p['default_tools_approval_mode'],'writes')
  self.assertNotIn('followup_task',p['enabled_tools'])
 def test_optional_delegation_is_explicit_and_does_not_include_interrupt(self):
  import tomllib
  profile=tomllib.loads(self.service.mcp_profile(True))['mcp_servers']['csl_bridge']
  self.assertEqual(set(profile['tools']),{'send_progress','followup_task','send_message'})
  self.assertNotIn('interrupt_agent',profile['enabled_tools'])
  self.assertTrue(all(v=={'approval_mode':'approve'} for v in profile['tools'].values()))
 def test_latest_task_acceptance_is_not_previous_final_or_new_activity(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True)
  old=str(uuid.uuid4());current=str(uuid.uuid4());turn=str(uuid.uuid4())
  with store.transaction() as db:
   db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode('workerbox')),('runtime_socket',encode(None))])
   db.execute('INSERT INTO agents VALUES(?,?)',('worker',str(uuid.uuid4())))
  for task,status in ((old,'completed'),(current,'running')):
   frame={'version':1,'op':'followup','id':task,'source':'owner','target':'worker','text':'Synthetic'}
   store.reserve('owner',frame);store.operation(task,'delivered',{'state':'delivered','turn_id':turn})
   with store.transaction() as db:db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?)',(task,'owner','owner','worker',turn,status))
  def event(task,kind):
   store.publish('owner',{'task_id':task,'source':'worker','target':'owner','kind':kind,'text':'Synthetic','turn_id':turn,'status':'completed' if kind=='final' else 'progress'},str(uuid.uuid4()))
  event(old,'final')
  self.assertEqual(self.service.task_phase(),'accepted_only')
  event(current,'progress');self.assertEqual(self.service.task_phase(),'activity_observed')
  event(current,'final');self.assertEqual(self.service.task_phase(),'final_observed')
  store.close()
 def test_new_outgoing_task_beats_old_incoming_final(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True);old=str(uuid.uuid4());new=str(uuid.uuid4());turn=str(uuid.uuid4())
  with store.transaction() as db:
   db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode('owner')),('runtime_socket',encode(None))])
   db.execute('INSERT INTO tasks VALUES(?,?,?,?,?,?)',(old,'remote','remote','worker',turn,'completed'))
  for task,peer in ((old,'remote'),(new,'outbound:remote')):
   frame={'version':1,'op':'followup','id':task,'source':'worker','target':'worker','text':'Synthetic'}
   store.reserve(peer,frame);store.operation(task,'delivered',{'state':'delivered','turn_id':turn})
  def event(task,kind):store.publish('remote',{'task_id':task,'source':'worker','target':'worker','kind':kind,'text':'Synthetic','turn_id':turn,'status':'completed' if kind=='final' else 'progress'},str(uuid.uuid4()))
  event(old,'final');self.assertEqual(self.service.task_phase(),'accepted_only')
  event(new,'progress');self.assertEqual(self.service.task_phase(),'activity_observed')
  event(new,'final');self.assertEqual(self.service.task_phase(),'final_observed');store.close()
 def test_failed_or_interrupted_final_is_not_success(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True)
  for status,expected in (('failed','final_failed'),('interrupted','final_interrupted')):
   task=str(uuid.uuid4());frame={'version':1,'op':'followup','id':task,'source':'worker','target':'worker','text':'Synthetic'}
   store.reserve('remote',frame);store.operation(task,'delivered',{'state':'delivered'})
   store.publish('remote',{'task_id':task,'source':'worker','target':'worker','kind':'final','text':'Synthetic','turn_id':str(uuid.uuid4()),'status':status},str(uuid.uuid4()))
   self.assertEqual(self.service.task_phase(),expected)
  store.close()
 def test_channel_stop_clears_old_positive_proof_without_stopping_native(self):
  app.json_file(self.service.state/'peer-proof.json',{'ready':True,'observed_at':0})
  channel=self.service.state/'lan/session-control.sock';channel.parent.mkdir();channel.touch()
  with patch('csl_bridge_lan.control',return_value={'stopping':True}) as control,patch('csl_app_service.os.kill') as kill:
   result=self.service.lan_stop()
  self.assertTrue(result['stopping']);kill.assert_not_called()
  self.assertFalse(json.loads((self.service.state/'peer-proof.json').read_text())['ready'])
  control.assert_called_once_with(self.service.state/'lan','stop')
 def test_uncertain_pair_does_not_claim_ready_or_replace_selected_peer(self):
  self.service.settings={'coordinator':'existing-owner'}
  with patch.object(self.service,'start',return_value={'started_by_this_call':True}),patch('csl_bridge_lan.invitation_input',return_value=('A'*26,None)),patch('csl_bridge_lan.connect_code',return_value={'state':'uncertain','lan_session_owned':True}),patch.object(self.service,'peer_check') as verify:
   result=self.service.lan_connect('synthetic')
  self.assertEqual(result['state'],'uncertain');self.assertTrue(result['connector_owned']);verify.assert_not_called()
  self.assertEqual(self.service.settings['coordinator'],'existing-owner')
  self.assertFalse(json.loads((self.service.state/'peer-proof.json').read_text())['ready'])
 def test_auto_selects_provisioned_relay_without_reading_or_copying_enrollment(self):
  self.assertEqual(self.service.connection_mode(),'lan')
  self.service.state.mkdir(mode=0o700)
  profile=self.service.state/'relay-profile.json';profile.write_text('{"operator":"private"}');profile.chmod(0o600)
  self.assertEqual(self.service.connection_mode(),'relay');self.assertEqual(self.service.connection_mode('lan'),'lan')
  self.assertEqual(self.service.relay_profile_path(),profile)
 def test_relay_rejoin_invalidates_old_application_ready_until_new_response(self):
  import time
  self.service.settings['connection_mode']='relay';now=time.time()
  app.json_file(self.service.state/'peer-proof.json',{'ready':True,'observed_at':now-3})
  app.json_file(self.service.state/'relay/relay-status.json',{'phase':'relay_connected_peer_unverified','connected_at':now-2})
  with patch.object(app,'local_call',return_value={'agents':[{'status':'idle'}]}),patch.object(self.service,'owned',return_value=None):
   self.assertFalse(self.service.status()['connected'])
   app.json_file(self.service.state/'peer-proof.json',{'ready':True,'observed_at':now-1})
   self.assertTrue(self.service.status()['connected'])
   app.json_file(self.service.state/'relay/relay-status.json',{'phase':'reconnecting','connected_at':now-2})
   self.assertFalse(self.service.status()['connected'])
 def test_empty_stop_is_idempotent_and_creates_no_state_for_either_mode(self):
  for mode in ('lan','relay'):
   self.service.settings={'connection_mode':mode}
   with patch('csl_bridge_lan.control') as local,patch('csl_bridge_relay_client.session_control') as relayed:
    self.assertTrue(self.service.lan_stop()['already_stopped'])
    self.assertTrue(self.service.lan_stop()['already_stopped'])
   local.assert_not_called();relayed.assert_not_called()
   self.assertFalse(self.service.state.exists())
 def test_resident_pair_is_selected_before_historical_operator_peer(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True)
  with store.transaction() as db:
   db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode('owner')),('runtime_socket',encode(None))])
   db.execute('INSERT INTO agents VALUES(?,?)',('coordinator',str(uuid.uuid4())))
   db.execute('INSERT INTO peers VALUES(?,?)',('ubuntu-old',encode({'transport':'operator','helper':'/old-profile'})))
   db.execute('INSERT INTO peers VALUES(?,?)',('mac',encode({'transport':'relay','marker':'CURRENT'})))
  store.close()
  for mode,control in (('relay','csl_bridge_relay_client.session_control'),('lan','csl_bridge_lan.control')):
   self.service.settings={'connection_mode':mode,'coordinator':'ubuntu-old'}
   with patch(control,return_value={'peers':[{'identity':'mac','agent':'agent-mac'}]}) as resident,patch.object(app,'exchange',return_value={'agent':'agent-mac','thread_id':str(uuid.uuid4()),'status':'idle'}) as exchange:
    proof=self.service.peer_check()
   self.assertTrue(proof['ready']);self.assertEqual(proof['peer'],'mac')
   self.assertEqual(exchange.call_args.args[0]['marker'],'CURRENT')
   self.assertEqual(exchange.call_args.args[2]['target'],'agent-mac')
  self.service.settings={'connection_mode':'relay'}
  with patch('csl_bridge_relay_client.session_control',return_value={'peers':[{'identity':'mac','agent':'agent-mac'},{'identity':'another','agent':'worker'}]}),patch.object(app,'exchange') as exchange:
   with self.assertRaisesRegex(app.AppError,'Multiple session peers'):self.service.peer_check()
  exchange.assert_not_called()
 def test_cli_channel_status_verification_includes_application_proof_and_task(self):
  self.service.settings={'connection_mode':'relay'}
  channel={'running':True,'phase':'relay_connected_peer_unverified','peer_ready':False}
  verified={'connected':True,'runtime_ready':True,'peer':{'ready':True,'peer':'mac'},'task_phase':'accepted_only','note':'Accepted is not continuation.'}
  with patch('csl_bridge_relay_client.session_control',return_value=channel),patch.object(self.service,'status',return_value=verified) as status:
   result=self.service.lan_status(True)
  status.assert_called_once_with(True)
  self.assertTrue(result['peer_ready']);self.assertTrue(result['peer']['ready'])
  self.assertFalse(result['channel']['peer_ready']);self.assertEqual(result['task_phase'],'accepted_only')
 def test_retry_original_requires_explicit_confirmation_and_uses_durable_hash(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True);task=str(uuid.uuid4())
  body={'version':1,'op':'followup','id':task,'source':'worker','target':'worker','text':'Selected original synthetic instruction.'}
  store.reserve('outbound:mac',body);store.operation(task,'uncertain',{'state':'uncertain','request_id':task})
  digest=store.rows('SELECT hash FROM operations WHERE id=?',(task,))[0]['hash'];store.close()
  with patch.object(app,'local_call',return_value={'state':'delivered'}) as call:
   with self.assertRaisesRegex(app.AppError,'confirm-same-request'):self.service.retry_original(task)
   call.assert_not_called();self.service.retry_original(task,True)
  self.assertEqual(call.call_args.args[1],{'local_op':'retry_operation','operation_id':task,'expected_hash':digest,'confirmed':True})
 def test_only_owned_mcp_sections_replaced_other_text_preserved(self):
  old='model="owner-model"\n\n[mcp_servers.other]\ncommand="owner"\n\n[mcp_servers.csl_bridge]\ncommand="old"\n[mcp_servers.csl_bridge.tools.send_progress]\napproval_mode="prompt"\n'
  new,previous=app.Service.replace_owned(old,self.service.mcp_profile())
  self.assertTrue(new.startswith('model="owner-model"\n\n[mcp_servers.other]\ncommand="owner"\n\n'))
  self.assertIn('command="old"',previous)
  self.assertNotIn('owner-model',previous)
 def test_inline_personal_configuration_is_not_rewritten(self):
  with self.assertRaises(app.AppError):app.Service.replace_owned('mcp_servers={csl_bridge={command="owned"}}\n',self.service.mcp_profile())
 def test_live_connector_adopted_no_new_process_or_config_change(self):
  with patch.object(self.service,'status',return_value={'connected':True,'adopted':True}),patch('csl_app_service.subprocess.Popen') as spawn:
   result=self.service.start()
  spawn.assert_not_called();self.assertTrue(result['adopted'])
 def test_disconnected_existing_connector_does_not_start_second_writer(self):
  with patch.object(self.service,'status',return_value={'connected':False,'connector_running':True}),patch('csl_app_service.subprocess.Popen') as spawn:
   with self.assertRaises(app.AppError):self.service.start()
  spawn.assert_not_called()
 def test_unloaded_thread_never_registered(self):
  with patch.object(self.service,'discover',return_value={'sessions':[]}):
   with self.assertRaises(app.AppError):self.service.configure(str(uuid.uuid4()))
  self.assertFalse((self.service.home/'bridge.sqlite').exists())
 def test_stop_refuses_process_not_owned_by_app(self):
  with patch.object(self.service,'owned',return_value=None),patch('csl_app_service.os.kill') as kill:
   with self.assertRaises(app.AppError):self.service.stop()
  kill.assert_not_called()
 def test_live_local_runtime_is_not_remote_readiness(self):
  with patch.object(app,'local_call',return_value={'agents':[{'status':'idle'}]}),patch.object(self.service,'owned',return_value=None):
   result=self.service.status()
  self.assertTrue(result['runtime_ready']);self.assertTrue(result['connector_running'])
  self.assertFalse(result['connected']);self.assertFalse(result['peer']['ready'])
 def test_authenticated_peer_response_required_and_expires(self):
  import time
  app.json_file(self.service.state/'peer-proof.json',{'ready':True,'observed_at':time.time()-60})
  with patch.object(app,'local_call',return_value={'agents':[{'status':'idle'}]}),patch.object(self.service,'owned',return_value=None):
   self.assertFalse(self.service.status()['connected'])
  app.json_file(self.service.state/'peer-proof.json',{'ready':True,'observed_at':time.time(),'phase':'authenticated_application_response'})
  with patch.object(app,'local_call',return_value={'agents':[{'status':'idle'}]}),patch.object(self.service,'owned',return_value=None):
   self.assertTrue(self.service.status()['connected'])
 def test_peer_check_rejects_pid_or_tunnel_only_response(self):
  from csl_bridge_store import Store,encode
  store=Store(self.service.home,create=True)
  with store.transaction() as db:
   db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode('mac')),('runtime_socket',encode('/unused'))])
   db.execute('INSERT INTO agents VALUES(?,?)',('worker',str(uuid.uuid4())))
   db.execute('INSERT INTO peers VALUES(?,?)',('owner',encode({'transport':'operator','helper':'/unused'})))
  store.close()
  with patch.object(app,'exchange',return_value={'pid':42,'status':'running'}):
   with self.assertRaises(app.AppError):self.service.peer_check()
  self.assertFalse((self.service.state/'peer-proof.json').exists())

 def test_connection_descriptor_shell_quotes_paths(self):
  with patch.object(self.service,'registered',return_value={'worker':str(uuid.uuid4())}):
   result=self.service.connection_info()
  argv=shlex.split(result['receiver_command']);self.assertEqual(argv[0],'env');self.assertIn(sys.executable,argv)
if __name__=='__main__':unittest.main()
