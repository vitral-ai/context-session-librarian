"""Subsecond local IPC deadline tests; no SSH, runtime, auth or inference."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from csl_bridge import run
from csl_bridge_store import BridgeError
from csl_bridge_transport import local_call, exchange, CALL_TIMEOUT, EXCHANGE_TIMEOUT, RECEIVER_TIMEOUT, LOCAL_TIMEOUT, RETRY_CALL_TIMEOUT


class DeadlineTests(unittest.TestCase):
    def slow_receipt(self, client_timeout):
        with tempfile.TemporaryDirectory() as tmp:
            home=Path(tmp);home.chmod(0o700)
            server=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
            server.bind(str(home/'connector.sock'));server.listen(1)
            committed=[]
            def serve():
                conn,_=server.accept()
                with conn:
                    with conn.makefile('rb') as stream: request=json.loads(stream.readline())
                    time.sleep(.06)
                    committed.append(request['request']['id'])
                    try:conn.sendall(b'{"ok":true,"data":{"state":"delivered"}}\n')
                    except OSError:pass
            worker=threading.Thread(target=serve);worker.start()
            request={'local_op':'call','request':{'id':str(uuid.uuid4())}}
            try:
                with patch('csl_bridge_transport.CALL_TIMEOUT',.15):
                    if client_timeout is None:
                        result=local_call(home,request)
                        self.assertEqual(result['state'],'delivered')
                    else:
                        with self.assertRaisesRegex(BridgeError,'uncertain'):
                            local_call(home,request,timeout=client_timeout)
            finally:
                worker.join(1);server.close()
            self.assertEqual(committed,[request['request']['id']])

    def test_late_delivery_outlives_short_client_without_cancellation_or_replay(self):
        self.slow_receipt(.025)

    def test_nested_client_budget_returns_late_committed_receipt(self):
        self.slow_receipt(None)

    def test_layer_budgets_and_wait_remain_distinct(self):
        self.assertLess(RECEIVER_TIMEOUT,EXCHANGE_TIMEOUT)
        self.assertLess(EXCHANGE_TIMEOUT,CALL_TIMEOUT)
        with tempfile.TemporaryDirectory() as tmp, patch('csl_bridge_transport.socket.socket') as factory:
            Path(tmp).chmod(0o700)
            sock=factory.return_value
            sock.makefile.return_value.__enter__.return_value.readline.return_value=b'{"ok":true,"data":{}}\n'
            for op,expected in [('call',CALL_TIMEOUT),('_incoming',RECEIVER_TIMEOUT),('wait',LOCAL_TIMEOUT),('retry_operation',RETRY_CALL_TIMEOUT)]:
                local_call(tmp,{'local_op':op})
                sock.settimeout.assert_called_with(expected)

    def test_transport_excess_is_uncertain_and_never_retried(self):
        request={'version':1,'op':'status','id':str(uuid.uuid4()),'target':'worker'}
        with patch('csl_bridge_transport.subprocess.run',side_effect=subprocess.TimeoutExpired(['not-printed'],.1)) as call:
            with self.assertRaisesRegex(BridgeError,'uncertain'):
                exchange({'transport':'operator','helper':'/fixed/local/helper'},'origin',request)
        self.assertEqual(call.call_count,1)
        self.assertEqual(call.call_args.kwargs['timeout'],EXCHANGE_TIMEOUT)

    def test_cli_returned_uncertain_receipt_has_same_guidance(self):
        identifier=str(uuid.uuid4());errors=io.StringIO();output=io.StringIO()
        args=SimpleNamespace(action='followup',request_id=identifier,source='coordinator',target='worker',peer='mac',text_file='unused',scrub_secrets=False,home='unused')
        with patch('csl_bridge.reviewed_text',return_value='synthetic task'), patch('csl_bridge.local_call',return_value={'state':'uncertain','request_id':identifier}) as call, redirect_stderr(errors), redirect_stdout(output):
            run(args)
        self.assertEqual(call.call_count,1)
        self.assertEqual(json.loads(output.getvalue())['request_id'],identifier)
        self.assertIn('receipt --peer mac --operation-id '+identifier,errors.getvalue())

    def test_cli_lost_ipc_names_original_id_and_receipt_without_resend(self):
        identifier=str(uuid.uuid4());errors=io.StringIO()
        args=SimpleNamespace(action='send_message',request_id=identifier,source='coordinator',target='worker',peer='mac',text_file='unused',scrub_secrets=False,home='unused')
        with patch('csl_bridge.reviewed_text',return_value='synthetic message'), patch('csl_bridge.local_call',side_effect=BridgeError('outcome uncertain')) as call, redirect_stderr(errors):
            with self.assertRaises(SystemExit):run(args)
        self.assertEqual(call.call_count,1)
        self.assertIn('receipt --peer mac --operation-id '+identifier,errors.getvalue())
        self.assertIn('not cancelled',errors.getvalue())

if __name__=='__main__':unittest.main()
