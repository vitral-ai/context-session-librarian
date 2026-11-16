"""User-facing alias help and empty stop; isolated HOME, no runtime or network."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
ROOT=Path(__file__).resolve().parents[1]

class PairHelpTests(unittest.TestCase):
 def invoke(self,args,home):
  env={'HOME':str(home),'CODEX_HOME':str(home/'codex'),'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1'}
  return subprocess.run([sys.executable,'-B',str(ROOT/'csl-pair'),*args],capture_output=True,text=True,env=env,timeout=5)
 def test_step_help_exposes_only_share_or_connect_options(self):
  with tempfile.TemporaryDirectory() as tmp:
   home=Path(tmp)
   for step in ('share','connect'):
    result=self.invoke([step,'--help'],home)
    self.assertEqual(result.returncode,0);self.assertIn('usage: csl-pair '+step,result.stdout)
    for text in ('--mode','--approve-communication','--approve-delegation','tokens/cost','Example:'):
     self.assertIn(text,result.stdout)
    for text in ('lan-share','mcp-install','transport-run','selftest'):
     self.assertNotIn(text,result.stdout)
    self.assertEqual('--pair-code-stdin' in result.stdout,step=='connect')
   self.assertEqual(list(home.iterdir()),[])
 def test_cli_empty_stop_twice_creates_no_state(self):
  with tempfile.TemporaryDirectory() as tmp:
   home=Path(tmp)
   for _ in range(2):
    result=self.invoke(['stop'],home);self.assertEqual(result.returncode,0,result.stderr)
    self.assertTrue(json.loads(result.stdout)['data']['already_stopped'])
   self.assertEqual(list(home.iterdir()),[])
if __name__=='__main__':unittest.main()
