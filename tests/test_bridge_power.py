"""Mock-only macOS assertion scope. No real Mac/sleep assertion is claimed."""
import subprocess
import unittest
from unittest.mock import Mock
from csl_bridge_power import Availability

class AvailabilityTests(unittest.TestCase):
 def test_opt_in_uses_only_idle_system_assertion_tied_to_resident_pid(self):
  process=Mock();process.poll.return_value=None;spawn=Mock(return_value=process)
  power=Availability(platform='darwin',spawn=spawn,pid=12345)
  self.assertFalse(power.status()['enabled']);spawn.assert_not_called()
  self.assertTrue(power.set(True)['enabled']);self.assertTrue(power.set(True)['enabled'])
  spawn.assert_called_once_with(['/usr/bin/caffeinate','-i','-s','-w','12345'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  self.assertNotIn('-d',spawn.call_args.args[0])
  self.assertFalse(power.set(False)['enabled']);process.terminate.assert_called_once();process.wait.assert_called_once_with(timeout=2)
  power.close();self.assertEqual(process.terminate.call_count,1)
 def test_linux_does_not_start_any_power_process_or_require_permission(self):
  spawn=Mock();power=Availability(platform='linux',spawn=spawn)
  self.assertFalse(power.set(True)['enabled']);self.assertFalse(power.status()['supported'])
  power.close();spawn.assert_not_called()
 def test_unavailable_command_is_visible_without_global_workaround(self):
  power=Availability(platform='darwin',spawn=Mock(side_effect=OSError()))
  state=power.set(True);self.assertFalse(state['enabled']);self.assertEqual(state['error'],'caffeinate_unavailable')
 def test_stop_terminates_only_owned_assertion_and_forces_child_if_it_stalls(self):
  process=Mock();process.poll.return_value=None
  process.wait.side_effect=[subprocess.TimeoutExpired('caffeinate',2),0]
  power=Availability(platform='darwin',spawn=Mock(return_value=process));power.set(True);power.close()
  process.terminate.assert_called_once();process.kill.assert_called_once()
  self.assertEqual(process.wait.call_count,2)
 def test_exited_assertion_is_not_claimed_active_and_no_other_process_is_signalled(self):
  process=Mock();process.poll.return_value=0
  power=Availability(platform='darwin',spawn=Mock(return_value=process));self.assertFalse(power.set(True)['enabled'])
  power.close();process.terminate.assert_not_called()

if __name__=='__main__':unittest.main()
