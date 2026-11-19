"""Owned synthetic latency only; actual provider/binding/worker code is unchanged."""
from pathlib import Path
import select
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_workload_auth import WorkloadTokens, boot_clock
from csl_workload_launch_binding import LaunchObservation
from csl_workload_runner import worker_main

original_admitted = LaunchObservation.admitted
original_token = WorkloadTokens.access_token
def admitted(self, *args):
    original_admitted(self, *args)
    if sys.argv[-1] == 'ready-fixture' and not self.failed:
        # Fixture readiness barrier only. Never installed in a product worker.
        until = min(self.deadline, boot_clock()+2)
        while boot_clock() < until and not select.select([self.sock], [], [], 0)[0]: time.sleep(.001)

def token(self):
    observation = getattr(self.http, 'observation', None)
    remote = observation.remote if isinstance(observation, LaunchObservation) else None
    if sys.argv[-1] == 'ready-fixture' and remote is not None and remote.call:
        until = min(remote.deadline, boot_clock()+1)
        while boot_clock() < until:
            remote.readiness()
            if remote.call in remote.prepared: break
            time.sleep(.001)
    return original_token(self)

LaunchObservation.admitted = admitted
WorkloadTokens.access_token = token
args = sys.argv[1:-1]
worker_main(args[0], int(args[1]), float(args[2]), int(args[3]) if len(args) == 4 else None)
