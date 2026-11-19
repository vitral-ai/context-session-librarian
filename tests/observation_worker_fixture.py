"""Owned test worker only: model token-work latency independently of observation."""
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csl_workload_auth import WorkloadTokens
from csl_workload_runner import worker_main
from csl_workload_auth import boot_clock
from csl_workload_observation_agent import RemoteObservation

original = WorkloadTokens.access_token
def token_work(self):
    observation = getattr(self.http, 'observation', None)
    if sys.argv[-1] == 'ready-fixture' and isinstance(observation, RemoteObservation) and observation.call:
        end = min(observation.deadline, boot_clock()+1)
        while not observation.failed and boot_clock() < end:
            try:
                observation.readiness()
                if observation.call in observation.prepared: break
            except Exception: break
            time.sleep(.001)
    else: time.sleep(.04)
    return original(self)
WorkloadTokens.access_token = token_work
args = sys.argv[1:-1]
worker_main(args[0], int(args[1]), float(args[2]), int(args[3]) if len(args) == 4 else None)
