"""Optional session-scoped macOS idle-sleep assertion; no global power settings."""
import os
import subprocess
import sys
import threading


class Availability:
    def __init__(self,platform=None,spawn=None,pid=None):
        self.platform=sys.platform if platform is None else platform
        self.spawn=subprocess.Popen if spawn is None else spawn
        self.pid=os.getpid() if pid is None else pid
        self.process=None;self.lock=threading.RLock();self.error=None

    def status(self):
        with self.lock:
            active=self.process is not None and self.process.poll() is None
            return {'supported':self.platform=='darwin','enabled':active,
                    'scope':'this session client; idle system sleep only; display sleep unchanged',
                    'energy_cost':'may increase energy use while connected','error':self.error}

    def set(self,enabled):
        if type(enabled) is not bool:raise ValueError('Choose a boolean local availability option')
        with self.lock:
            if not enabled:self.close();return self.status()
            if self.platform!='darwin':return self.status()
            if self.process is not None and self.process.poll() is None:return self.status()
            try:
                # -w binds assertion lifetime to THIS resident client PID even on SIGKILL.
                # -i prevents idle sleep; -s is effective on AC. No -d/display assertion.
                self.process=self.spawn(['/usr/bin/caffeinate','-i','-s','-w',str(self.pid)],
                    stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                self.error=None
            except OSError:self.process=None;self.error='caffeinate_unavailable'
            return self.status()

    def close(self):
        with self.lock:
            process=self.process;self.process=None
            if process is None or process.poll() is not None:return
            process.terminate()
            try:process.wait(timeout=2)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=2)
