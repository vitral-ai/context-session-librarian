"""Run one writer process group and leave the kubelet's sidecar drain window."""
import argparse
import os
import signal
import subprocess
import sys
import time


def run(command, shutdown_seconds):
    deadline = None

    def clock():
        return time.clock_gettime(time.CLOCK_BOOTTIME)

    def stopping(*_):
        nonlocal deadline
        if deadline is None:
            deadline = clock() + shutdown_seconds

    def signal_group(pid, signum):
        try:
            os.killpg(pid, signum)
            return True
        except ProcessLookupError:
            return False

    signal.signal(signal.SIGTERM, stopping)
    signal.signal(signal.SIGINT, stopping)
    if deadline is not None:
        return 143
    with subprocess.Popen(command, start_new_session=True) as writer:
        forwarded = False
        while True:
            result = writer.poll()
            if result is not None and deadline is None:
                if not signal_group(writer.pid, 0):
                    break
                stopping()
            if deadline is not None:
                if not forwarded:
                    forwarded = True
                    present = signal_group(writer.pid, signal.SIGTERM)
                    if not present and result is not None:
                        break
                if not signal_group(writer.pid, 0):
                    break
                if clock() >= deadline:
                    signal_group(writer.pid, signal.SIGKILL)
                    # The container runtime tears down the writer PID namespace
                    # when PID 1 exits. Do not block past the writer's budget
                    # waiting for a stuck process or orphan.
                    os._exit(137 if result is None else (128 - result if result < 0 else result))
            time.sleep(0.02)
        result = writer.wait()
    return 128 - result if result < 0 else result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('shutdown_seconds', type=int)
    arguments = sys.argv[1:]
    separator = arguments.index('--') if '--' in arguments else len(arguments)
    args = parser.parse_args(arguments[:separator])
    command = arguments[separator + 1:]
    if not 0 <= args.shutdown_seconds <= 120:
        parser.error('shutdown_seconds must be between 0 and 120')
    if not command:
        parser.error('an explicit writer command must follow --')
    raise SystemExit(run(command, args.shutdown_seconds))


if __name__ == '__main__':
    main()
