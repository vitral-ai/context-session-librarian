"""Opt-in workload watch and literal-argv job supervisor; no image activation."""
from contextlib import contextmanager
import ctypes
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
import uuid
import urllib.request
import urllib.parse

from csl_gemini_workload import WorkloadGeminiProvider
from csl_sync_engine import Collector, digest
from csl_sync_http import Registry, NoRedirect
from csl_sync_state import State, SyncError, exclusive, private_json
from csl_text_workload import WorkloadTextProvider
from csl_workload_auth import WorkloadTokens, boot_clock
from csl_workload_config import Configuration, PREFIX
from csl_workload_http import WorkloadHTTP
from csl_workload_identity import WorkloadKey, canonical, metadata
from csl_workload_stream import Bounds, opened_source, verify_prefix
from csl_workload_state import child, read_record
from csl_workload_producer import Producer, recover as recover_producer

POLL_SECONDS = 0.1
PASS_SECONDS = 60
WRITER_SECONDS = 5
REAP_SECONDS = 0.5


def clean_receipts(runtime):
    directory = child(runtime, 'receipts')
    entries = []
    with os.scandir(directory) as scanned:
        for entry in scanned:
            if len(entries) == 128: raise SyncError('workload_runtime_storage_refused')
            entries.append(Path(entry.path))
    for path in entries:
        try: named = path.name == str(uuid.UUID(path.stem))+'.json'
        except ValueError: named = path.name.startswith('.write-')
        info = path.lstat()
        if (not named or not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 65536):
            raise SyncError('workload_runtime_storage_refused')
        path.unlink()


def scope(config):
    value = {'version': 1, 'origin': config.origin, 'provider': config.provider,
             'profile': config.profile, 'transcript_root': str(config.transcript_root), 'location': config.location}
    return hashlib.sha256(canonical(value)).hexdigest()


def environment(config):
    values = {'CLIENT_ID': 'vitral-workload-collector', 'ORIGIN': config.origin, 'PROVIDER': config.provider,
        'PROFILE': config.profile, 'TRANSCRIPT_ROOT': str(config.transcript_root), 'STATE': str(config.state_root),
        'REDACTION': 'required', 'FLUSH_SECONDS': str(config.flush_seconds), 'SPOOL_LIMIT_BYTES': str(config.spool_bytes)}
    result = {PREFIX+key: value for key, value in values.items()}
    result.update({'VITRAL_LOCATION_'+key.upper(): value for key, value in config.location.items()})
    if config.ca_file is not None: result[PREFIX+'TRUST_CA'] = config.ca_file
    return result


def checked_config(config):
    if not isinstance(config, Configuration): raise SyncError('workload_configuration_refused')
    metadata(config.location)
    return Configuration.from_environment(environment(config))


def sources(root, *, files=32):
    """Bounded transcript-only mount walk; no symlink, home or referenced-file discovery."""
    result, visited = [], 0
    def walk(fd, prefix='', depth=0):
        nonlocal visited
        if depth > 8: raise SyncError('workload_source_limit')
        with os.scandir(fd) as entries:
            for entry in entries:
                visited += 1
                if visited > 4096: raise SyncError('workload_source_limit')
                relative = prefix+entry.name
                if len(relative.encode('utf-8')) > 1024: raise SyncError('workload_source_limit')
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    sub = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    try: walk(sub, relative+'/', depth+1)
                    finally: os.close(sub)
                elif not stat.S_ISREG(info.st_mode):
                    raise SyncError('workload_source_refused')
                elif entry.name.endswith('.jsonl'):
                    result.append(relative)
                    if len(result) > files: raise SyncError('workload_source_limit')
    try:
        if root.resolve() != root: raise SyncError('workload_source_refused')
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try: walk(fd)
        finally: os.close(fd)
    except (OSError, UnicodeError):
        raise SyncError('workload_source_refused') from None
    return tuple(sorted(result))


def bounds(config):
    # Reserve half for projection (including atomic replacements), and at most
    # one quarter for raw/view snapshots, allowing 3x JSON ASCII expansion.
    if config.spool_bytes < 1024*1024: raise SyncError('workload_spool_budget_too_small')
    output = min(16*1024*1024, config.spool_bytes//16)
    return Bounds(output_bytes=output, record_bytes=min(1024*1024, output), spool_bytes=config.spool_bytes//2)


class DeadlineHTTP(WorkloadHTTP):
    def __init__(self, origin, deadline, **options):
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= boot_clock():
            raise SyncError('workload_flush_deadline')
        super().__init__(origin, **options)
        self.deadline = deadline
        context = next(handler._context for handler in self.opener.handlers
                       if isinstance(handler, urllib.request.HTTPSHandler))
        def check():
            if boot_clock() >= deadline: raise SyncError('workload_flush_deadline')
        class Connection(http.client.HTTPSConnection):
            def connect(self):
                check()
                super().connect()
                try: check()
                except SyncError:
                    self.close(); raise

            def send(self, data):
                if self.sock is None: self.connect()
                check()  # After DNS/TLS and immediately before each byte write.
                return super().send(data)

        class Handler(urllib.request.HTTPSHandler):
            def https_open(self, request):
                return self.do_open(Connection, request, context=self._context)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), Handler(context=context))
        opener = self.opener
        class Guard:
            def open(self, request, timeout):
                remaining = deadline-boot_clock()
                if remaining <= 0: raise SyncError('workload_flush_deadline')
                return opener.open(request, timeout=min(timeout, remaining))
        self.opener = Guard()

    def request(self, *args, **kwargs):
        if kwargs.get('data') is not None and not isinstance(kwargs['data'], bytes):
            raise SyncError('workload_request_body_refused')
        try: return super().request(*args, **kwargs)
        finally:
            # Covers response bodies and HTTP-error reads as well as headers.
            if boot_clock() >= self.deadline: raise SyncError('workload_flush_deadline')


class ProofRegistry(Registry):
    """Fresh final proof of the exact existing plan, never a new generation."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.plans = {}

    def call(self, method, path, **kwargs):
        result = super().call(method, path, **kwargs)
        if method == 'PUT' and path.startswith('/sessions/') and len(path.split('/')) == 4:
            self.plans[path] = json.loads(json.dumps(kwargs['body']))
        return result

    def prove(self, state, provider):
        if not self.plans: raise SyncError('workload_final_proof_refused')
        for path, plan in list(self.plans.items()):
            local = state.session(provider, urllib.parse.unquote(path.split('/')[-1]))
            expected = {(row['file_key'], row['revision']): row for row in plan['files']}
            cursor, seen, found, count = None, set(), set(), 0
            for _ in range(1000):
                suffix = '/files?limit=100' + ('&cursor='+urllib.parse.quote(cursor, safe='') if cursor else '')
                page = self.call('GET', path+suffix)
                rows = page.get('items')
                if not isinstance(rows, list) or len(rows)+count > 10000: raise SyncError('workload_final_proof_refused')
                count += len(rows)
                for row in rows:
                    if not isinstance(row, dict) or not isinstance(row.get('file_key'), str) or not isinstance(row.get('revision'), str):
                        raise SyncError('workload_final_proof_refused')
                    key = row['file_key'], row['revision']
                    if key in expected:
                        target = expected[key]
                        if key in found or type(row.get('length')) is not int or row['length'] != target['length'] or row.get('sha256') != target['sha256']:
                            raise SyncError('workload_final_proof_refused')
                        found.add(key)
                cursor = page.get('next_cursor')
                if cursor is None: break
                if not isinstance(cursor, str) or not 1 <= len(cursor) <= 2048 or cursor in seen: raise SyncError('workload_final_proof_refused')
                seen.add(cursor)
            else: raise SyncError('workload_final_proof_refused')
            if found != set(expected) or local.get('snapshot_id') != plan['snapshot_id'] or local.get('status') != 'synced':
                raise SyncError('workload_final_proof_refused')
            if (local.get('view', {}).get('generation') != plan['view']['generation']
                    or local.get('view', {}).get('last_seq') != plan['view']['last_seq']):
                raise SyncError('workload_final_proof_refused')
            for (file_key, revision), target in expected.items():
                checkpoint = local['files'][file_key]
                if (checkpoint['revision'] != revision or checkpoint.get('offset') != target['length']
                        or checkpoint.get('offset_sha256') != target['sha256']):
                    raise SyncError('workload_final_proof_refused')
            result = self.call('PUT', path, body=plan, key=digest(plan))
            view = result.get('view')
            if (result.get('snapshot_id') != plan['snapshot_id'] or not isinstance(view, dict)
                    or view.get('generation') != plan['view']['generation'] or type(view.get('last_seq')) is not int
                    or view['last_seq'] != plan['view']['last_seq'] or not isinstance(result.get('sync'), dict)
                    or result['sync'].get('complete') is not True):
                raise SyncError('workload_final_proof_refused')


def source_manifest(config, adapter):
    if len(adapter.sources) != len(adapter.prepared): raise SyncError('workload_capture_incomplete')
    manifest = []
    for (_, relative), entry in zip(adapter.sources, adapter.prepared.values()):
        cursor = entry['source_cursor']
        if cursor['partial']: raise SyncError('workload_source_incomplete')
        verify_prefix(config.transcript_root, relative, cursor)
        with opened_source(config.transcript_root, relative) as source:
            info = os.fstat(source.fileno())
            if info.st_size != cursor['complete_length'] or (info.st_dev, info.st_ino) != (cursor['device'], cursor['inode']):
                raise SyncError('source_changed')
        manifest.append({'path': relative, **cursor})
    if sources(config.transcript_root) != tuple(sorted(row['path'] for row in manifest)):
        raise SyncError('source_changed')
    return manifest


def run_pass(config, deadline):
    config = checked_config(config)
    limits = bounds(config)
    runtime = child(config.state_root, 'runtime')
    sync = child(config.state_root, 'sync')
    with exclusive(sync):
        key = WorkloadKey(config.state_root/'identity')
        http = DeadlineHTTP(config.origin, deadline, ca_file=config.ca_file, attempts=1, timeout=5)
        tokens = WorkloadTokens(child(config.state_root, 'auth'), config.origin, config.location, http=http, key=key,
                               stopped=lambda: boot_clock() >= deadline)
        identity = tokens.status()
        if identity['state'] not in ('ready', 'refresh_inflight'):
            raise SyncError('workload_approval_required')
        binding = {'scope': scope(config), 'account_id': identity['account_id'], 'device_id': identity['device_id']}
        previous = read_record(runtime/'binding.json')
        if previous is not None and previous != binding: raise SyncError('workload_runtime_binding_changed')
        projection_path = config.state_root/'projection'
        populated_projection = projection_path.exists() and (projection_path.is_symlink() or not projection_path.is_dir()
                                                            or any(projection_path.iterdir()))
        if previous is None and ((sync/'state.sqlite').exists() or (runtime/'sources.json').exists() or populated_projection):
            raise SyncError('workload_runtime_binding_changed')
        private_json(runtime/'binding.json', binding)
        selected = sources(config.transcript_root)
        known = read_record(runtime/'sources.json') or {'paths': []}
        if not isinstance(known.get('paths'), list) or not set(known['paths']) <= set(selected):
            raise SyncError('workload_source_disappeared')
        cursor = known.get('next_index', 0)
        if type(cursor) is not int or not 0 <= cursor < max(1, len(selected)):
            raise SyncError('workload_runtime_record_refused')
        private_json(runtime/'sources.json', {'paths': list(selected), 'next_index': (cursor+1) % max(1, len(selected))})
        if not selected: return {'ok': False, 'synced': 0, 'pending': 1, 'manifest': []}
        ordered = selected[cursor:]+selected[:cursor]
        inputs = tuple((config.transcript_root, path) for path in ordered)
        projection = child(config.state_root, 'projection')
        adapter = (WorkloadGeminiProvider(inputs, projection, bounds=limits) if config.provider == 'gemini'
                   else WorkloadTextProvider(config.provider, inputs, projection, bounds=limits))
        state = State(sync)
        try:
            state.set('device_id', identity['device_id'])
            state.set('settings', {'label': config.location['label'], 'kind': 'workload'})
            registry = ProofRegistry(http.origin.registry, tokens, http=http)
            result = Collector(state, registry, adapter).run(
                providers=[config.provider], days=None)
            if result['pending']: return {'ok': False, 'synced': result['synced'], 'pending': result['pending'], 'manifest': []}
            registry.prove(state, config.provider)
            manifest = source_manifest(config, adapter)
            return {'ok': True, 'synced': result['synced'], 'pending': 0, 'manifest': manifest}
        finally:
            state.close()


class PassProcess:
    def __init__(self, config, sequence, deadline):
        try: valid = isinstance(sequence, str) and sequence == str(uuid.UUID(sequence))
        except ValueError: valid = False
        if not valid: raise SyncError('workload_runtime_record_refused')
        config = checked_config(config)
        if type(deadline) not in (int, float) or not math.isfinite(deadline) or deadline <= boot_clock():
            raise SyncError('workload_flush_deadline')
        self.config = config
        self.directory = child(child(config.state_root, 'runtime'), 'receipts')
        self.path = self.directory/(sequence+'.json')
        self.sequence = sequence
        home = child(config.state_root, 'worker-home')
        if boot_clock() >= deadline: raise SyncError('workload_flush_deadline')
        self.process = subprocess.Popen([sys.executable, '-S', str(Path(__file__).absolute()), '--worker',
            str(self.path), str(os.getpid()), str(deadline)],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
            env={'PATH': os.defpath, 'LANG': 'C.UTF-8', 'HOME': str(home), 'PYTHONDONTWRITEBYTECODE': '1'})

    def poll(self): return self.process.poll()

    def receipt(self):
        if self.process.returncode != 0: return None
        value = read_record(self.path)
        if (value is None or value.get('sequence') != self.sequence or value.get('scope') != scope(self.config)
                or type(value.get('ok')) is not bool or type(value.get('synced')) is not int
                or type(value.get('pending')) is not int or min(value['synced'], value['pending']) < 0
                or not isinstance(value.get('manifest'), list) or len(value['manifest']) > 32
                or value['ok'] and (not value['manifest'] or value['synced'] != len(value['manifest']) or value['pending'] != 0)):
            return None
        return value

    def remove_receipt(self):
        self.path.unlink(missing_ok=True)

    def kill(self):
        try: os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError: pass

    def reap(self, seconds):
        try: self.process.wait(timeout=max(0, seconds))
        except subprocess.TimeoutExpired: return False
        return True


@contextmanager
def stop_signals(stop):
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    for sig in previous: signal.signal(sig, lambda *_: stop())
    try: yield
    finally:
        for sig, handler in previous.items(): signal.signal(sig, handler)


class Runner:
    def __init__(self, config, *, pass_factory=PassProcess, clock=boot_clock, sleep=time.sleep):
        config = checked_config(config)
        bounds(config)
        self.config, self.pass_factory, self.clock, self.sleep = config, pass_factory, clock, sleep
        self.stopping = False

    def stop(self): self.stopping = True

    def run(self, command=None):
        runtime = child(self.config.state_root, 'runtime')
        with exclusive(runtime, 'runner.lock'), stop_signals(self.stop):
            owner = {'version': 2, 'scope': scope(self.config)}
            previous = read_record(runtime/'runner.json')
            if previous is None:
                # The lock is the only expected entry before the first claim.
                with os.scandir(runtime) as entries:
                    if any(entry.name != 'runner.lock' for entry in entries):
                        raise SyncError('workload_runtime_storage_refused')
                private_json(runtime/'runner.json', owner)
            elif previous != owner:
                raise SyncError('workload_runtime_binding_changed')
            recover_producer(runtime, scope(self.config))
            clean_receipts(runtime)
            private_json(runtime/'configuration.json', environment(self.config))
            return self._run(command)

    def _run(self, command):
        writer = None
        writer_group_finished = False
        worker = None
        drain_deadline = None
        report = {'complete': False, 'pending': True, 'producer_quiesced': False,
                  'writer_exit': None, 'synced': 0, 'worker_reaped': True}
        try:
            if command is not None:
                if not isinstance(command, (list, tuple)) or not command or any(not isinstance(v, str) or '\x00' in v for v in command):
                    raise SyncError('workload_command_refused')
                if self.stopping: return report
                # The writer inherits its own environment without inspection;
                # collector children instead receive only the allowlist above.
                writer = Producer(child(self.config.state_root, 'runtime'), scope(self.config), command)
            next_pass = self.clock()
            while not self.stopping and (writer is None or writer.poll() is None):
                if worker is None and self.clock() >= next_pass:
                    started = self.clock()
                    worker = self.pass_factory(self.config, str(uuid.uuid4()), started+PASS_SECONDS)
                if worker is not None:
                    if worker.poll() is not None:
                        receipt = worker.receipt()
                        if receipt: report['synced'] = receipt['synced']
                        worker.remove_receipt(); worker = None; next_pass = self.clock()+0.25
                    elif self.clock()-started >= PASS_SECONDS:
                        worker.kill()
                        if worker.reap(REAP_SECONDS):
                            worker.remove_receipt(); worker = None; next_pass = self.clock()+1
                self.sleep(POLL_SECONDS)
            if writer is not None:
                writer.stop()
                until = self.clock()+WRITER_SECONDS+2*REAP_SECONDS
                while writer.poll() is None and self.clock() < until: self.sleep(POLL_SECONDS)
                try: writer.wait(timeout=max(0, min(REAP_SECONDS, until-self.clock())))
                except subprocess.TimeoutExpired: pass
                report['producer_quiesced'] = writer.quiesced()
                writer_group_finished = report['producer_quiesced']
                report['writer_exit'] = writer.returncode
            # The budget begins after producer quiescence/its bounded attempt.
            deadline = self.clock()+self.config.flush_seconds
            drain_deadline = deadline
            if worker is not None:
                worker.kill()
                report['worker_reaped'] = worker.reap(min(REAP_SECONDS, max(0, deadline-self.clock())))
                worker.remove_receipt(); worker = None
                if not report['worker_reaped']: return report
            while self.clock() < deadline-REAP_SECONDS:
                if worker is None: worker = self.pass_factory(self.config, str(uuid.uuid4()), deadline-REAP_SECONDS)
                if worker.poll() is not None:
                    receipt = worker.receipt(); worker.remove_receipt(); worker = None
                    if receipt and receipt['ok'] and receipt['pending'] == 0:
                        # This fresh worker captured and checked EOF after both
                        # content and view ACKs, after producer quiescence. Keep
                        # hashing/I/O inside the killable worker, not the timer.
                        report.update(synced=receipt['synced'], pending=not report['producer_quiesced'])
                        report['complete'] = report['producer_quiesced']
                        if self.clock() >= deadline: report.update(complete=False, pending=True)
                        return report
                self.sleep(POLL_SECONDS)
            return report
        except (OSError, SyncError):
            return report
        finally:
            if worker is not None:
                worker.kill()
                remaining = REAP_SECONDS if drain_deadline is None else min(REAP_SECONDS, max(0, drain_deadline-self.clock()))
                report['worker_reaped'] = worker.reap(remaining); worker.remove_receipt()
            if writer is not None and not writer_group_finished:
                writer.stop()
                remaining = REAP_SECONDS if drain_deadline is None else min(REAP_SECONDS, max(0, drain_deadline-self.clock()))
                try: writer.wait(timeout=remaining)
                except subprocess.TimeoutExpired: pass


def parent_death(parent):
    # Linux unprivileged process-lifetime binding; no preexec_fn in a threaded
    # host. Recheck the parent to close the fork-to-prctl death race.
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0 or os.getppid() != parent:
        raise SyncError('workload_parent_unavailable')


def worker_main(receipt, parent, deadline):
    parent_death(parent)
    if not math.isfinite(deadline) or deadline <= boot_clock(): raise SyncError('workload_flush_deadline')
    receipt = Path(receipt)
    if (not receipt.is_absolute() or receipt.resolve() != receipt or receipt.parent.name != 'receipts'
            or receipt.parent.parent.name != 'runtime' or receipt.name != str(uuid.UUID(receipt.stem))+'.json'):
        raise SyncError('workload_runtime_storage_refused')
    values = read_record(receipt.parent.parent/'configuration.json')
    if values is None: raise SyncError('workload_configuration_refused')
    config = Configuration.from_environment(values)
    directory = child(child(config.state_root, 'runtime'), 'receipts')
    if receipt.parent != directory or receipt.name != str(uuid.UUID(receipt.stem))+'.json':
        raise SyncError('workload_runtime_storage_refused')
    try: result = run_pass(config, deadline)
    except Exception: result = {'ok': False, 'synced': 0, 'pending': 1, 'manifest': []}
    if boot_clock() >= deadline: result.update(ok=False, pending=1)
    private_json(receipt, {'sequence': receipt.stem, 'scope': scope(config), **result})


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        if len(argv) == 4 and argv[0] == '--worker':
            worker_main(argv[1], int(argv[2]), float(argv[3])); return 0
        if argv and argv[0] != '--': raise SyncError('workload_command_refused')
        config = Configuration.from_environment(os.environ)
        result = Runner(config).run(argv[1:] if argv else None)
        print(json.dumps(result, separators=(',', ':')))
        if not result['complete']: return 2
        code = result['writer_exit'] or 0
        return 128-code if code < 0 else code
    except Exception:
        print('{"complete":false,"pending":true,"error":"workload_run_refused"}')
        return 2


if __name__ == '__main__': raise SystemExit(main())
