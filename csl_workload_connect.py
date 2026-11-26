"""Key-bound enrollment of a customer-managed runtime."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import stat
import struct
import subprocess
import tempfile
from uuid import UUID, uuid4

from csl_sync_fs import durable_replace, flush_directory
from csl_sync_http import HTTP
from csl_sync_state import SyncError, exclusive, private_directory, private_json
from csl_workload_http import WorkloadHTTP
from csl_workload_identity import canonical, metadata, public_key, unb64
from csl_workload_state import read_record

ISSUER = 'vitral-connect-enrollment'
CLIENT = 'vitral-connect-v1'
NAMESPACE = 'vitral-connect-enroll@vitral'
REQUEST_FIELDS = ('organization_id', 'method', 'request_id', 'challenge_id')
WORKLOAD_FIELDS = ('label', 'provider', 'cluster', 'namespace', 'workload', 'replica')
SSH_ED25519 = b'\0\0\0\x0bssh-ed25519\0\0\0\x20'


def length_prefixed(fields):
    return b''.join(struct.pack('>I', len(field)) + field for field in fields)


def ssh_public_blob(key):
    if not isinstance(key, str) or not 16 <= len(key) <= 4096:
        raise SyncError('workload_connect_key_refused')
    fields = key.split()
    if len(fields) < 2 or fields[0] != 'ssh-ed25519':
        raise SyncError('workload_connect_key_refused')
    try:
        blob = base64.b64decode(fields[1], validate=True)
    except ValueError:
        raise SyncError('workload_connect_key_refused') from None
    if len(blob) != len(SSH_ED25519) + 32 or not blob.startswith(SSH_ED25519):
        raise SyncError('workload_connect_key_refused')
    return blob


def signed_message(request, nonce):
    host, collector = request['host'], request['collector']
    tunnel = ssh_public_blob(host['tunnel_public_key'])
    host_key = ssh_public_blob(host['host_public_key'])
    if tunnel == host_key:
        raise SyncError('workload_connect_key_refused')
    jwk = collector['public_jwk']
    public, thumbprint = public_key(unb64(jwk.get('x'), 32))
    if public != jwk:
        raise SyncError('workload_connect_key_refused')
    workload, _ = metadata(collector['workload'])
    fields = [request[name] for name in REQUEST_FIELDS]
    fields.extend((host['username'], host['hostname']))
    fields.extend(workload[name] for name in WORKLOAD_FIELDS)
    packed = length_prefixed([field.encode('utf-8') for field in fields] + [tunnel, host_key, thumbprint])
    digest = hashlib.sha256(b'vitral-connect-exchange-v1\0' + packed).digest()
    credential_hash = hashlib.sha256(request['credential'].encode('utf-8')).digest()
    return b'vitral-host-key-proof-v1' + length_prefixed((
        ISSUER.encode('ascii'), CLIENT.encode('ascii'), credential_hash, unb64(nonce, 32), thumbprint, digest,
    )), thumbprint


def proofs_for(request, nonce, host_state, collector_key):
    message, thumbprint = signed_message(request, nonce)
    try:
        host = subprocess.check_output(
            ['ssh-keygen', '-q', '-Y', 'sign', '-n', NAMESPACE, '-f', str(host_state / 'tunnel_key')],
            input=message, stderr=subprocess.DEVNULL, timeout=15,
        ).decode('ascii')
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise SyncError('workload_connect_host_proof_refused') from None
    if not host.startswith('-----BEGIN SSH SIGNATURE-----\n') or len(host) > 8192:
        raise SyncError('workload_connect_host_proof_refused')
    return {'host': host, 'collector': collector_key.sign_message(message, thumbprint)}


class ConnectHTTP(WorkloadHTTP):
    def post(self, resource, payload):
        if resource not in ('challenges', 'exchanges') or self.origin.authority[1] != 443:
            raise SyncError('workload_connect_endpoint_refused')
        # The normal workload client only admits auth and registry paths.
        return HTTP.request(self, self.origin.value + '/vitral-connect/v1/' + resource,
                            'POST', data=canonical(payload), headers={'Content-Type': 'application/json'})


def credential_from_file(path):
    # Kubernetes projects its read-only token through a link; verify the file
    # we open, including when that projection has just been rotated.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.getuid())
                or info.st_mode & 0o037 or info.st_size > 16385):
            raise SyncError('workload_connect_credential_refused')
        raw = stream.read(16386).removesuffix(b'\n')
    if not re.fullmatch(rb'[\x21-\x7e]{1,16384}', raw):
        raise SyncError('workload_connect_credential_refused')
    return raw.decode('ascii')


def request_uuid(value):
    try:
        if isinstance(value, str) and str(UUID(value)) == value:
            return value
    except ValueError:
        pass
    raise SyncError('workload_connect_response_refused')


def run_host(argv, env):
    try:
        return subprocess.check_output(argv, env=env, stderr=subprocess.DEVNULL, timeout=45)
    except (OSError, subprocess.SubprocessError):
        raise SyncError('workload_connect_host_unavailable') from None


def enroll_runtime(config, tokens, *, organization, method, credential_path, host_state, host_binary,
                   host_kind, http=None):
    if (not re.fullmatch(r'[a-f0-9]{24}', organization)
            or method not in ('oidc', 'join_token') or host_kind not in ('container', 'ssh')
            or not host_state.is_absolute() or host_state.resolve() != host_state
            or host_state.is_relative_to(config.state_root) or config.state_root.is_relative_to(host_state)):
        raise SyncError('workload_connect_configuration_refused')
    host_state = private_directory(host_state)
    env = {key: os.environ[key] for key in ('HOME', 'PATH', 'USER') if key in os.environ}
    env.update(VITRAL_TUNNEL_HOME=str(host_state), VITRAL_API=config.origin,
               VITRAL_HOST_TERMINAL_MODE='embedded', VITRAL_TUNNEL_KIND=host_kind)
    http = http or ConnectHTTP(config.origin, ca_file=config.ca_file)
    with exclusive(host_state, 'connect.lock'):
        identity = tokens.connect_identity()
        host = json.loads(run_host([host_binary, 'assignment-identity'], env))
        host = {name: host[name] for name in ('username', 'tunnel_public_key', 'host_public_key')}
        host['hostname'] = socket.gethostname() or 'host'
        collector = {'public_jwk': identity['public_jwk'], 'workload': config.location}
        journal = host_state / 'connect.json'
        scope = hashlib.sha256(canonical({'organization': organization, 'method': method,
                                          'origin': config.origin, 'host': host, 'collector': collector})).hexdigest()
        intent = read_record(journal)
        if intent is None:
            if (host_state/'api.HOST_ID').exists() or identity['state'] != 'signed_out':
                raise SyncError('workload_connect_existing_grant')
            intent = {'scope': scope, 'request_id': str(uuid4())}
            private_json(journal, intent, durable=True)
        if intent.get('scope') != scope or (host_state/'revoked').exists():
            raise SyncError('workload_connect_existing_grant')
        assignment = intent.get('assignment', {})
        if not assignment and (host_state/'api.HOST_ID').exists():
            raise SyncError('workload_connect_existing_grant')
        result = {
            'enrollment_id': assignment.get('enrollment_id'),
            'environment_id': assignment.get('environment_id'),
        }
        if intent.get('complete') is True:
            if identity['state'] not in ('ready', 'refresh_inflight'):
                raise SyncError('workload_connect_existing_grant')
            return {**result, 'host_id': enrolled_host(host_state, result['environment_id'])}

        request = {'organization_id': organization, 'method': method, 'request_id': request_uuid(intent['request_id'])}
        challenge = http.post('challenges', request)
        request = {**request, 'challenge_id': request_uuid(challenge.get('challenge_id')),
                   'credential': credential_from_file(credential_path), 'host': host, 'collector': collector}
        request['proofs'] = proofs_for(request, challenge.get('nonce'), host_state, tokens.key)
        answer = http.post('exchanges', request)
        received = {key: request_uuid(answer.get(key)) for key in ('enrollment_id', 'environment_id')}
        generation = answer.get('generation')
        if type(generation) is not int or not 1 <= generation < 2**64:
            raise SyncError('workload_connect_response_refused')
        received['generation'] = generation
        if assignment and received != assignment:
            raise SyncError('workload_connect_assignment_changed')
        for part in ('host', 'collector'):
            section = answer.get(part)
            assertion = section.get('assignment_assertion') if isinstance(section, dict) else None
            if not isinstance(assertion, str) or len(assertion) > 8192 or not re.fullmatch(
                    r'[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+', assertion):
                raise SyncError('workload_connect_response_refused')
        intent['assignment'] = received
        private_json(journal, intent, durable=True)
        result = {'enrollment_id': received['enrollment_id'], 'environment_id': received['environment_id']}
        with tempfile.TemporaryDirectory(prefix='connect-assignment-', dir=host_state) as directory:
            assertion_file = Path(directory) / 'assignment'
            fd = os.open(assertion_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w', encoding='ascii') as stream:
                stream.write(answer['host']['assignment_assertion'])
            run_host([host_binary, 'enroll', '--assignment-file', str(assertion_file)], env)
        result['host_id'] = enrolled_host(host_state, received['environment_id'])
        tokens.enroll_connect(answer['collector']['assignment_assertion'])
        handoff = config.state_root / 'host-id'
        with tempfile.NamedTemporaryFile(mode='w', encoding='ascii', dir=config.state_root,
                                         prefix='.host-id-', delete=False) as temporary:
            temporary.write(result['host_id'] + '\n')
            temporary.flush()
            os.fsync(temporary.fileno())
        try:
            durable_replace(temporary.name, handoff)
            flush_directory(config.state_root)
        finally:
            Path(temporary.name).unlink(missing_ok=True)
        intent['complete'] = True
        private_json(journal, intent, durable=True)
        return result


def enrolled_host(state, environment):
    host_id = (state/'api.HOST_ID').read_text(encoding='ascii').strip()
    stored_environment = (state/'api.ENVIRONMENT_ID').read_text(encoding='ascii').strip()
    if stored_environment != environment or not re.fullmatch(r'host-[a-f0-9]{16}', host_id):
        raise SyncError('workload_connect_assignment_changed')
    return host_id
