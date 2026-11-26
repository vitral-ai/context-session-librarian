"""Workload metadata and W7 LP-v1 proof codec. No login or token authority."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import subprocess
import tempfile

from csl_sync_state import SyncError, private_directory
from csl_secrets import PATTERNS
from csl_workload_http import Origin

CLIENT_ID = 'vitral-workload-collector'
SCOPES = 'session_sync offline_access'
AUDIENCE = 'vitral-session-registry'
FIELDS = frozenset({'label', 'provider', 'cluster', 'namespace', 'workload', 'replica'})
IDENTIFIER = re.compile(r'[A-Za-z0-9._:/-]{1,128}', re.ASCII)
SPKI_PREFIX = bytes.fromhex('302a300506032b6570032100')


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False,
                      allow_nan=False, separators=(',', ':')).encode('utf-8')


def metadata(value):
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise SyncError('workload_metadata_refused')
    for name, text in value.items():
        if not isinstance(text, str) or not 1 <= len(text) <= 128:
            raise SyncError('workload_metadata_refused')
        try:
            if name == 'label':
                if (not text or text.strip() != text or not text.isprintable()
                        or len(text.encode('utf-8')) > 128):
                    raise ValueError()
            elif not IDENTIFIER.fullmatch(text) or name == 'provider' and text != text.lower():
                raise ValueError()
        except (ValueError, UnicodeError):
            raise SyncError('workload_metadata_refused') from None
    if any(pattern.search(text) for text in value.values() for _, pattern, _ in PATTERNS):
        raise SyncError('workload_metadata_refused')
    result = dict(value)
    return result, hashlib.sha256(canonical(result)).hexdigest()


def b64(value):
    return base64.urlsafe_b64encode(value).rstrip(b'=').decode('ascii')


def unb64(value, length):
    if (not isinstance(value, str) or len(value) != (length * 8 + 5)//6
            or not re.fullmatch(r'[A-Za-z0-9_-]+', value, re.ASCII)):
        raise SyncError('workload_proof_refused')
    try:
        raw = base64.urlsafe_b64decode(value + '=' * (-len(value) % 4))
        if len(raw) != length or b64(raw) != value:
            raise ValueError()
        return raw
    except ValueError:
        raise SyncError('workload_proof_refused') from None


def public_key(raw):
    if not isinstance(raw, bytes) or len(raw) != 32:
        raise SyncError('workload_proof_refused')
    jwk = {'crv': 'Ed25519', 'kty': 'OKP', 'x': b64(raw)}
    return jwk, hashlib.sha256(canonical(jwk)).digest()


def proof_message(issuer, credential, nonce, thumbprint, metadata_sha256, *, refresh=False):
    if (not isinstance(issuer, str) or not issuer or len(issuer) > 2048
            or not isinstance(credential, str) or not 1 <= len(credential) <= 4096
            or type(refresh) is not bool
            or any(not isinstance(v, bytes) or len(v) != 32
                   for v in (nonce, thumbprint, metadata_sha256))):
        raise SyncError('workload_proof_refused')
    if not issuer.endswith('/native-auth/v1'):
        raise SyncError('workload_proof_refused')
    Origin(issuer[:-len('/native-auth/v1')])
    try:
        fields = (issuer.encode('utf-8'), CLIENT_ID.encode('ascii'),
                  hashlib.sha256(credential.encode('utf-8')).digest(),
                  nonce, thumbprint, metadata_sha256)
        domain = b'vitral-host-refresh-proof-v1' if refresh else b'vitral-host-key-proof-v1'
        return domain + b''.join(struct.pack('>I', len(value)) + value for value in fields)
    except (UnicodeError, struct.error):
        raise SyncError('workload_proof_refused') from None


class WorkloadKey:
    """Optional OpenSSL 3 Ed25519 helper; private persisted replica key only.

    Caller holds the existing exclusive collector lock. No import from harness
    stores and no automatic key replacement or rotation is performed.
    """
    def __init__(self, root, *, executable=None):
        root = Path(root).expanduser().absolute()
        if root.resolve() != root:
            raise SyncError('workload_key_storage_refused')
        try:
            info = root.lstat()
        except FileNotFoundError:
            pass
        else:
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o700):
                raise SyncError('workload_key_storage_refused')
        self.root = private_directory(root)
        self.path = self.root / 'workload-ed25519.pem'
        binary = executable or shutil.which('openssl')
        if not binary or not Path(binary).is_absolute():
            raise SyncError('workload_crypto_unavailable')
        self.binary = str(binary)

    def _run(self, *arguments):
        result = None
        try:
            # No cloud/harness credentials, OpenSSL config overrides or output
            # from the helper are inherited/logged. Secret bytes never enter argv.
            result = subprocess.run([self.binary, *arguments], stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=5,
                env={'PATH': os.defpath, 'LANG': 'C', 'HOME': str(self.root)})
        except (OSError, subprocess.SubprocessError):
            pass
        if result is None or result.returncode or len(result.stdout) > 4096:
            result = None
            raise SyncError('workload_crypto_unavailable')
        return result.stdout

    def _existing(self):
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return False
        except OSError:
            raise SyncError('workload_key_storage_refused') from None
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or not 1 <= info.st_size <= 4096):
                raise SyncError('workload_key_storage_refused')
        finally:
            os.close(fd)
        return True

    def ensure(self):
        if not self._existing():
            pem = self._run('genpkey', '-algorithm', 'ED25519')
            # O_EXCL means a racing initializer can never replace an identity.
            try:
                fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            except FileExistsError:
                raise SyncError('workload_key_storage_refused') from None
            with os.fdopen(fd, 'wb') as target:
                target.write(pem); target.flush(); os.fsync(target.fileno())
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try: os.fsync(directory)
            finally: os.close(directory)
        return self.public()

    def public(self):
        if not self._existing():
            raise SyncError('workload_key_unavailable')
        der = self._run('pkey', '-in', str(self.path), '-passin', 'pass:', '-pubout', '-outform', 'DER')
        if len(der) != len(SPKI_PREFIX)+32 or not der.startswith(SPKI_PREFIX):
            raise SyncError('workload_proof_refused')
        return public_key(der[len(SPKI_PREFIX):])

    def proof(self, issuer, credential, nonce, metadata_sha256, *, expected_thumbprint, refresh=False):
        message = proof_message(issuer, credential, nonce, expected_thumbprint, metadata_sha256, refresh=refresh)
        return self.sign_message(message, expected_thumbprint)

    def sign_message(self, message, expected_thumbprint):
        _, thumbprint = self.public()
        if not isinstance(expected_thumbprint, bytes) or thumbprint != expected_thumbprint:
            raise SyncError('workload_key_binding_changed')
        fd, name = tempfile.mkstemp(prefix='.proof-', dir=self.root)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, 'wb') as target:
                target.write(message)
            signature = self._run('pkeyutl', '-sign', '-rawin', '-inkey', str(self.path), '-passin', 'pass:', '-in', name)
            if len(signature) != 64:
                raise SyncError('workload_proof_refused')
            return b64(signature)
        finally:
            Path(name).unlink(missing_ok=True)
