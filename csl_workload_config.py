"""Pure opt-in workload configuration; no login, discovery, mkdir or processes."""
from dataclasses import dataclass
import json
from pathlib import Path
import re

from csl_gemini_workload import PROFILE as GEMINI_PROFILE
from csl_sync_state import SyncError
from csl_text_workload import PROFILES
from csl_workload_http import Origin
from csl_workload_identity import CLIENT_ID, metadata
from csl_workload_observation import identity as observation_identity

with (Path(__file__).parent/'packaging/workload-collector/profiles.json').open() as _source:
    PROVIDER_PROFILES = json.load(_source)
# Refuse packaging/runtime drift rather than qualifying a different native format.
if PROVIDER_PROFILES != {**PROFILES, 'gemini': GEMINI_PROFILE}:
    raise SyncError('workload_configuration_refused')
PREFIX = 'VITRAL_COLLECTOR_'


def refused():
    raise SyncError('workload_configuration_refused')


def root_path(value):
    if not isinstance(value, str) or not value or not Path(value).is_absolute(): refused()
    path = Path(value)
    if str(path) != value or path.resolve() != path or not path.is_dir(): refused()
    return path


def integer(value, maximum):
    if not isinstance(value, str) or not re.fullmatch(r'[1-9][0-9]{0,9}', value): refused()
    result = int(value)
    if result > maximum: refused()
    return result


@dataclass(frozen=True)
class Configuration:
    origin: str
    provider: str
    profile: str
    transcript_root: Path
    state_root: Path
    location: dict
    flush_seconds: int
    spool_bytes: int
    ca_file: str | None
    observe_session: str | None = None

    @classmethod
    def from_environment(cls, env):
        # Read only the documented packaging ABI, never a harness home/token.
        if env.get(PREFIX+'CLIENT_ID', CLIENT_ID) != CLIENT_ID: refused()
        if env.get(PREFIX+'REDACTION') != 'required': refused()
        provider, profile = env.get(PREFIX+'PROVIDER'), env.get(PREFIX+'PROFILE')
        if not isinstance(provider, str) or profile != PROVIDER_PROFILES.get(provider) or profile is None: refused()
        origin = Origin(env.get(PREFIX+'ORIGIN')).value
        source = root_path(env.get(PREFIX+'TRANSCRIPT_ROOT'))
        state = root_path(env.get(PREFIX+'STATE'))
        if state.is_relative_to(source) or source.is_relative_to(state): refused()
        location, _ = metadata({name: env.get('VITRAL_LOCATION_'+name.upper())
                               for name in ('label', 'provider', 'cluster', 'namespace', 'workload', 'replica')})
        flush = integer(env.get(PREFIX+'FLUSH_SECONDS', '20'), 20)
        spool = integer(env.get(PREFIX+'SPOOL_LIMIT_BYTES', '134217728'), 536870912)
        ca = env.get(PREFIX+'TRUST_CA')
        # A public ConfigMap CA may use Kubernetes' normal symlink projection.
        # WorkloadHTTP still validates certificate and hostname when it loads it.
        if ca is not None and (not isinstance(ca, str) or not Path(ca).is_absolute() or not Path(ca).is_file()): refused()
        observe = env.get(PREFIX+'OBSERVE_SESSION')
        if observe is not None:
            try: observation_identity(observe)
            except (ValueError, TypeError, AttributeError): refused()
        return cls(origin, provider, profile, source, state, location, flush, spool, ca, observe)
