"""Gemini CLI 0.68.0 text workload projection; not a native resume adapter."""
from collections import OrderedDict
import copy
from datetime import datetime
import hashlib
from pathlib import Path
import re

from csl_sync_state import SyncError
from csl_workload_stream import Bounds, ProjectionStore, Redactor, encoded, read_complete, verify_prefix

SOURCE_COMMIT = '5a5c7180ff3f0a02511f54b54db7f228d4982a30'
PROFILE = 'gemini-cli-0.68.0-jsonl-text-v1'
EMPTY = hashlib.sha256(b'').hexdigest()
METADATA = {'sessionId', 'projectHash', 'startTime', 'lastUpdated', 'summary',
            'memoryScratchpad', 'directories', 'kind'}
MESSAGE = {'id', 'timestamp', 'type', 'content', 'displayContent', 'toolCalls',
           'thoughts', 'tokens', 'model'}
TOOL = {'id', 'name', 'args', 'result', 'status', 'timestamp', 'agentId',
        'displayName', 'description', 'resultDisplay', 'renderOutputAsMarkdown'}


def refused():
    raise SyncError('gemini_workload_format_refused')


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value):
        refused()
    return value


def text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts = []
        for part in value:
            if isinstance(part, str):
                parts.append(part)
            elif (isinstance(part, dict) and set(part) <= {'text', 'thought'}
                  and isinstance(part.get('text'), str)
                  and ('thought' not in part or type(part['thought']) is bool)):
                parts.append(part['text'])
            else:
                raise SyncError('gemini_workload_opaque_refused')
        return '\n'.join(parts)
    refused()


def timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return value if parsed.tzinfo is not None else None
    except ValueError:
        return None


class Conversation:
    """Ordered state reducer matching the pinned writer's patch/rewind semantics."""
    def __init__(self):
        self.metadata = {}
        self.messages = OrderedDict()

    def message(self, value, offset):
        if (not isinstance(value, dict) or not set(value) <= MESSAGE
                or not isinstance(value.get('type'), str)
                or value['type'] not in {'user', 'gemini', 'info', 'error', 'warning'}):
            refused()
        sid = identifier(value.get('id'))
        text(value.get('content'))
        if 'displayContent' in value:
            text(value['displayContent'])
        calls = value.get('toolCalls', [])
        if not isinstance(calls, list):
            refused()
        seen = set()
        for call in calls:
            if not isinstance(call, dict) or not set(call) <= TOOL:
                refused()
            cid = identifier(call.get('id'))
            identifier(call.get('name'))
            if cid in seen or not isinstance(call.get('args'), dict):
                refused()
            seen.add(cid)
            if call.get('resultDisplay') is not None:
                text(call['resultDisplay'])
            if call.get('result') is not None:
                text(call['result'])
        self.messages[sid] = {'value': copy.deepcopy(value), 'offset': offset}

    def patch(self, patch, offset):
        if not isinstance(patch, dict) or not set(patch) <= {'id', 'content', 'toolCalls', 'updates', 'removeIds', 'orderIds'}:
            refused()
        updates = patch.get('updates', [])
        if not isinstance(updates, list):
            refused()
        if 'id' in patch:
            updates = [{k: v for k, v in patch.items() if k in {'id', 'content', 'toolCalls'}}, *updates]
        for update in updates:
            if not isinstance(update, dict) or not set(update) <= {'id', 'content', 'toolCalls'}:
                refused()
            entry = self.messages.get(identifier(update.get('id')))
            if 'content' in update:
                text(update['content'])
            calls = update.get('toolCalls', [])
            if not isinstance(calls, list):
                refused()
            for call in calls:
                if not isinstance(call, dict) or not set(call) <= {'id', 'result'}:
                    refused()
                identifier(call.get('id'))
                if call.get('result') is not None:
                    text(call['result'])
            if entry is None:
                continue
            value = entry['value']
            if 'content' in update:
                value['content'] = copy.deepcopy(update['content'])
            for changed in calls:
                if value['type'] == 'gemini':
                    for existing in value.get('toolCalls', []):
                        if existing['id'] == changed['id'] and 'result' in changed:
                            existing['result'] = copy.deepcopy(changed['result'])
            entry['offset'] = offset
        for field in ('removeIds', 'orderIds'):
            ids = patch.get(field, [])
            if not isinstance(ids, list):
                refused()
            for sid in ids:
                identifier(sid)
            if field == 'removeIds':
                for sid in ids:
                    self.messages.pop(sid, None)
            else:
                ordered = OrderedDict()
                for sid in ids:
                    if sid in self.messages:
                        ordered[sid] = self.messages[sid]
                self.messages = OrderedDict(
                    [(sid, entry) for sid, entry in self.messages.items() if sid not in ordered]
                    + list(ordered.items()))

    def apply(self, record, offset):
        if set(record) == {'$rewindTo'}:
            target = identifier(record['$rewindTo'])
            keys = list(self.messages)
            for sid in keys[keys.index(target):] if target in self.messages else keys:
                del self.messages[sid]
        elif set(record) == {'$patch'}:
            self.patch(record['$patch'], offset)
        elif 'id' in record:
            self.message(record, offset)
        else:
            values = record.get('$set') if set(record) == {'$set'} else record
            if not isinstance(values, dict) or not set(values) <= METADATA | {'messages'}:
                refused()
            for field in ('sessionId', 'projectHash'):
                if field in values:
                    identifier(values[field])
                    if field in self.metadata and self.metadata[field] != values[field]:
                        refused()
            if 'messages' in values:
                if not isinstance(values['messages'], list):
                    refused()
                self.messages.clear()
                for message in values['messages']:
                    self.message(message, offset)
            self.metadata.update({k: v for k, v in values.items() if k != 'messages'})

    def view(self, bounds):
        result, size = [], 0
        for sid, entry in self.messages.items():
            value, offset = entry['value'], entry['offset']
            role = {'user': 'user', 'gemini': 'assistant'}.get(value['type'], 'system')
            def emit(kind, body, tool=None, call=None):
                nonlocal size
                row = {'seq': len(result)+1, 'time': timestamp(value.get('timestamp')),
                    'role': 'tool' if kind == 'tool_result' else role, 'kind': kind,
                    'text': body[:16000], 'truncated': len(body) > 16000,
                    'tool': tool, 'call_id': call, 'raw_offset': {'file_key': 'main', 'offset': offset},
                    'native_pointer': '/messages/' + sid}
                size += len(encoded(row))
                if len(result) >= bounds.records or size > bounds.output_bytes:
                    raise SyncError('workload_view_limit')
                result.append(row)
            content = text(value['content'])
            if content:
                emit('text' if role != 'system' else 'notice', content)
            for call in value.get('toolCalls', []):
                emit('tool_call', encoded(call['args']).decode(), call['name'], call['id'])
                if call.get('result') is not None:
                    emit('tool_result', text(call['result']), call['name'], call['id'])
        return result


class WorkloadGeminiProvider:
    """Existing collector interface over explicit sources and sanitized private files.

    No CLI is activated here. Identity/device admission belongs to the caller.
    One exclusively locked collector owns each projection store.
    """
    supported_providers = ('gemini',)

    def __init__(self, sources, store, *, bounds=None, extra_literals=()):
        self.bounds = bounds or Bounds()
        if not isinstance(sources, tuple) or not 1 <= len(sources) <= self.bounds.files:
            raise SyncError('workload_source_limit')
        self.sources = sources
        store_root = Path(store).expanduser().absolute()
        for source in sources:
            if not isinstance(source, tuple) or len(source) != 2:
                raise SyncError('workload_source_refused')
            root, relative = source
            root = Path(root).expanduser().absolute()
            if (not isinstance(relative, str) or not relative.endswith('.jsonl')
                    or root.resolve() != root
                    or store_root.is_relative_to(root)
                    or root.is_relative_to(store_root)):
                raise SyncError('workload_source_refused')
        self.store = ProjectionStore(store_root, self.bounds)
        self.redactor = Redactor(self.bounds, extra_literals)
        self.prepared = {}

    def list_sessions(self, provider):
        if provider != 'gemini':
            raise SyncError('unsupported_provider')
        entries = {}
        for root, relative in self.sources:
            records, cursor = read_complete(root, relative, self.bounds)
            clean = self.redactor.clean(records)
            state, output = Conversation(), bytearray()
            for record in clean:
                line = encoded(record) + b'\n'
                if len(line) > self.bounds.record_bytes or len(output) + len(line) > self.bounds.output_bytes:
                    raise SyncError('workload_output_limit')
                state.apply(record, len(output))
                output.extend(line)
            sid = identifier(state.metadata.get('sessionId'))
            identifier(state.metadata.get('projectHash'))
            if state.metadata.get('kind', 'main') != 'main' or sid in entries:
                refused()
            view = state.view(self.bounds)
            title = state.metadata.get('summary')
            if title is not None and not isinstance(title, str):
                refused()
            model = next((entry['value'].get('model') for entry in reversed(list(state.messages.values()))
                          if entry['value'].get('model')), None)
            if model is not None and not isinstance(model, str):
                refused()
            verify_prefix(root, relative, cursor)
            source_key = str(Path(root).absolute()) + '/' + relative
            path = self.store.publish(source_key, bytes(output))
            self.store.checkpoint(source_key, cursor, self.redactor.fingerprint, output)
            descriptor = {'file_key': 'main', 'path': str(path), 'relative_path': 'session.jsonl',
                'mode': 'append', 'complete_length': len(output), 'size': len(output),
                'sha256': hashlib.sha256(output).hexdigest(), 'role': 'transcript'}
            metadata = {'provider': 'gemini', 'native_id': sid, 'title': (title or sid)[:200],
                'cwd': None, 'repository': None, 'created_at': timestamp(state.metadata.get('startTime')),
                'updated_at': timestamp(state.metadata.get('lastUpdated')), 'model': model,
                'record_count': len(clean), 'byte_count': len(output), 'parent': None,
                'origin': 'native', 'state': 'idle'}
            entries[sid] = {'metadata': metadata, 'file': descriptor, 'view': view,
                'source_cursor': cursor, 'projection_version': 1, 'policy_sha256': self.redactor.fingerprint}
        self.prepared = entries
        return [entry['metadata'] for entry in entries.values()]

    def _entry(self, provider, native_id):
        if provider != 'gemini' or native_id not in self.prepared:
            raise SyncError('workload_session_unprepared')
        return self.prepared[native_id]

    def raw_files(self, provider, native_id):
        return [dict(self._entry(provider, native_id)['file'])]

    def view_records(self, provider, native_id, *, position=None, limit=100):
        entry = self._entry(provider, native_id)
        records = entry['view']
        target = position.get('next_seq') if isinstance(position, dict) else 1
        if (position is not None and not isinstance(position, dict)
                or type(target) is not int or target < 1 or type(limit) is not int or not 1 <= limit <= 1000
                or position is not None and (position.get('version') != 1
                    or position.get('provider') != provider or position.get('native_id') != native_id)):
            raise SyncError('invalid_view_position')
        def digest(rows):
            return hashlib.sha256(b''.join(encoded(row)+b'\n' for row in rows)).hexdigest()
        prefix = digest(records[:target-1])
        reset = bool(position and (target > len(records)+1 or position.get('prefix_sha256') != prefix))
        page = [] if reset else records[target-1:target-1+limit]
        end = 1 if reset else target + len(page)
        source = entry['file']
        return {'records': page, 'position': {'version': 1, 'provider': provider, 'native_id': native_id,
                'next_seq': end, 'prefix_sha256': EMPTY if reset else digest(records[:end-1])},
            'reset_required': reset, 'complete': not reset and end > len(records),
            'source_file': {'file_key': 'main', 'length': source['complete_length'], 'sha256': source['sha256']}}

    def package_descriptor(self, provider, native_id):
        self._entry(provider, native_id)
        return {'available': False, 'reason': 'workload_view_only', 'descriptor_version': 1,
                'manifest': None, 'members': []}
