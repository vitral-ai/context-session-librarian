"""Explicit-source Codex/Claude workload text projections, never native packages."""
from itertools import chain
import hashlib
import re
from urllib.parse import urlsplit

from csl_canonical import strict_json
from csl_gemini_workload import identifier, timestamp
from csl_sync_providers import _view
from csl_sync_state import SyncError
from csl_workload_provider import PreparedProvider
from csl_workload_stream import OPAQUE, StructureBudget, encoded

PROFILES = {'codex': 'codex-0.169.0-text-v1', 'claude': 'claude-2.1.308-text-v1'}
VERSIONS = {'codex': '0.169.0', 'claude': '2.1.308'}
MARKER = 'workload_omission_v1'
PROFILE_MARKER = 'workload_profile_v1'
TOOL_DECLARATIONS_NOTICE = 'Native tool declarations omitted; workload text is view-only.'
PROVENANCE_FIELDS = {'csl_conversion', 'csl_source_provider', 'csl_source_session_id',
                     'forkedFrom', 'forked_from_id', 'forked_from_ordinal_exclusive',
                     'parent_thread_id'}
TEXT_BLOCKS = {'text', 'input_text', 'output_text'}
CLAUDE_META = {'custom-title', 'file-history-snapshot', 'queue-operation', 'progress',
               'system', 'summary', 'last-prompt', 'agent-name', 'bridge_status'}
CODEX_EVENTS = {'task_started', 'task_complete', 'item_started', 'item_completed',
                'thread_name_updated', 'user_message', 'agent_message', 'token_count',
                'agent_reasoning', 'agent_reasoning_raw_content', 'turn_aborted',
                'exec_command_begin', 'exec_command_end', 'exec_command_output_delta',
                'mcp_tool_call_begin', 'mcp_tool_call_end'}


def refused():
    raise SyncError('workload_text_format_refused')


def selected(value, names):
    return {name: value[name] for name in names if name in value}


def tool_declarations(value):
    """Validate the bounded readable catalog before omitting it in its entirety."""
    if (not {'type', 'role', 'tools'} <= set(value) <= {'type', 'id', 'role', 'tools'}
            or value['role'] not in ('system', 'developer', 'user', 'assistant')
            or not isinstance(value['tools'], list)
            or any(not isinstance(tool, dict) for tool in value['tools'])):
        refused()
    if value.get('id') is not None:
        identifier(value['id'])
    # project() has validated every descendant against the capture-wide budget.
    # Visit credential subtrees too: omission must not hide an opaque schema.
    pending = [value['tools']]
    while pending:
        item = pending.pop()
        if isinstance(item, dict):
            if (any(key in OPAQUE for key in item)
                    or item.get('type') in ('image', 'document', 'audio', 'video',
                                           'base64', 'input_image', 'input_audio', 'input_file')):
                raise SyncError('workload_text_opaque_or_block_refused')
            for name in ('image_url', 'audio_url'):
                media = item.get(name)
                if isinstance(media, dict):
                    media = media.get('url')
                if isinstance(media, str):
                    try:
                        inline = urlsplit(media).scheme == 'data'
                    except ValueError:
                        inline = True
                    if inline:
                        raise SyncError('workload_text_opaque_or_block_refused')
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def blocks(value, *, claude=False):
    """Only declared readable blocks; opaque reasoning becomes a fixed notice."""
    if isinstance(value, str):
        return value, False
    if not isinstance(value, list):
        refused()
    result, omitted = [], False
    for block in value:
        if not isinstance(block, dict) or not isinstance(block.get('type'), str):
            refused()
        kind = block['type']
        if kind in TEXT_BLOCKS:
            if not isinstance(block.get('text'), str): refused()
            result.append({'type': kind, 'text': block['text']})
        elif claude and kind == 'tool_use':
            identifier(block.get('id')); identifier(block.get('name'))
            if not isinstance(block.get('input'), dict): refused()
            result.append(selected(block, ('type', 'id', 'name', 'input')))
        elif claude and kind == 'tool_result':
            identifier(block.get('tool_use_id'))
            content, nested_omission = blocks(block.get('content'))
            if nested_omission: refused()
            result.append({'type': kind, 'tool_use_id': block['tool_use_id'], 'content': content})
        elif claude and kind in ('thinking', 'redacted_thinking'):
            # Explicitly qualified omission, not a scan/decode of hidden state.
            omitted = True
        else:
            raise SyncError('workload_text_opaque_or_block_refused')
    return result, omitted


class WorkloadTextProvider(PreparedProvider):
    def __init__(self, provider, sources, store, **options):
        if not isinstance(provider, str) or provider not in PROFILES:
            raise SyncError('unsupported_provider')
        self.provider, self.profile = provider, PROFILES[provider]
        super().__init__(sources, store, **options)

    @property
    def policy_fingerprint(self):
        policy = {'profile': self.profile, 'redaction_sha256': self.redactor.fingerprint}
        if self.provider == 'codex':
            policy['tool_declarations_omission'] = 1
        return hashlib.sha256(encoded(policy)).hexdigest()

    def _codex(self, record, budget):
        kind, value = record.get('type'), record.get('payload', {})
        if (MARKER in record or PROFILE_MARKER in record
                or not isinstance(kind, str) or not isinstance(value, dict)): refused()
        result = {'type': kind, 'timestamp': timestamp(record.get('timestamp')), 'payload': {}}
        if kind == 'session_meta':
            for header in (record, value):
                if (any(field in header for field in PROVENANCE_FIELDS)
                        or header.get('attached_thread') is not None and header['attached_thread'] is not False
                        or 'source' in header and (not isinstance(header['source'], str)
                                                  or header['source'] not in ('cli', 'vscode', 'exec'))):
                    raise SyncError('workload_text_provenance_refused')
            if value.get('history_base') is not None:
                raise SyncError('workload_external_history_refused')
            if value.get('history_mode') not in (None, 'legacy', 'paginated'): refused()
            if value.get('cli_version') != VERSIONS['codex']: refused()
            sid = value.get('id', value.get('session_id'))
            identifier(sid)
            if value.get('id', sid) != sid or value.get('session_id', sid) != sid: refused()
            result['payload'] = {'id': sid}
        elif kind == 'turn_context':
            result['payload'] = selected(value, ('model',))
        elif kind == 'event_msg':
            identifier(value.get('type'))
            if value['type'] not in CODEX_EVENTS: refused()
            result['payload'] = selected(value, ('type',))
            if value['type'] == 'thread_name_updated':
                if not isinstance(value.get('thread_name'), str): refused()
                result['payload']['thread_name'] = value['thread_name']
        elif kind == 'compacted':
            result[MARKER] = 'Native compaction state omitted; workload text is view-only.'
        elif kind == 'response_item':
            subtype = value.get('type')
            if subtype == 'additional_tools':
                tool_declarations(value)
                result['payload'] = {'type': subtype}
                result[MARKER] = TOOL_DECLARATIONS_NOTICE
            elif subtype == 'message':
                if value.get('role') not in ('user', 'assistant', 'system', 'developer'): refused()
                content, omitted = blocks(value.get('content'))
                if omitted: refused()
                result['payload'] = {'type': subtype, 'role': value['role'], 'content': content}
            elif subtype in ('function_call', 'custom_tool_call'):
                identifier(value.get('call_id')); identifier(value.get('name'))
                result['payload'] = selected(value, ('type', 'call_id', 'name'))
                field = 'arguments' if subtype == 'function_call' else 'input'
                if not isinstance(value.get(field), str): refused()
                if field == 'arguments':
                    try: arguments = strict_json(value[field])
                    except (ValueError, UnicodeError, RecursionError): refused()
                    if not isinstance(arguments, dict): refused()
                    # Charge expanded JSON to the same capture-wide budget as
                    # source records, before any credential subtree is removed.
                    budget.check(arguments, depth=3)
                    # Decode before scanning, so short/numeric credential fields
                    # cannot hide inside the native JSON-encoded string.
                    result['payload'][field] = arguments
                else:
                    result['payload'][field] = value[field]
            elif subtype in ('function_call_output', 'custom_tool_call_output'):
                identifier(value.get('call_id'))
                content, omitted = blocks(value.get('output'))
                if omitted: refused()
                result['payload'] = {'type': subtype, 'call_id': value['call_id'], 'output': content}
            elif subtype == 'reasoning':
                result['payload'] = {'type': 'reasoning'}
                result[MARKER] = 'Native reasoning omitted from view-only workload text.'
            else:
                refused()
        else:
            refused()
        return result

    def _claude(self, record):
        kind = record.get('type')
        if not isinstance(kind, str): refused()
        if (MARKER in record or PROFILE_MARKER in record or record.get('isSidechain')
                or record.get('isCompactSummary')):
            raise SyncError('workload_claude_branch_refused')
        if any(record.get(field) is not None and record.get(field) is not False
               for field in ('isSidechain', 'isCompactSummary')):
            raise SyncError('workload_claude_branch_refused')
        if record.get('version', VERSIONS['claude']) != VERSIONS['claude']: refused()
        result = {'type': kind, 'timestamp': timestamp(record.get('timestamp'))}
        for field in ('sessionId', 'session_id', 'uuid', 'parentUuid'):
            if record.get(field) is not None:
                identifier(record[field]); result[field] = record[field]
        if kind in ('user', 'assistant'):
            if record.get('version') != VERSIONS['claude']: refused()
            if not record.get('sessionId', record.get('session_id')) or not record.get('uuid'): refused()
            message = record.get('message')
            if not isinstance(message, dict) or message.get('role', kind) != kind: refused()
            content, omitted = blocks(message.get('content'), claude=True)
            result['message'] = {'role': kind, 'content': content}
            if 'model' in message: result['message']['model'] = message['model']
            if omitted: result[MARKER] = 'Native reasoning omitted from view-only workload text.'
        elif kind == 'custom-title':
            if not isinstance(record.get('customTitle'), str): refused()
            result['customTitle'] = record['customTitle']
        elif kind not in CLAUDE_META:
            refused()
        if kind == 'system' and record.get('subtype') not in (None, 'stop_hook_summary', 'turn_duration'):
            refused()
        return result

    def project(self, records):
        budget = StructureBudget(self.bounds)
        budget.check(records)
        if self.provider == 'codex' and (not records or records[0].get('type') != 'session_meta'):
            refused()
        if self.provider == 'codex':
            header = records[0].get('payload')
            if not isinstance(header, dict): refused()
            if any(row.get('type') == 'session_meta' for row in records[1:]): refused()
            if header.get('history_mode') == 'paginated':
                # Same no-base ordinal boundary as csl_codex_history._node,
                # without resolving another file or a harness-home index.
                if any(type(row.get('ordinal')) is not int or row['ordinal'] != index
                       for index, row in enumerate(records)):
                    raise SyncError('workload_codex_ordinal_refused')
        for record in records:
            payload = record.get('payload', {})
            if (any(name in record for name in PROVENANCE_FIELDS)
                    or isinstance(payload, dict) and any(name in payload for name in PROVENANCE_FIELDS)):
                raise SyncError('workload_text_provenance_refused')
        selected_records = [self._codex(record, budget) if self.provider == 'codex'
                            else self._claude(record) for record in records]
        clean = self.redactor.clean(selected_records)
        output, view, tools, view_bytes = bytearray(), [], {}, 0
        sid = title = created = updated = model = None
        main_ids, auxiliary_parents, last_main = set(), {}, None
        for record in clean:
            if self.provider == 'codex':
                payload = record['payload']
                candidate = payload.get('id') if record['type'] == 'session_meta' else None
                if record['type'] == 'turn_context': model = payload.get('model', model)
                if payload.get('type') == 'thread_name_updated': title = payload['thread_name']
                if payload.get('type') == 'function_call':
                    payload['arguments'] = encoded(payload['arguments']).decode()
            else:
                candidate = record.get('sessionId', record.get('session_id'))
                if record.get('sessionId', candidate) != candidate or record.get('session_id', candidate) != candidate: refused()
                for field in ('uuid', 'parentUuid'):
                    if record.get(field) is not None: identifier(record[field])
                uuid, parent = record.get('uuid'), record.get('parentUuid')
                if record['type'] in ('user', 'assistant'):
                    visited = set()
                    while parent in auxiliary_parents:
                        if parent in visited: raise SyncError('workload_claude_branch_refused')
                        visited.add(parent); parent = auxiliary_parents[parent]
                    if uuid in main_ids or uuid in auxiliary_parents or parent != last_main:
                        raise SyncError('workload_claude_branch_refused')
                    main_ids.add(uuid); last_main = uuid
                elif uuid is not None:
                    if uuid in main_ids or uuid in auxiliary_parents and auxiliary_parents[uuid] != parent:
                        raise SyncError('workload_claude_branch_refused')
                    auxiliary_parents[uuid] = parent
                model = record.get('message', {}).get('model', model)
                if record['type'] == 'custom-title': title = record['customTitle']
            if candidate is not None:
                identifier(candidate)
                if sid is not None and sid != candidate: refused()
                sid = candidate
            stamp = record['timestamp']; created = created or stamp; updated = stamp or updated
            record[PROFILE_MARKER] = self.profile
            offset = len(output); line = encoded(record)+b'\n'
            if len(line) > self.bounds.record_bytes or len(output)+len(line) > self.bounds.output_bytes:
                raise SyncError('workload_output_limit')
            output.extend(line)
            # Compaction is a fixed omission notice, never the raw renderer's
            # promise that a native checkpoint was retained.
            render = () if record['type'] == 'compacted' else _view(
                self.provider, None, 16000, records=[(record, offset)], tools=tools)
            notices = []
            if MARKER in record:
                notices.append({'time': stamp, 'role': 'system', 'kind': 'notice', 'text': record[MARKER],
                    'tool': None, 'call_id': None, 'truncated': False,
                    'raw_offset': {'file_key': 'main', 'offset': offset}, 'native_pointer': None})
            for row in chain(render, notices):
                if row['kind'] not in ('text', 'tool_call', 'tool_result', 'notice'): refused()
                row = {'seq': len(view)+1, **row}
                if title is None and row['role'] == 'user' and row['kind'] == 'text': title = row['text'][:200]
                view_bytes += len(encoded(row))
                if len(view) >= self.bounds.records or view_bytes > self.bounds.output_bytes:
                    raise SyncError('workload_view_limit')
                view.append(row)
        identifier(sid)
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', sid): refused()
        if model is not None and not isinstance(model, str): refused()
        metadata = {'provider': self.provider, 'native_id': sid, 'title': (title or sid)[:200],
            'cwd': None, 'repository': None, 'created_at': created, 'updated_at': updated, 'model': model,
            'record_count': len(clean), 'byte_count': len(output), 'parent': None, 'origin': 'native', 'state': 'idle'}
        return metadata, bytes(output), view
