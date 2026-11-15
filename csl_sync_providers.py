"""Local read-only session adapters for collectors and readers (interface v1)."""
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import stat
import subprocess
from urllib.parse import urlsplit, urlunsplit

from csl_canonical import strict_json
import csl_gemini
import csl_antigravity

PROVIDERS = ('codex', 'claude', 'gemini', 'antigravity')
API_VERSION = 1


class ProviderError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__('Provider error: ' + code)


def provider_home(provider, home=None):
    if provider not in PROVIDERS:
        raise ProviderError('unsupported_provider')
    default = {'codex': os.environ.get('CODEX_HOME', '~/.codex'),
               'claude': os.environ.get('CLAUDE_CONFIG_DIR', '~/.claude'),
               'gemini': os.environ.get('CSL_GEMINI_HOME') or str(Path(os.environ.get('GEMINI_CLI_HOME',str(Path.home()))) / ('.cache/.gemini' if os.environ.get('SANDBOX')=='sandbox-exec' else '.gemini')),
               'antigravity': os.environ.get('CSL_ANTIGRAVITY_HOME', '~/.gemini/antigravity-cli')}[provider]
    return Path(home if home is not None else default).expanduser().resolve()


def _safe(path, root):
    try:
        relative = path.relative_to(root)
        current = root
        for part in relative.parts:
            current = current / part
            if current.is_symlink():
                raise ProviderError('symlink_source')
        if not path.resolve().is_relative_to(root) or not stat.S_ISREG(path.stat().st_mode):
            raise ProviderError('unsafe_source')
    except (OSError, ValueError):
        raise ProviderError('source_unavailable') from None
    return path


def _lines(path, max_bytes=None):
    """Fixed opening length; never parse a concurrently growing partial tail."""
    try:
        with path.open('rb') as source:
            left = os.fstat(source.fileno()).st_size
            if max_bytes is not None:left=min(left,max_bytes)
            while left:
                offset = source.tell()
                line = source.readline(left)
                left -= len(line)
                if not line.endswith(b'\n'):
                    break
                if not line.strip():
                    continue
                try:
                    record = strict_json(line)
                except (ValueError, UnicodeError):
                    raise ProviderError('invalid_complete_record') from None
                if not isinstance(record, dict):
                    raise ProviderError('invalid_complete_record')
                yield record, offset
    except OSError:
        raise ProviderError('source_unavailable') from None


def _json(path):
    try:
        with path.open('rb') as stream:
            before = os.fstat(stream.fileno())
            body = stream.read(before.st_size)
            after = os.fstat(stream.fileno())
        if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            raise ProviderError('source_changing')
        record = strict_json(body)
        if not isinstance(record, dict):
            raise ValueError()
        return record
    except (ValueError, UnicodeError):
        raise ProviderError('incomplete_json_snapshot') from None
    except OSError:
        raise ProviderError('source_unavailable') from None


def _paths(provider, root):
    if provider == 'antigravity':
        return sorted((root/'conversations').glob('*.db'))
    if provider == 'codex':
        return sorted([*root.glob('sessions/**/*.jsonl'), *root.glob('archived_sessions/**/*.jsonl')])
    if provider == 'claude':
        return sorted((root/'projects').glob('*/*.jsonl'))
    journals = list(root.glob('tmp/*/chats/session-*.jsonl'))
    # Native migration can leave the old JSON next to its authoritative journal.
    return sorted(journals + [p for p in root.glob('tmp/*/chats/session-*.json') if p.with_suffix('.jsonl') not in journals])


def _gemini(path, max_bytes=None):
    try:
        if path.suffix == '.json':
            record = _json(path)
            return record, [(r, 0) for r in record.get('messages', [])]
        return csl_gemini.replay(_lines(path,max_bytes))
    except ValueError as e:
        if isinstance(e, ProviderError):
            raise
        raise ProviderError('invalid_gemini_journal') from None


def _stamp(value):
    try:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return datetime.fromtimestamp(value / (1000 if value > 10**11 else 1), timezone.utc).isoformat()
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, AttributeError, OverflowError):
        pass
    return None


def _text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return '\n'.join(_text(v) for v in value)
    if isinstance(value, dict):
        if isinstance(value.get('text'), str):
            return value['text']
        return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    return '' if value is None else str(value)


def _repository(cwd):
    if not isinstance(cwd, str) or not Path(cwd).is_dir():
        return None
    def git(*args):
        try:
            r = subprocess.run(['git', '-C', cwd, *args], capture_output=True, text=True, timeout=2)
            return r.stdout.strip() if r.returncode == 0 else None
        except (OSError, subprocess.TimeoutExpired):
            return None
    if git('rev-parse', '--is-inside-work-tree') != 'true':
        return None
    remote = git('config', '--get', 'remote.origin.url')
    if remote and '://' in remote:
        try:
            parts = urlsplit(remote)
            host = parts.hostname or ''
            if parts.port:host += ':' + str(parts.port)
            remote = urlunsplit((parts.scheme, host, parts.path, '', ''))
        except ValueError:remote=None
    return {'remote_url': remote, 'branch': git('symbolic-ref', '--short', 'HEAD')}


def _metadata(provider, path, root, active_seconds):
    if provider == 'antigravity':
        try:
            with csl_antigravity.snapshot(root,path.stem) as (db,files):
                row=csl_antigravity.metadata(db,path.stem)
            modified=max(Path(f['path']).stat().st_mtime for f in files)
            return {**row,'repository':_repository(row['cwd']),
                    'byte_count':sum(f['complete_length'] for f in files),
                    'state':'active' if datetime.now(timezone.utc).timestamp()-modified<=active_seconds else 'idle'}
        except (csl_antigravity.FormatError,OSError) as error:
            raise ProviderError(str(error) if isinstance(error,csl_antigravity.FormatError) else 'source_unavailable') from None
    sid = title = cwd = created = updated = model = parent = None
    if provider=='claude':sid=path.stem
    if provider=='codex':
        match=re.search(r'([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$',path.stem)
        if match:sid=match.group(1)
    origin, count = 'native', 0
    if provider == 'gemini':
        meta, records = _gemini(path)
        sid = meta.get('sessionId'); title = meta.get('summary')
        if meta.get('csl_source_session_id'):
            parent = meta['csl_source_session_id']; origin = 'imported'
        created = _stamp(meta.get('startTime')); updated = _stamp(meta.get('lastUpdated'))
        dirs = meta.get('directories', [])
        cwd = meta.get('cwd') or (dirs[0] if dirs and isinstance(dirs[0], str) else None)
        if not cwd:
            marker=path.parent.parent/'.project_root'
            if marker.is_file():
                _safe(marker,root)
                if marker.stat().st_size<=16384:cwd=marker.read_text(encoding='utf-8').strip() or None
        if not cwd and (root/'projects.json').is_file():
            _safe(root/'projects.json',root)
            registry=_json(root/'projects.json').get('projects',{})
            if isinstance(registry,dict):cwd=next((k for k,v in registry.items() if v==path.parent.parent.name),None)
        for record, _ in records:
            count += 1
            if not title and record.get('type') == 'user':
                title = _text(record.get('content'))[:200]
            model = record.get('model') or model
    else:
        for record, _ in _lines(path):
            count += 1
            stamp = _stamp(record.get('timestamp'))
            created = created or stamp; updated = stamp or updated
            if provider == 'codex':
                payload = record.get('payload', {})
                if not isinstance(payload, dict):
                    raise ProviderError('invalid_codex_payload')
                if record.get('type') == 'session_meta':
                    sid = sid or payload.get('id') or payload.get('session_id')
                    cwd = payload.get('cwd') or cwd
                    parent = payload.get('forked_from_id') or parent
                if record.get('type') == 'turn_context':
                    model = payload.get('model') or model
                if record.get('type') == 'event_msg' and payload.get('type') == 'thread_name_updated':
                    title = payload.get('thread_name') or title
                if not title and payload.get('type') == 'message' and payload.get('role') == 'user':
                    title = _text(payload.get('content'))[:200]
                source = record.get('csl_source_session_id') or payload.get('csl_source_session_id')
            else:
                candidate = record.get('sessionId') or record.get('session_id')
                sid = sid or candidate; cwd = record.get('cwd') or cwd
                if record.get('type') == 'custom-title':
                    title = record.get('customTitle') or title
                message = record.get('message', {})
                if isinstance(message, dict):
                    model = message.get('model') or model
                    if not title and record.get('type') == 'user':
                        title = _text(message.get('content'))[:200]
                source = record.get('csl_source_session_id')
                fork = record.get('forkedFrom')
                if isinstance(fork, dict):
                    parent = fork.get('sessionId') or fork.get('session_id') or parent
                elif isinstance(fork, str):
                    parent = fork
            if source:
                parent = source; origin = 'imported'
            if record.get('csl_conversion') or record.get('csl_source_provider'):
                origin = 'converted'
    if not isinstance(sid, str) or not sid:
        raise ProviderError('missing_native_id')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',sid):raise ProviderError('invalid_native_id')
    if any(v is not None and not isinstance(v,str) for v in (title,cwd,model,parent)):
        raise ProviderError('invalid_metadata_field')
    files = _files(provider, path, root)
    modified = max((Path(f['path']).stat().st_mtime for f in files), default=path.stat().st_mtime)
    state = 'archived' if 'archived_sessions' in path.relative_to(root).parts else (
        'active' if datetime.now(timezone.utc).timestamp() - modified <= active_seconds else 'idle')
    return {'provider': provider, 'native_id': sid, 'title': (title or sid)[:200], 'cwd': cwd,
            'repository': _repository(cwd), 'created_at': created, 'updated_at': updated,
            'model': model, 'record_count': count,
            'byte_count': sum(f['complete_length'] for f in files),
            'parent': {'provider': provider, 'native_id': parent} if isinstance(parent, str) else None,
            'origin': origin, 'state': state}


def _descriptor(path, root, relative, role):
    _safe(path, root)
    digest = hashlib.sha256(); complete = 0
    with path.open('rb') as stream:
        size = os.fstat(stream.fileno()).st_size
        if path.suffix == '.jsonl':
            # Validate complete records first, preserving original raw whitespace.
            for _ in _lines(path):
                pass
            while stream.tell() < size:
                line = stream.readline(size-stream.tell())
                if not line.endswith(b'\n'):
                    break
                digest.update(line); complete += len(line)
        else:
            if path.suffix == '.json' and role in ('transcript', 'subagent'):
                _json(path)
            while complete < size:
                chunk = stream.read(min(1024*1024, size-complete))
                if not chunk:
                    raise ProviderError('source_changing')
                digest.update(chunk); complete += len(chunk)
        stream.seek(0);check=hashlib.sha256();left=complete
        while left:
            chunk=stream.read(min(left,1024*1024))
            if not chunk:raise ProviderError('source_changing')
            check.update(chunk);left-=len(chunk)
        if check.hexdigest()!=digest.hexdigest():raise ProviderError('source_changing')
    return {'file_key': 'main' if role == 'transcript' else 'f-' + hashlib.sha256(relative.encode()).hexdigest(),
            'path': str(path), 'relative_path': relative,
            'mode': 'append' if path.suffix == '.jsonl' else 'rewrite',
            'complete_length': complete, 'size': size, 'sha256': digest.hexdigest(), 'role': role}


def _files(provider, path, root):
    if provider == 'antigravity':
        try:return csl_antigravity.inventory(root,path.stem)
        except (csl_antigravity.FormatError,OSError) as error:
            raise ProviderError(str(error) if isinstance(error,csl_antigravity.FormatError) else 'source_unavailable') from None
    suffix = path.suffix
    result = [_descriptor(path, root, 'session'+suffix, 'transcript')]
    companion = path.with_suffix('') if provider == 'claude' else path.parent/path.stem if provider == 'gemini' else None
    if provider == 'gemini':
        meta, _ = _gemini(path)
        sid=meta.get('sessionId')
        if not isinstance(sid,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',sid):
            raise ProviderError('invalid_native_id')
        companion = path.parent / sid
    if companion and companion.is_dir():
        for child in sorted(companion.rglob('*')):
            if child.is_symlink():
                raise ProviderError('symlink_source')
            if not child.is_file():
                continue
            rel = child.relative_to(companion)
            if provider == 'claude' and rel.parts[0] not in ('subagents', 'tool-results'):
                continue
            role = ('subagent' if child.suffix=='.jsonl' else 'context') if rel.parts[0]=='subagents' else (
                'tool_result' if rel.parts[0]=='tool-results' else 'subagent' if child.suffix=='.jsonl' else 'context')
            result.append(_descriptor(child, root, 'session/'+rel.as_posix(), role))
    return result


def _resolve(provider, native_id, home):
    root = provider_home(provider, home)
    matches = []
    for path in _paths(provider, root):
        _safe(path, root)
        if provider == 'gemini':
            sid = _gemini(path)[0].get('sessionId')
        elif provider in ('claude','antigravity'):
            sid = path.stem
        else:
            match=re.search(r'([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$',path.stem)
            sid=match.group(1) if match else next((r.get('payload', {}).get('id') for r, _ in _lines(path) if r.get('type') == 'session_meta'), None)
        if sid == native_id:
            matches.append(path)
    if len(matches) != 1:
        raise ProviderError('session_not_found' if not matches else 'ambiguous_native_id')
    return root, matches[0]


def list_sessions(provider, *, home=None, active_seconds=300):
    root = provider_home(provider, home)
    if not isinstance(active_seconds, (int, float)) or active_seconds < 0:
        raise ProviderError('invalid_active_window')
    result = [_metadata(provider, _safe(path, root), root, active_seconds) for path in _paths(provider, root)]
    ids = [r['native_id'] for r in result]
    if len(ids) != len(set(ids)):
        raise ProviderError('ambiguous_native_id')
    return sorted(result, key=lambda r: (r['updated_at'] or '', r['native_id']), reverse=True)


def raw_files(provider, native_id, *, home=None):
    root, path = _resolve(provider, native_id, home)
    return _files(provider, path, root)


def _blocks(provider, record):
    if provider == 'codex':
        p = record.get('payload', {})
        if record.get('type') == 'response_item' and isinstance(p, dict):
            kind = p.get('type')
            if kind == 'message':
                return p.get('role', 'system'), p.get('content', [])
            if kind in ('function_call', 'custom_tool_call'):
                return 'assistant', [{'type':'tool_use','name':p.get('name'),'id':p.get('call_id'),'input':p.get('arguments',p.get('input'))}]
            if kind in ('function_call_output', 'custom_tool_call_output'):
                return 'tool', [{'type':'tool_result','tool_use_id':p.get('call_id'),'content':p.get('output')}]
        if record.get('type') == 'compacted':
            return 'system', [{'type':'notice','text':'Native compaction checkpoint; raw retains original state.'}]
        return None, []
    if provider == 'claude':
        if record.get('type') not in ('user','assistant'):
            return None, []
        m=record.get('message', {})
        if not isinstance(m, dict):
            raise ProviderError('invalid_claude_message')
        return m.get('role',record['type']), m.get('content', [])
    role = {'gemini':'assistant','user':'user'}.get(record.get('type'),'system')
    content=record.get('content', [])
    blocks=[{'type':'text','text':content}] if isinstance(content,str) else [content] if isinstance(content,dict) else list(content or [])
    for tool in record.get('toolCalls', []):
        blocks.append({'type':'tool_use','name':tool.get('name'),'id':tool.get('id'),'input':tool.get('args')})
        if 'result' in tool and tool['result'] is not None:
            blocks.append({'type':'tool_result','tool_use_id':tool.get('id'),'name':tool.get('name'),'content':tool['result']})
    return role, blocks


def _view(provider, path, text_limit, max_bytes=None):
    records = _gemini(path,max_bytes)[1] if provider == 'gemini' else _lines(path,max_bytes)
    tools={}
    for record_index, (record, offset) in enumerate(records):
        role, blocks = _blocks(provider, record)
        if role is None:
            continue
        if isinstance(blocks, str):
            blocks=[{'type':'text','text':blocks}]
        if not isinstance(blocks,list):
            raise ProviderError('invalid_message_blocks')
        for index, block in enumerate(blocks):
            if not isinstance(block,dict):
                raise ProviderError('invalid_message_block')
            kind=block.get('type'); tool=call=None
            if kind in ('thinking','redacted_thinking'):
                continue
            if kind in ('text','input_text','output_text') or 'text' in block:
                kind='text'; text=_text(block.get('text'))
            elif kind=='tool_use' or 'functionCall' in block:
                b=block.get('functionCall',block); kind='tool_call';tool=b.get('name');call=b.get('id');text=_text(b.get('input',b.get('args')))
                if call and tool:tools[call]=tool
            elif kind=='tool_result' or 'functionResponse' in block:
                b=block.get('functionResponse',block);kind='tool_result';tool=b.get('name');call=b.get('tool_use_id',b.get('id'));text=_text(b.get('content',b.get('response')))
                tool=tool or tools.get(call)
            elif kind in ('image','input_image') or 'inlineData' in block:
                data=block.get('inlineData',block.get('source',{}));mime=data.get('mimeType',data.get('media_type','image')) if isinstance(data,dict) else 'image'
                kind='document' if mime=='application/pdf' else 'image';text='Embedded '+kind+' reference; see raw record.'
            elif kind=='document' or 'fileData' in block:
                kind='document';text='Document reference; see raw record.'
            else:
                kind='notice';text='Unsupported native block retained in raw record.'
            native_offset=offset
            if provider=='gemini' and path.suffix=='.jsonl':
                if kind=='tool_result':native_offset=record.get('_csl_tool_result_offsets',{}).get(call,offset)
                elif kind not in ('tool_call',):native_offset=record.get('_csl_content_offset',offset)
            yield {'time':_stamp(record.get('timestamp')), 'role':'tool' if kind=='tool_result' else role if role in ('user','assistant','system','tool') else 'system',
                   'kind':kind,'text':text[:text_limit],'tool':tool,'call_id':call,
                   'truncated':len(text)>text_limit,'raw_offset':{'file_key':'main','offset':native_offset},
                   'native_pointer':f'/messages/{record_index}' if provider=='gemini' and path.suffix=='.json' else None,
                   'block_index':index}


def view_records(provider, native_id, *, position=None, limit=500, text_limit=16000, home=None):
    if type(limit) is not int or not 1 <= limit <= 1000 or type(text_limit) is not int or not 1 <= text_limit <= 64000:
        raise ProviderError('invalid_view_limit')
    root, path = _resolve(provider,native_id,home)
    if provider=='antigravity':
        return _antigravity_view(root,native_id,position,limit,text_limit)
    primary=_descriptor(path,root,'session'+path.suffix,'transcript')
    seq=1; digest=hashlib.sha256();page=[]
    if position is not None and not isinstance(position,dict):
        raise ProviderError('invalid_view_position')
    target=1 if position is None else position.get('next_seq')
    if position is not None and (position.get('version')!=1 or position.get('provider')!=provider or position.get('native_id')!=native_id or type(target) is not int or target<1 or position.get('text_limit')!=text_limit):
        raise ProviderError('invalid_view_position')
    def result(reset=False,complete=False):
        check=hashlib.sha256()
        with path.open('rb') as stream:
            left=primary['complete_length']
            while left:
                chunk=stream.read(min(left,1024*1024))
                if not chunk:raise ProviderError('source_changing')
                check.update(chunk);left-=len(chunk)
        if check.hexdigest()!=primary['sha256']:raise ProviderError('source_changing')
        return {'records':[] if reset else page,'position':{'version':1,'provider':provider,'native_id':native_id,'next_seq':1 if reset else seq,'prefix_sha256':hashlib.sha256().hexdigest() if reset else digest.hexdigest(),'text_limit':text_limit},
                'reset_required':reset,'reason':'view_prefix_changed' if reset else None,'complete':False if reset else complete,
                'source_file':{'file_key':'main','length':primary['complete_length'],'sha256':primary['sha256']}}
    verified=position is None
    for event in _view(provider,path,text_limit,primary['complete_length']):
        if seq==target:
            if position is not None and digest.hexdigest()!=position.get('prefix_sha256'):
                return result(True)
            verified=True
        if seq>=target and len(page)>=limit:
            return result(complete=False)
        row={'seq':seq,**event}
        if seq>=target:page.append(row)
        digest.update(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()+b'\n');seq+=1
    if not verified and seq==target and digest.hexdigest()==position.get('prefix_sha256'):
        verified=True
    return result(reset=not verified,complete=True)


def package_descriptor(provider, native_id, *, home=None):
    root,path=_resolve(provider,native_id,home)
    meta=_metadata(provider,path,root,300);files=_files(provider,path,root)
    if provider=='antigravity':
        try:
            with csl_antigravity.snapshot(root,native_id) as (db,files):
                reason=csl_antigravity.package_reason(db)
            if reason:return {'available':False,'reason':reason,'descriptor_version':1,'manifest':None,'members':[]}
        except csl_antigravity.FormatError as error:
            raise ProviderError(str(error)) from None
    elif not any(event['kind'] in ('text','tool_call','tool_result','image','document') for event in _view(provider,path,16000,files[0]['complete_length'])):
        return {'available':False,'reason':'no_conversation','descriptor_version':1,'manifest':None,'members':[]}
    if provider=='codex' and any(r.get('type')=='session_meta' and isinstance(r.get('payload'),dict) and r['payload'].get('history_base') for r,_ in _lines(path,files[0]['complete_length'])):
        return {'available':False,'reason':'external_history_base_not_packaged','descriptor_version':1,'manifest':None,'members':[]}
    members=[];entries=[]
    for f in files:
        member=f'sessions/{native_id}/'+('main'+path.suffix if f['file_key']=='main' else 'files/'+f['relative_path'])
        members.append({'file_key':f['file_key'],'member':member})
        entries.append({'file_key':f['file_key'],'file':member,'relative_path':f['relative_path'],'role':f['role']})
    entry={'id':native_id,'provider':provider,'title':meta['title'],'cwd':meta['cwd'],'primary':members[0]['member'],'files':entries}
    return {'available':True,'reason':None,'descriptor_version':1,
            'manifest':{'format':'codex-session-librarian','version':3,'mode':'raw','created_at':meta['updated_at'] or meta['created_at'] or '1970-01-01T00:00:00+00:00','sessions':[entry]},
            'members':members}


def _antigravity_view(root,native_id,position,limit,text_limit):
    if position is not None and (not isinstance(position,dict) or
            position.get('version')!=1 or position.get('provider')!='antigravity' or
            position.get('native_id')!=native_id or type(position.get('next_seq')) is not int or
            position['next_seq']<1 or position.get('text_limit')!=text_limit):
        raise ProviderError('invalid_view_position')
    target=position['next_seq'] if position else 1
    page=[];seq=1;digest=hashlib.sha256();verified=position is None
    def result(files,reset=False,complete=False):
        anchors=[{'file_key':f['file_key'],'length':f['complete_length'],'sha256':f['sha256']} for f in files]
        return {'records':[] if reset else page,'reset_required':reset,
                'reason':'view_prefix_changed' if reset else None,'complete':False if reset else complete,
                'position':{'version':1,'provider':'antigravity','native_id':native_id,
                            'next_seq':1 if reset else seq,'text_limit':text_limit,
                            'prefix_sha256':hashlib.sha256().hexdigest() if reset else digest.hexdigest()},
                'source_file':anchors[0],'source_files':anchors}
    try:
        with csl_antigravity.snapshot(root,native_id) as (db,files):
            for event in csl_antigravity.views(db,text_limit):
                if seq==target:
                    if position is not None and digest.hexdigest()!=position.get('prefix_sha256'):
                        return result(files,reset=True)
                    verified=True
                if seq>=target and len(page)>=limit:
                    return result(files)
                row={'seq':seq,**event}
                if seq>=target:page.append(row)
                digest.update(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()+b'\n')
                seq+=1
            if not verified and seq==target and digest.hexdigest()==position.get('prefix_sha256'):
                verified=True
            return result(files,reset=not verified,complete=True)
    except (csl_antigravity.FormatError,OSError) as error:
        raise ProviderError(str(error) if isinstance(error,csl_antigravity.FormatError) else 'source_unavailable') from None
