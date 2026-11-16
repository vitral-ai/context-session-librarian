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
import csl_antigravity
import csl_codex_history

PROVIDERS = ('codex', 'claude', 'antigravity')
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


def _lines(path, max_bytes=None, start_offset=0):
    """Fixed opening length; never parse a concurrently growing partial tail."""
    try:
        with path.open('rb') as source:
            left = os.fstat(source.fileno()).st_size
            if max_bytes is not None:left=min(left,max_bytes)
            if not 0 <= start_offset <= left:raise ProviderError('invalid_view_position')
            source.seek(start_offset);left-=start_offset
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


def _paths(provider, root):
    if provider == 'antigravity':
        return sorted((root/'conversations').glob('*.db'))
    if provider == 'codex':
        return csl_codex_history.current_paths(root,[*root.glob('sessions/**/*.jsonl'), *root.glob('archived_sessions/**/*.jsonl')])
    if provider == 'claude':
        return sorted((root/'projects').glob('*/*.jsonl'))
    raise ProviderError('unsupported_provider')


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


def _metadata(provider, path, root, active_seconds, files=None, *, evidence=None):
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
        ids=csl_codex_history.identity(path)
        match=re.search(r'([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$',path.stem)
        if ids:sid=ids[0]
        elif match:sid=match.group(1)
    origin, count = 'native', 0
    for record, _ in _lines(path):
        count += 1
        stamp = _stamp(record.get('timestamp'))
        created = created or stamp; updated = stamp or updated
        if provider == 'codex':
            payload = record.get('payload', {})
            if not isinstance(payload, dict):
                raise ProviderError('invalid_codex_payload')
            if record.get('type') == 'session_meta':
                if evidence is not None and payload.get('history_base'): evidence['external_history_base'] = True
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
    files = _files(provider, path, root) if files is None else files
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
    if provider=='codex':
        try:
            if csl_codex_history.has_base(path):
                return csl_codex_history.inventory(csl_codex_history.lineage(root,path))
        except csl_codex_history.HistoryError as error:
            raise ProviderError(str(error)) from None
    if provider == 'antigravity':
        try:return csl_antigravity.inventory(root,path.stem)
        except (csl_antigravity.FormatError,OSError) as error:
            raise ProviderError(str(error) if isinstance(error,csl_antigravity.FormatError) else 'source_unavailable') from None
    suffix = path.suffix
    result = [_descriptor(path, root, 'session'+suffix, 'transcript')]
    companion = path.with_suffix('') if provider == 'claude' else None
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
        if provider in ('claude','antigravity'):
            sid = path.stem
        else:
            ids=csl_codex_history.identity(path)
            match=re.search(r'([0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$',path.stem)
            sid=ids[0] if ids else match.group(1) if match else next((r.get('payload', {}).get('id') for r, _ in _lines(path) if r.get('type') == 'session_meta'), None)
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
    raise ProviderError('unsupported_provider')


def _view(provider, path, text_limit, max_bytes=None, records=None, tools=None, *, start_offset=0):
    records = records if records is not None else _lines(path,max_bytes,start_offset)
    tools={} if tools is None else tools
    for record, offset in records:
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
            elif kind=='tool_use':
                b=block; kind='tool_call';tool=b.get('name');call=b.get('id');text=_text(b.get('input'))
                if call and tool:tools[call]=tool
            elif kind=='tool_result':
                b=block;kind='tool_result';tool=b.get('name');call=b.get('tool_use_id',b.get('id'));text=_text(b.get('content'))
                tool=tool or tools.get(call)
            elif kind in ('image','input_image'):
                data=block.get('source',{});mime=data.get('media_type','image') if isinstance(data,dict) else 'image'
                kind='document' if mime=='application/pdf' else 'image';text='Embedded '+kind+' reference; see raw record.'
            elif kind=='document':
                kind='document';text='Document reference; see raw record.'
            else:
                kind='notice';text='Unsupported native block retained in raw record.'
            yield {'time':_stamp(record.get('timestamp')), 'role':'tool' if kind=='tool_result' else role if role in ('user','assistant','system','tool') else 'system',
                   'kind':kind,'text':text[:text_limit],'tool':tool,'call_id':call,
                   'truncated':len(text)>text_limit,'raw_offset':{'file_key':'main','offset':offset},
                   'native_pointer':None,
                   'block_index':index}


def view_records(provider, native_id, *, position=None, limit=500, text_limit=16000, home=None):
    if type(limit) is not int or not 1 <= limit <= 1000 or type(text_limit) is not int or not 1 <= text_limit <= 64000:
        raise ProviderError('invalid_view_limit')
    root, path = _resolve(provider,native_id,home)
    if provider=='antigravity':
        return _antigravity_view(root,native_id,position,limit,text_limit)
    if provider=='codex':
        try:
            if csl_codex_history.has_base(path):
                nodes=csl_codex_history.lineage(root,path)
                return _codex_lineage_view(nodes,native_id,position,limit,text_limit)
        except csl_codex_history.HistoryError as error:
            raise ProviderError(str(error)) from None
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
    nodes=None
    if provider=='codex':
        try:
            if csl_codex_history.has_base(path):nodes=csl_codex_history.lineage(root,path)
        except csl_codex_history.HistoryError as error:
            return {'available':False,'reason':str(error),'descriptor_version':1,'manifest':None,'members':[]}
    files=csl_codex_history.inventory(nodes) if nodes else _files(provider,path,root)
    meta=_metadata(provider,path,root,300,files=files)
    if provider=='antigravity':
        try:
            with csl_antigravity.snapshot(root,native_id) as (db,files):
                reason=csl_antigravity.package_reason(db)
            if reason:return {'available':False,'reason':reason,'descriptor_version':1,'manifest':None,'members':[]}
        except csl_antigravity.FormatError as error:
            raise ProviderError(str(error)) from None
    elif not any(event['kind'] in ('text','tool_call','tool_result','image','document') for event in (_codex_lineage_events(nodes,16000) if nodes else _view(provider,path,16000,files[0]['complete_length']))):
        return {'available':False,'reason':'no_conversation','descriptor_version':1,'manifest':None,'members':[]}
    return _package_mapping(provider, native_id, path.suffix, meta, files, nodes=nodes)


def _package_mapping(provider, native_id, suffix, meta, files, *, nodes=None):
    members=[];entries=[]
    for f in files:
        member=f'sessions/{native_id}/'+('main'+suffix if f['file_key']=='main' else 'files/'+f['relative_path'])
        members.append({'file_key':f['file_key'],'member':member})
        entries.append({'file_key':f['file_key'],'file':member,'relative_path':f['relative_path'],'role':f['role']})
    entry={'id':native_id,'provider':provider,'title':meta['title'],'cwd':meta['cwd'],'primary':members[0]['member'],'files':entries}
    if nodes:
        csl_codex_history.verify(nodes)
        entry['codex_history']=csl_codex_history.graph_manifest(nodes)
    return {'available':True,'reason':None,'descriptor_version':1,
            'manifest':{'format':'codex-session-librarian','version':3,'mode':'raw','created_at':meta['updated_at'] or meta['created_at'] or '1970-01-01T00:00:00+00:00','sessions':[entry]},
            'members':members}


def _codex_lineage_events(nodes,text_limit):
    tools={}
    for node in nodes:
        records=((r,start) for r,start,_,_ in csl_codex_history.lines(node.path,node.length))
        for event in _view('codex',node.path,text_limit,records=records,tools=tools):
            offset=event['raw_offset']['offset']
            event['raw_offset']['file_key']=node.descriptor['file_key']
            event['inherited']=node is not nodes[-1]
            if str(node.path).endswith('.zst'):
                event['raw_offset']['offset']=0
                event['native_pointer']='/decoded_jsonl/'+str(offset)
            yield event


def _codex_lineage_view(nodes,native_id,position,limit,text_limit):
    if position is not None and (not isinstance(position,dict) or
            position.get('version')!=1 or position.get('provider')!='codex' or
            position.get('native_id')!=native_id or type(position.get('next_seq')) is not int or
            position['next_seq']<1 or position.get('text_limit')!=text_limit):
        raise ProviderError('invalid_view_position')
    target=position['next_seq'] if position else 1
    page=[];seq=1;digest=hashlib.sha256();verified=position is None
    def result(reset=False,complete=False):
        csl_codex_history.verify(nodes)
        anchors=[{'file_key':f['file_key'],'length':f['complete_length'],'sha256':f['sha256']} for f in csl_codex_history.inventory(nodes)]
        return {'records':[] if reset else page,'reset_required':reset,
                'reason':'view_prefix_changed' if reset else None,'complete':False if reset else complete,
                'position':{'version':1,'provider':'codex','native_id':native_id,
                            'next_seq':1 if reset else seq,'text_limit':text_limit,
                            'prefix_sha256':hashlib.sha256().hexdigest() if reset else digest.hexdigest()},
                'source_file':anchors[0],'source_files':anchors}
    for event in _codex_lineage_events(nodes,text_limit):
        if seq==target:
            if position is not None and digest.hexdigest()!=position.get('prefix_sha256'):
                return result(reset=True)
            verified=True
        if seq>=target and len(page)>=limit:return result()
        row={'seq':seq,**event}
        if seq>=target:page.append(row)
        digest.update(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()+b'\n');seq+=1
    if not verified and seq==target and digest.hexdigest()==position.get('prefix_sha256'):verified=True
    return result(reset=not verified,complete=True)


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
