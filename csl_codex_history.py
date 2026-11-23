"""Bounded native Codex 0.169.0 history_base lineage, without models or auth.

Reference: openai/codex rust-v0.169.0 (d32063f96c2dff1dc96af839b3952abb6acfeda4).
Rollout identity comes from the canonical basename, not necessarily metadata.id.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import uuid

from csl_canonical import strict_json
from csl_sync_fs import open_stream, reparse

MAX_DEPTH = 32
MAX_FILE = 2 * 1024**3
MAX_TOTAL = 4 * 1024**3
UUID = r'[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'
NAME = re.compile(r'^rollout-\d{4}-\d\d-\d\dT\d\d-\d\d-\d\d-(' + UUID + r')(?:_(' + UUID + r'))?\.jsonl(?:\.zst)?$')


class HistoryError(ValueError):
    """Stable diagnostics never include source text or local paths."""


def identity(path):
    match = NAME.fullmatch(Path(path).name)
    if not match:return None
    try:datetime.strptime(Path(path).name[8:27],'%Y-%m-%dT%H-%M-%S')
    except ValueError:return None
    return match[1].lower(),(match[2] or match[1]).lower()


def current_paths(root,paths):
    """Native filename fallback: stable owner, active first, newest basename."""
    grouped={};other=[]
    for path in paths:
        ids=identity(path)
        if not ids:other.append(path);continue
        group=grouped.setdefault(ids[0],[]);group.append(path)
    for group in grouped.values():
        active=[p for p in group if p.relative_to(root).parts[0]=='sessions']
        other.append(max(active or group,key=lambda p:p.name))
    return sorted(other)


def _uuid(value):
    if not isinstance(value, str) or not re.fullmatch(UUID, value):
        raise HistoryError('invalid_history_base_id')
    return str(uuid.UUID(value))


def reference(value):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {'thread_id','end_ordinal_exclusive','end_byte_offset'}:
        raise HistoryError('unsupported_history_base_reference')
    result = dict(value); result['thread_id'] = _uuid(value['thread_id'])
    for key in ('end_ordinal_exclusive','end_byte_offset'):
        if type(value[key]) is not int or not 0 < value[key] < 2**64:
            raise HistoryError('invalid_history_base_boundary')
    if result['end_byte_offset'] > MAX_FILE:
        raise HistoryError('history_base_size_limit')
    return result


def _safe(path, root):
    try:
        cursor = root
        for part in path.relative_to(root).parts:
            cursor = cursor / part
            if reparse(cursor):
                raise HistoryError('history_base_symlink')
        if not path.is_file():
            raise HistoryError('history_base_missing')
    except (OSError, ValueError) as error:
        if isinstance(error, HistoryError):
            raise
        raise HistoryError('history_base_unavailable') from None


@contextmanager
def decoded(path):
    """Optional zstd needs stdlib Python 3.14 or an existing zstd executable."""
    if not str(path).endswith('.zst'):
        with open_stream(path) as stream:
            yield stream
        return
    class Limited:
        def __init__(self,stream):self.stream=stream;self.count=0
        def _count(self,data):
            self.count+=len(data)
            if self.count>MAX_FILE:raise HistoryError('history_base_decoded_size_limit')
            return data
        def read(self,size=-1):return self._count(self.stream.read(size))
        def readline(self,size=-1):return self._count(self.stream.readline(size))
    try:
        from compression import zstd
    except ImportError:
        executable = shutil.which('zstd')
        if not executable:
            raise HistoryError('history_base_zstd_decoder_unavailable') from None
        with open_stream(path) as source:
            process = subprocess.Popen([executable, '-d', '-q', '-c'], stdin=source,
                                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            reader=Limited(process.stdout)
            yield reader
            # Verify the complete compressed stream, even past the inherited prefix.
            while reader.read(1024*1024):pass
            if process.wait(timeout=30):
                raise HistoryError('invalid_compressed_history_base')
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.terminate(); process.wait(timeout=10)
    else:
        try:
            with open_stream(path) as source, zstd.open(source, 'rb') as stream:
                reader=Limited(stream);yield reader
                while reader.read(1024*1024):pass
        except (EOFError, zstd.ZstdError):
            raise HistoryError('invalid_compressed_history_base') from None


def lines(path, length=None):
    """Decoded offsets refer to the exact native JSONL prefix."""
    offset = 0
    try:
        with decoded(path) as stream:
            while length is None or offset < length:
                limit = -1 if length is None else length - offset
                body = stream.readline(limit)
                if not body:
                    break
                start = offset; offset += len(body)
                if offset > MAX_FILE:
                    raise HistoryError('history_base_decoded_size_limit')
                if not body.endswith(b'\n'):
                    if length is not None:
                        raise HistoryError('history_base_incomplete_boundary')
                    break
                if not body.strip():
                    continue
                try:
                    record = strict_json(body)
                except (ValueError, UnicodeError):
                    raise HistoryError('invalid_history_base_record') from None
                if not isinstance(record,dict):
                    raise HistoryError('invalid_history_base_record')
                yield record,start,offset,body
            if length is not None and offset != length:
                raise HistoryError('history_base_offset_past_source')
    except OSError:
        raise HistoryError('history_base_unavailable') from None


def header(path):
    iterator = lines(path)
    try:
        record,_,end,_ = next(iterator)
        if record.get('type') != 'session_meta' or not isinstance(record.get('payload'),dict):
            raise HistoryError('history_base_metadata_missing')
        return record,end
    except StopIteration:
        raise HistoryError('history_base_metadata_missing') from None
    finally:
        iterator.close()


def has_base(path):
    record,_ = header(path)
    return record['payload'].get('history_base') is not None


def _hash(path,length):
    result=hashlib.sha256()
    with open_stream(path) as stream:
        while length:
            data=stream.read(min(length,1024*1024))
            if not data:raise HistoryError('history_base_source_changing')
            result.update(data);length-=len(data)
    return result.hexdigest()


@dataclass
class Node:
    path: Path
    rollout_id: str
    owner: str
    meta: dict
    boundary: dict | None
    length: int
    count: int
    descriptor: dict
    root: Path


def node(root,path,boundary=None,main=False):
    _safe(path,root)
    before=path.stat()
    if before.st_size>MAX_FILE and (main or str(path).endswith('.zst')):
        raise HistoryError('history_base_size_limit')
    ids=identity(path)
    if not ids:
        raise HistoryError('history_base_noncanonical_filename')
    record,_=header(path);meta=record['payload']
    owner=_uuid(meta.get('id'))
    if owner!=ids[0]:raise HistoryError('history_base_owner_mismatch')
    if meta.get('history_mode')!='paginated':
        raise HistoryError('history_base_not_paginated')
    if meta.get('attached_thread'):
        raise HistoryError('history_base_attached_thread_unverified')
    base=reference(meta.get('history_base'))
    expected=base['end_ordinal_exclusive'] if base else 0
    count=0;complete=0
    cutoff=boundary['end_byte_offset'] if boundary else None
    for row,start,end,_ in lines(path,cutoff):
        if type(row.get('ordinal')) is not int or row['ordinal']!=expected:
            raise HistoryError('history_base_ordinal_mismatch')
        if count and row.get('type')=='session_meta':
            raise HistoryError('history_base_multiple_headers')
        expected+=1;count+=1;complete=end
    if boundary and (expected!=boundary['end_ordinal_exclusive'] or complete!=cutoff):
        raise HistoryError('history_base_boundary_mismatch')
    if not count:raise HistoryError('history_base_empty')
    compressed=str(path).endswith('.zst')
    length=before.st_size if compressed else complete
    digest=_hash(path,length)
    after=path.stat()
    if (before.st_ino,before.st_size,before.st_mtime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns):
        raise HistoryError('history_base_source_changing')
    key='main' if main else 'base-'+ids[1]
    relative='session.jsonl' if main else 'session/history/'+ids[1]+'.jsonl'+('.zst' if compressed else '')
    descriptor={'file_key':key,'path':str(path),'relative_path':relative,'role':'transcript' if main else 'context',
                'mode':'rewrite' if compressed else 'append','complete_length':length,'size':after.st_size,'sha256':digest}
    return Node(path,ids[1],owner,meta,boundary,complete,count,descriptor,root)


def candidates(root,rollout_id):
    result=[]
    for subdir in ('sessions','archived_sessions'):
        paths=sorted((root/subdir).rglob('rollout-*.jsonl*'))
        for path in paths:
            ids=identity(path)
            if not ids or ids[1]!=rollout_id:continue
            # Native readers prefer a plain sibling over its compressed copy.
            if str(path).endswith('.zst') and Path(str(path)[:-4]).exists():continue
            _safe(path,root);result.append(path)
    return result


def lineage(root,path):
    root=Path(root).resolve();path=Path(path)
    child=node(root,path,main=True);nodes=[child];seen={child.rollout_id}
    total=child.descriptor['complete_length']
    while base:=reference(nodes[-1].meta.get('history_base')):
        if len(nodes)>MAX_DEPTH:raise HistoryError('history_base_depth_limit')
        if base['thread_id'] in seen:raise HistoryError('history_base_cycle')
        possible=candidates(root,base['thread_id'])
        if not possible:raise HistoryError('history_base_missing')
        valid=[];failure=None
        for candidate in possible:
            try:valid.append(node(root,candidate,base))
            except HistoryError as error:failure=error
        if not valid:raise failure
        # ID is immutable: conflicting complete copies must not silently choose context.
        chosen=valid[0]
        def semantic_digest(item):
            h=hashlib.sha256()
            for row,_,_,_ in lines(item.path,item.length):
                h.update(json.dumps(row,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()+b'\n')
            return h.digest()
        for other in valid[1:]:
            if semantic_digest(chosen)!=semantic_digest(other):
                raise HistoryError('ambiguous_history_base')
        total+=chosen.descriptor['complete_length']
        if total>MAX_TOTAL:raise HistoryError('history_base_total_size_limit')
        seen.add(chosen.rollout_id);nodes.append(chosen)
    return list(reversed(nodes))


def inventory(nodes):
    return [nodes[-1].descriptor]+[n.descriptor for n in nodes[:-1]]


def verify(nodes):
    for n in nodes:
        _safe(n.path,n.root)
        if _hash(n.path,n.descriptor['complete_length'])!=n.descriptor['sha256']:
            raise HistoryError('history_base_source_changing')


def graph_manifest(nodes):
    return {'version':1,'primary_rollout_id':nodes[-1].rollout_id,
            'nodes':[{'rollout_id':n.rollout_id,'owner_id':n.owner,'file_key':n.descriptor['file_key'],
                      'decoded_length':n.length,'file_name':n.path.name} for n in nodes]}


def validate_graph(entry):
    graph=entry.get('codex_history')
    if not isinstance(graph,dict) or set(graph)!={'version','primary_rollout_id','nodes'} or type(graph['version']) is not int or graph['version']!=1:
        raise HistoryError('invalid_codex_history_manifest')
    rows=graph['nodes']
    if not isinstance(rows,list) or not 2<=len(rows)<=MAX_DEPTH+1:
        raise HistoryError('history_base_depth_limit')
    files={f['file_key']:f for f in entry['files']};seen=set();keys=set()
    for row in rows:
        if not isinstance(row,dict) or set(row)!={'rollout_id','owner_id','file_key','decoded_length','file_name'}:
            raise HistoryError('invalid_codex_history_manifest')
        if any(not isinstance(row[key],str) for key in ('rollout_id','owner_id','file_key','file_name')):
            raise HistoryError('invalid_codex_history_manifest')
        rid=_uuid(row['rollout_id']);owner=_uuid(row['owner_id'])
        if rid in seen or row['file_key'] in keys:raise HistoryError('history_base_cycle')
        seen.add(rid);keys.add(row['file_key'])
        if type(row['decoded_length']) is not int or not 0<row['decoded_length']<=MAX_FILE:
            raise HistoryError('history_base_size_limit')
        ids=identity(row['file_name'])
        if ids!=(owner,rid) or Path(row['file_name']).name!=row['file_name']:
            raise HistoryError('invalid_codex_history_filename')
        item=files.get(row['file_key'])
        is_main=row is rows[-1]
        expected='session.jsonl' if is_main else 'session/history/'+rid+'.jsonl'+('.zst' if row['file_name'].endswith('.zst') else '')
        if not item or item['relative_path']!=expected or item['role']!=('transcript' if is_main else 'context') or row['file_key']!=('main' if is_main else 'base-'+rid):
            raise HistoryError('invalid_codex_history_file_mapping')
    if keys!=set(files) or rows[-1]['owner_id']!=entry['id'] or rows[-1]['rollout_id']!=graph['primary_rollout_id']:
        raise HistoryError('invalid_codex_history_manifest')
    return graph


def unpack(archive,entry,stage,*,check=None,storage=None):
    graph=validate_graph(entry);root=Path(stage)/'source'
    files={f['file_key']:f for f in entry['files']};primary=None
    for row in graph['nodes']:
        if check:check()
        path=root/'sessions'/row['file_name']
        if storage:storage.mkdirs(path.parent)
        else:path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        item=files[row['file_key']];digest=hashlib.sha256()
        with archive.open(item['file']) as source:
            if check:check()
            with (storage.open(path) if storage else path.open('xb', buffering=0 if check else -1)) as dest:
                if check:check()
                if not storage:os.chmod(path,0o600)
                for data in iter(lambda:source.read(1024*1024),b''):
                    digest.update(data)
                    view=memoryview(data)
                    while view:
                        if check:check()
                        count=dest.write(view)
                        if not count:raise HistoryError('history_base_short_write')
                        view=view[count:]
        if digest.hexdigest()!=item['sha256']:raise HistoryError('native_checksum_mismatch')
        if row['file_key']=='main':primary=path
    nodes=lineage(root,primary)
    if graph_manifest(nodes)!=graph:raise HistoryError('codex_history_manifest_mismatch')
    return nodes


def reserve_mapping(nodes):
    """Pure complete mapping, persisted by an unwired caller before publication."""
    root = str(uuid.uuid4())
    rollouts = {nodes[-1].rollout_id: root}
    owners = {nodes[-1].owner: root}
    for node in nodes[:-1]:
        rollouts[node.rollout_id] = str(uuid.uuid4())
        owners.setdefault(node.owner, rollouts[node.rollout_id])
    return {'rollouts': rollouts, 'owners': owners}


def _mapping(nodes, value, new_thread_id):
    if not isinstance(value, dict) or set(value) != {'rollouts', 'owners'}:
        raise HistoryError('invalid_reserved_mapping')
    rollouts, owners = value['rollouts'], value['owners']
    if not isinstance(rollouts, dict) or not isinstance(owners, dict) or set(rollouts) != {n.rollout_id for n in nodes} or set(owners) != {n.owner for n in nodes}:
        raise HistoryError('incomplete_reserved_mapping')
    if any(not isinstance(v, str) or _uuid(v) != v for v in (*rollouts.values(), *owners.values())) or len(set(rollouts.values())) != len(rollouts):
        raise HistoryError('invalid_reserved_mapping')
    if (set(rollouts.values()) | set(owners.values())) & ({n.owner for n in nodes} | {n.rollout_id for n in nodes}):
        raise HistoryError('reserved_mapping_reuses_source')
    if len(set(owners.values())) != len(owners) or rollouts[nodes[-1].rollout_id] != new_thread_id or owners[nodes[-1].owner] != new_thread_id or any(v not in rollouts.values() for v in owners.values()):
        raise HistoryError('invalid_reserved_mapping_root')
    for owner in owners:
        expected = new_thread_id if owner == nodes[-1].owner else next(rollouts[n.rollout_id] for n in nodes if n.owner == owner)
        if owners[owner] != expected:
            raise HistoryError('reserved_mapping_owner_mismatch')
    return dict(rollouts), dict(owners)


def restore(nodes,cwd,new_thread_id,stage,target_home,*,mapping=None,check=None,storage=None):
    """Rewrite identity/coordinates, never remove the native lineage reference."""
    if mapping is None:
        owners={nodes[-1].owner:new_thread_id}
        rollouts={nodes[-1].rollout_id:new_thread_id}
        for n in nodes[:-1]:
            rollouts[n.rollout_id]=str(uuid.uuid4())
            owners.setdefault(n.owner,rollouts[n.rollout_id])
    else:
        rollouts, owners = _mapping(nodes, mapping, new_thread_id)
    lengths={};plans=[]
    for n in nodes:
        if check:check()
        base=reference(n.meta.get('history_base'))
        new_owner=owners[n.owner];new_rollout=rollouts[n.rollout_id]
        main=n is nodes[-1]
        stamp=n.path.name[8:27]
        suffix=new_owner if new_owner==new_rollout else new_owner+'_'+new_rollout
        target=Path(target_home)/('sessions' if main else 'archived_sessions')/stamp[:4]/stamp[5:7]/stamp[8:10]/('rollout-'+stamp+'-'+suffix+'.jsonl')
        relative=target.relative_to(target_home)
        local=Path(stage)/'rewritten'/relative;length=0
        if storage:storage.mkdirs(local.parent)
        else:local.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        if check:check()
        with (storage.open(local) if storage else local.open('xb',buffering=0 if check else -1)) as output:
            if check:check()
            if not storage:os.chmod(local,0o600)
            for record,_,_,_ in lines(n.path,n.length):
                if check:check()
                payload=record.get('payload')
                if record.get('type')=='session_meta':
                    payload.update(id=new_owner,session_id=owners.get(payload.get('session_id'),new_owner),cwd=str(cwd))
                    # A local imported identity must not impersonate the source
                    # account or inherit its account-scoped discovery binding.
                    payload.pop('creator_user_id',None)
                    payload.pop('creator_account_id',None)
                    if 'account_scoped' in payload:payload['account_scoped']=False
                    if base:
                        payload['history_base']={**base,'thread_id':rollouts[base['thread_id']],
                                                 'end_byte_offset':lengths[base['thread_id']]}
                    for key in ('forked_from_id','parent_thread_id'):
                        if payload.get(key) in owners:payload[key]=owners[payload[key]]
                    if 'runtime_workspace_roots' in payload:payload['runtime_workspace_roots']=[str(cwd)]
                    record['csl_source_session_id']=n.owner
                elif isinstance(payload,dict):
                    if record.get('type')=='turn_context':
                        payload['cwd']=str(cwd)
                        if 'runtime_workspace_roots' in payload:payload['runtime_workspace_roots']=[str(cwd)]
                    elif record.get('type')=='event_msg' and payload.get('thread_id') in owners:
                        payload['thread_id']=owners[payload['thread_id']]
                body=(json.dumps(record,ensure_ascii=False)+'\n').encode()
                if check and (length+len(body)>MAX_FILE or sum(lengths.values())+length+len(body)>MAX_TOTAL):
                    raise HistoryError('history_base_size_limit')
                view=memoryview(body)
                while view:
                    if check:check()
                    count=output.write(view)
                    if not count:raise HistoryError('history_base_short_write')
                    view=view[count:]
                length+=len(body)
                if check:check()
        lengths[n.rollout_id]=length;plans.append((local,target,main))
    # Validate the rewritten graph before any publication to the destination.
    if check:check()
    lineage(Path(stage)/'rewritten',plans[-1][0])
    if check:check()
    return plans
