"""Native-file package v3: opaque raw files, explicit independent native import.

The registry can assemble this format without understanding a harness. This
module never executes a model or follows a transcript's external file paths.
"""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
import uuid
import zipfile

import csl_sync_providers as providers

FORMAT='codex-session-librarian'
MAX_FILE=2*1024**3
MAX_TOTAL=4*1024**3


def validate(archive):
    infos=archive.infolist();names=[i.filename for i in infos]
    if len(infos)>10000 or len(names)!=len(set(names)) or 'manifest.json' not in names:
        raise ValueError('invalid_native_file_members')
    if archive.getinfo('manifest.json').file_size>1024*1024:
        raise ValueError('oversized_native_manifest')
    manifest=json.loads(archive.read('manifest.json'))
    if not isinstance(manifest,dict) or manifest.get('format')!=FORMAT or manifest.get('version')!=3 or manifest.get('mode')!='raw':
        raise ValueError('unsupported_native_package')
    sessions=manifest.get('sessions')
    if not isinstance(sessions,list) or not 1<=len(sessions)<=1000:
        raise ValueError('invalid_native_sessions')
    expected={'manifest.json'};ids=set();total=0
    for entry in sessions:
        if not isinstance(entry,dict):raise ValueError('invalid_native_session')
        sid=entry.get('id')
        if not isinstance(sid,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',sid) or sid in ids:
            raise ValueError('invalid_native_source_id')
        ids.add(sid)
        if entry.get('provider') not in providers.PROVIDERS:
            raise ValueError('Unsupported native provider; supported providers: '+', '.join(providers.PROVIDERS))
        files=entry.get('files')
        if not isinstance(files,list) or not files:
            raise ValueError('missing_native_files')
        relative=set();keys=set();primary=0
        for item in files:
            if not isinstance(item,dict):raise ValueError('invalid_native_file')
            member=item.get('file');rel=item.get('relative_path');key=item.get('file_key')
            if not isinstance(member,str) or not isinstance(rel,str) or not isinstance(key,str):
                raise ValueError('invalid_native_file')
            if not re.fullmatch(r'[A-Za-z0-9._-]{1,128}',key):
                raise ValueError('invalid_native_file_key')
            parts=PurePosixPath(rel)
            if parts.is_absolute() or str(parts)!=rel or '..' in parts.parts or '\\' in rel or rel in relative or key in keys:
                raise ValueError('unsafe_native_relative_path')
            if item.get('role') not in ('transcript','subagent','tool_result','context'):
                raise ValueError('invalid_native_file_role')
            if item['role']=='transcript':
                primary+=1
                allowed_primary=('session.db',) if entry['provider']=='antigravity' else ('session.jsonl',)
                if rel not in allowed_primary or key!='main' or member!=f'sessions/{sid}/main'+Path(rel).suffix:
                    raise ValueError('invalid_native_primary')
            elif not rel.startswith('session/') or member!=f'sessions/{sid}/files/{rel}':
                raise ValueError('invalid_native_companion')
            if entry['provider']=='antigravity' and item['role']!='transcript' and (rel!='session/session.db-wal' or key!='sqlite-wal' or item['role']!='context'):
                raise ValueError('unsupported_antigravity_companion')
            if member in expected or member not in names:
                raise ValueError('duplicate_or_missing_native_member')
            info=archive.getinfo(member)
            if type(item.get('bytes')) is not int or item['bytes']!=info.file_size or not 0<=info.file_size<=MAX_FILE:
                raise ValueError('invalid_native_size')
            if not re.fullmatch(r'[0-9a-f]{64}',str(item.get('sha256',''))):
                raise ValueError('invalid_native_checksum')
            if stat.S_ISLNK(info.external_attr>>16) or info.flag_bits&1:
                raise ValueError('unsafe_native_member_type')
            expected.add(member);relative.add(rel);keys.add(key);total+=info.file_size
        if primary!=1 or entry.get('primary') not in {f['file'] for f in files if f['role']=='transcript'}:
            raise ValueError('invalid_native_primary')
        if 'codex_history' in entry:
            if entry['provider']!='codex':raise ValueError('invalid_codex_history_provider')
            import csl_codex_history
            csl_codex_history.validate_graph(entry)
    if set(names)!=expected or total>MAX_TOTAL:
        raise ValueError('unexpected_or_oversized_native_package')
    # Inspection validates every checksum, not only names or a sender manifest.
    for entry in sessions:
        for item in entry['files']:
            digest=hashlib.sha256()
            with archive.open(item['file']) as stream:
                for chunk in iter(lambda:stream.read(1024*1024),b''):digest.update(chunk)
            if digest.hexdigest()!=item['sha256']:
                raise ValueError('native_checksum_mismatch')
    return manifest


def export_native(provider,native_id,destination,*,home=None,allow_secrets=False):
    """Explicit raw export; source bytes remain unchanged, including whitespace."""
    from csl_secrets import SecretReview
    import argparse
    files=providers.raw_files(provider,native_id,home=home)
    descriptor=providers.package_descriptor(provider,native_id,home=home)
    if not descriptor['available']:
        raise ValueError('native_package_unavailable')
    destination=Path(destination).expanduser()
    if destination.exists():raise ValueError('native_destination_exists')
    review=SecretReview(argparse.Namespace(allow_secrets=allow_secrets,scrub_secrets=False))
    manifest=descriptor['manifest']
    if destination.suffix.lower()!='.zip':
        destination=destination/(provider+'-'+native_id+'.csl.zip')
    destination.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.csl-native-',dir=destination.parent) as tmp:
        snapshots=[]
        for f,item in zip(files,manifest['sessions'][0]['files']):
            local=Path(tmp)/f['file_key'];digest=hashlib.sha256()
            with Path(f['path']).open('rb') as source,local.open('xb') as target:
                os.chmod(local,0o600);left=f['complete_length']
                while left:
                    chunk=source.read(min(left,1024*1024))
                    if not chunk:raise ValueError('native_source_changed')
                    digest.update(chunk);target.write(chunk);left-=len(chunk)
            if digest.hexdigest()!=f['sha256']:raise ValueError('native_source_changed')
            # Scan selected readable files; raw cannot silently scrub/rewrite them.
            # A compressed base is retained byte-for-byte. Review its entire
            # decoded stream, including any post-cutoff bytes carried in it.
            import csl_codex_history
            compressed_base = provider=='codex' and f['relative_path'].endswith('.zst')
            if compressed_base:
                compressed_path=local.with_suffix('.zst')
                local.rename(compressed_path);local=compressed_path
            scan_context = csl_codex_history.decoded(local) if compressed_base else local.open('rb')
            with scan_context as scan:
                for line in iter(scan.readline,b''):
                    try:text=line.decode('utf-8',errors='replace' if provider=='antigravity' else 'strict')
                    except UnicodeError:continue
                    review.scan(text,'native selected file')
            item.update(bytes=f['complete_length'],sha256=digest.hexdigest())
            snapshots.append((local,item['file']))
        review.resolve()
        if review.scrubbed:
            raise ValueError('raw_native_files_cannot_be_scrubbed_without_changing_source_bytes')
        staged=Path(tmp)/'package.zip'
        with zipfile.ZipFile(staged,'w',compression=zipfile.ZIP_DEFLATED) as archive:
            for local,member in snapshots:archive.write(local,member)
            archive.writestr('manifest.json',json.dumps(manifest,ensure_ascii=False,indent=2))
        os.chmod(staged,0o600)
        with zipfile.ZipFile(staged) as archive:validate(archive)
        os.link(staged,destination)
    return destination


def _rewrite_paths(value,source_id,new_root,files,key=None):
    if isinstance(value,dict):
        return {k:_rewrite_paths(v,source_id,new_root,files,k) for k,v in value.items()}
    if isinstance(value,list):return [_rewrite_paths(v,source_id,new_root,files,key) for v in value]
    if isinstance(value,str) and ('<persisted-output>' in value or key in ('file_path','filePath','outputFilePath','path')):
        for f in files:
            if f['role']!='tool_result':continue
            relative=f['relative_path'].removeprefix('session/')
            pattern=r'(?:[A-Za-z]:)?[/\\][^\n<>"\']*?[/\\]'+re.escape(source_id)+r'[/\\]'+re.escape(relative)
            value=re.sub(pattern,lambda _:str(new_root/relative),value)
    return value


def import_native(archive,manifest,cwd,*,name=None,max_context_chars=None):
    """Install independent native copies; never overwrite existing source IDs."""
    import csl_claude
    from csl_transfer import read_records,json_line,rewrite_import_record
    cwd=Path(cwd).expanduser().resolve()
    if not cwd.is_dir():raise ValueError('native_cwd_missing')
    if name and len(manifest['sessions'])!=1:raise ValueError('name_requires_single_session')
    installed=[];plans=[]
    with tempfile.TemporaryDirectory(prefix='csl-native-import-') as tmp:
        for entry in manifest['sessions']:
            sid=str(uuid.uuid4());provider=entry['provider'];source_id=entry['id']
            if provider=='codex' and 'codex_history' in entry:
                import csl_codex_history
                if name or max_context_chars is not None:
                    raise ValueError('Codex referenced history does not support title override or static context limits')
                stage=Path(tmp)/sid;stage.mkdir(mode=0o700)
                nodes=csl_codex_history.unpack(archive,entry,stage)
                rewritten=csl_codex_history.restore(nodes,cwd,sid,stage,providers.provider_home('codex'))
                plans.extend(rewritten)
                installed.append({'provider':provider,'id':sid,'source_id':source_id,'path':str(rewritten[-1][1]),'cwd':str(cwd)})
                print('continuity: native history_base graph and bounded prefixes verified; identity/byte coordinates rebound; native/live not tested by import')
                continue
            if provider=='antigravity':
                import csl_antigravity
                if name or max_context_chars is not None:
                    raise ValueError('Antigravity title override and static context-limit analysis are not implemented')
                stage=Path(tmp)/sid;stage.mkdir(mode=0o700)
                original=stage/'session.db'
                for item in entry['files']:
                    selected=original if item['role']=='transcript' else Path(str(original)+'-wal')
                    digest=hashlib.sha256()
                    with archive.open(item['file']) as source,selected.open('xb') as output:
                        os.chmod(selected,0o600)
                        for chunk in iter(lambda:source.read(1024*1024),b''):
                            digest.update(chunk);output.write(chunk)
                    if digest.hexdigest()!=item['sha256']:raise ValueError('native_checksum_mismatch')
                local=stage/'imported.db'
                csl_antigravity.rewrite_copy(original,local,source_id,sid,cwd)
                target=providers.provider_home('antigravity')/'conversations'/(sid+'.db')
                plans.append((local,target,True))
                installed.append({'provider':provider,'id':sid,'source_id':source_id,'path':str(target),'cwd':str(cwd)})
                continue
            if provider=='claude':
                target=csl_claude.target_path(sid,cwd)
            elif provider=='codex':
                stamp=datetime_now()
                target=providers.provider_home('codex')/'sessions'/stamp[:4]/stamp[5:7]/stamp[8:10]/('rollout-'+stamp.replace(':','-')+'-'+sid+'.jsonl')
            else:
                raise ValueError('Unsupported native provider')
            companion=target.with_suffix('') if provider=='claude' else target.parent/sid
            stage=Path(tmp)/sid;stage.mkdir(mode=0o700)
            for item in entry['files']:
                primary=item['file']==entry['primary']
                relative=item['relative_path'].removeprefix('session/')
                dest=target if primary else companion/relative
                local=stage/('main'+Path(target).suffix) if primary else stage/'files'/relative
                local.parent.mkdir(parents=True,exist_ok=True)
                with archive.open(item['file']) as source:body=source.read()
                if hashlib.sha256(body).hexdigest()!=item['sha256']:raise ValueError('native_checksum_mismatch')
                if primary:
                    import io
                    rows=list(read_records(io.BytesIO(body)))
                    if provider=='claude' and (not any(r.get('type') in ('user','assistant') for r in rows) or any(r.get('sessionId') not in (None,source_id) for r in rows)):
                        raise ValueError('invalid_native_primary_content')
                    if provider=='claude' and any(not isinstance(r.get('message'),dict) or r['message'].get('role')!=r['type'] or not isinstance(r.get('uuid'),str) for r in rows if r.get('type') in ('user','assistant')):
                        raise ValueError('invalid_native_primary_content')
                    if provider=='codex' and not any(r.get('type')=='session_meta' and r.get('payload',{}).get('id')==source_id for r in rows):
                        raise ValueError('invalid_native_primary_content')
                    if provider=='codex' and any(r.get('type')=='session_meta' and isinstance(r.get('payload'),dict) and r['payload'].get('history_base') for r in rows):
                        raise ValueError('external_history_base_not_packaged')
                if provider=='claude' and item['role'] in ('transcript','subagent') and item['file'].endswith('.jsonl'):
                    import io
                    records=[]
                    for record in read_records(io.BytesIO(body)):
                        # Raw owner context preserves thinking, but imported policy stays local.
                        r=csl_claude.rewrite(record,sid,cwd,source_id)
                        if r is not None:records.append(_rewrite_paths(r,source_id,companion,entry['files']))
                    body=b''.join(json_line(r) for r in records)
                elif provider=='claude' and item['role']=='context' and item['file'].endswith('.json'):
                    r=json.loads(body)
                    if isinstance(r,dict):
                        for field in ('sessionId','parentSessionId'):
                            if r.get(field)==source_id:r[field]=sid
                    body=json.dumps(r,ensure_ascii=False).encode()
                elif provider=='codex' and primary:
                    import io
                    rows=[]
                    for record in read_records(io.BytesIO(body)):
                        rewritten=rewrite_import_record(record,sid,entry,cwd,datetime_now())
                        if rewritten.get('type')=='session_meta':rewritten['csl_source_session_id']=source_id
                        rows.append(rewritten)
                    body=b''.join(json_line(r) for r in rows)
                if primary and name:
                    if provider=='claude':body+=json_line({'type':'custom-title','sessionId':sid,'customTitle':name})
                    elif provider=='codex':body+=json_line({'type':'event_msg','timestamp':datetime_now(),'payload':{'type':'thread_name_updated','thread_name':name}})
                with local.open('xb') as output:os.chmod(local,0o600);output.write(body)
                if provider=='codex' and primary:
                    from csl_display import project_file
                    projected=local.with_name('display.jsonl')
                    project_file(local,projected,read_records,json_line,{source_id,sid})
                    local=projected
                if primary:
                    from csl_diagnostics import preflight
                    preflight(local,provider,max_context_chars=max_context_chars)
                plans.append((local,dest,primary))
            installed.append({'provider':provider,'id':sid,'source_id':source_id,'path':str(target),'cwd':str(cwd)})
        # Validate all destinations first; exclusive hard links never overwrite existing files.
        if any(dest.exists() or dest.is_symlink() for _,dest,_ in plans):raise ValueError('native_destination_exists')
        committed=[]
        try:
            # Copy beside destinations to support imports across filesystems.
            for source,dest,_ in sorted(plans,key=lambda p: p[2]):
                dest.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
                with tempfile.NamedTemporaryFile(dir=dest.parent,delete=False,prefix='.csl-stage-') as stream:
                    stage_path=Path(stream.name);stream.write(source.read_bytes());stream.flush();os.fsync(stream.fileno())
                try:os.link(stage_path,dest);committed.append(dest)
                finally:stage_path.unlink(missing_ok=True)
        except BaseException:
            for dest in reversed(committed):dest.unlink(missing_ok=True)
            raise
    return installed


def datetime_now():
    from datetime import datetime,timezone
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
