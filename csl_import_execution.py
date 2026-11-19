"""Private Linux Codex publisher. No CLI, native authority, index or Start caller.

The caller owns qualified borrowed directory FDs and the original checker. A
fresh lookup is metadata-only; it cannot resume an older publication window.
"""
from contextlib import contextmanager
import ctypes
import datetime as dt
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

from csl_backup_core import canonical
from csl_canonical import strict_json
import csl_codex_history as history
import csl_durable as durable
import csl_execution as execution
from csl_transfer import inspect_bundle, json_line, read_records, rewrite_import_record

MAX_JOURNAL = 256 * 1024
MAX_MEMBERS = 33
ROW_KEYS = frozenset('v revision intent plan state roots rewrite_at planning workspace members receipt'.split())
MEMBER_KEYS = frozenset('path role source_rollout source_owner native_id owner_id size sha256 keeper identity phase scratch_path scratch_identity'.split())
PHASES = ('planned', 'allocating', 'writing', 'kept', 'publishing', 'published', 'retiring', 'retired')
STATES = ('planning', 'reserved', 'preparing', 'prepared', 'publishing', 'receipted', 'retiring_keepers', 'complete')


class Pending(execution.ExecutionError):
    """Custody is retained; absence or another UUID must not be inferred."""


class _Row(dict):
    def __init__(self,value,custody,fd=None):
        super().__init__(value)
        self.custody,self.fd,self.snapshot=custody,fd,None


class _Custody:
    """Pin the exact current metadata inode, including our own replacements."""
    def __init__(self,fd,req):
        self.parent,self.name=fd,_name(req)
        self.current=None
        self.transition=None
        self.owned=set()
        self.absent=False

    def __enter__(self):return self

    def __exit__(self,*exc):
        for fd in self.owned:os.close(fd)

    def keep(self,fd):
        self.owned.add(fd)
        return fd

    @staticmethod
    def stamp(s):return (s.st_size,s.st_mtime_ns,s.st_ctime_ns)

    def adopt(self,row):
        previous=self.current
        row.snapshot=self.stamp(durable.regular(row.fd))
        self.current,self.transition,self.absent=row,None,False
        if previous is not None and previous.fd!=row.fd:
            self.owned.remove(previous.fd)
            os.close(previous.fd)

    def check(self):
        candidates=self.transition if self.transition is not None else (self.current,)
        try:
            named=os.stat(self.name,dir_fd=self.parent,follow_symlinks=False)
        except FileNotFoundError:
            if None in candidates and (self.absent or self.transition is not None):return
            if self.current is None and not self.absent:return
            raise Pending('execution_journal_disappeared') from None
        if self.current is None and self.transition is None and not self.absent:return
        for row in candidates:
            if row is None:continue
            held=os.fstat(row.fd)
            if (named.st_dev,named.st_ino)==(held.st_dev,held.st_ino):
                durable.regular(row.fd)
                if self.transition is None and self.stamp(held)!=row.snapshot:
                    raise Pending('execution_journal_changed')
                return
        raise Pending('execution_journal_inode_changed')


class _Scratch:
    """Owned per-entry inode ledger. Unknown entries are NEVER adopted/reset."""
    def __init__(self,journal,req,custody,field,check,fault):
        self.journal,self.req,self.custody,self.field=journal,req,custody,field
        self.check,self.fault,self.fd=check,fault,None

    @property
    def row(self):return self.custody.current

    @property
    def spec(self):return self.row[self.field]

    def update(self,**changes):
        spec=strict_json(canonical(self.spec));spec.update(changes)
        _advance(self.journal,self.req,self.row,self.check,self.fault,**{self.field:spec})

    def entry(self,path,**changes):
        entries=strict_json(canonical(self.spec['entries']))
        found=next((e for e in entries if e['path']==path),None)
        if found is None:
            found={'path':path,'kind':changes.pop('kind'),'identity':None,'phase':'allocating'}
            entries.append(found)
        found.update(changes)
        self.update(entries=entries)

    def __enter__(self):
        if self.spec is None:
            name=('.planning-' if self.field=='planning' else '.publisher-')+self.req.value['import_key']
            _advance(self.journal,self.req,self.row,self.check,self.fault,**{self.field:dict(name=name,identity=None,phase='allocating',entries=[])})
        name=self.spec['name']
        if self.spec['phase']=='allocating':
            try:os.stat(name,dir_fd=self.journal,follow_symlinks=False)
            except FileNotFoundError:
                _effect(self.check,os.mkdir,name,0o700,dir_fd=self.journal)
                self.fault('scratch_root_created_'+self.field)
            else:raise Pending('execution_workspace_uncommitted')
            fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.journal)
            try:
                inode=identity(fd)
                _effect(self.check,os.fsync,self.journal)
                self.update(identity=inode,phase='owned')
            finally:os.close(fd)
        try:self.fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.journal)
        except FileNotFoundError:
            if self.spec['phase'] not in ('retiring','retired'):raise Pending('execution_workspace_missing') from None
        self.check.bindings.append(self.bound)
        try:self.check()
        except BaseException:
            self.__exit__();raise
        self.path=Path(os.readlink('/proc/self/fd/'+str(self.fd))) if self.fd is not None else None
        return self

    def __exit__(self,*exc):
        self.check.bindings.remove(self.bound)
        if self.fd is not None:os.close(self.fd)

    def bound(self):
        try:named=os.stat(self.spec['name'],dir_fd=self.journal,follow_symlinks=False)
        except FileNotFoundError:
            if self.spec['phase'] in ('retiring','retired'):return
            raise Pending('execution_workspace_changed') from None
        if self.fd is None or identity(self.fd)!=self.spec['identity'] or not _same(named,self.spec['identity']):
            raise Pending('execution_workspace_changed')

    def relative(self,path):
        value=Path(path).relative_to(self.path).as_posix()
        _scratch_path(value)
        return value

    @contextmanager
    def parent(self,path,*,admit=True):
        parts=PurePosixPath(path).parts;fd=os.dup(self.fd);prefix=[]
        def bound():
            walked=os.dup(self.fd)
            try:
                for part in prefix:
                    nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=walked)
                    os.close(walked);walked=nxt
                a,b=os.fstat(walked),os.fstat(fd)
                if (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino):raise Pending('execution_workspace_parent_changed')
            except (FileNotFoundError,NotADirectoryError):raise Pending('execution_workspace_parent_changed') from None
            finally:os.close(walked)
        if admit:self.check.bindings.append(bound)
        try:
            for part in parts[:-1]:
                if admit:self.check()
                nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
                os.close(fd);fd=nxt
                durable.directory(fd)
                prefix.append(part)
                e=next((e for e in self.spec['entries'] if e['path']=='/'.join(prefix)),None)
                if e is None or e['identity'] is None or not _same(os.fstat(fd),e['identity']):raise Pending('execution_workspace_entry_changed')
            yield fd,parts[-1]
        finally:
            if admit:self.check.bindings.remove(bound)
            os.close(fd)

    def create(self,path,kind):
        _scratch_path(path)
        item=next((e for e in self.spec['entries'] if e['path']==path),None)
        if item is None:self.entry(path,kind=kind)
        else:
            if item['kind']!=kind:raise Pending('execution_workspace_entry_changed')
            if item['phase']=='owned' and kind=='directory':
                with self.parent(path) as (parent,name):
                    fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
                    try:self.verify(fd,item)
                    except BaseException:os.close(fd);raise
                    return fd
            if item['phase']!='allocating':raise Pending('execution_workspace_entry_exists')
        with self.parent(path) as (parent,name):
            try:os.stat(name,dir_fd=parent,follow_symlinks=False)
            except FileNotFoundError:pass
            else:raise Pending('execution_workspace_entry_uncommitted')
            if kind=='directory':
                _effect(self.check,os.mkdir,name,0o700,dir_fd=parent)
                fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
            else:fd=_open(self.check,name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=parent)
            try:
                s=os.fstat(fd)
                if kind=='directory':durable.directory(fd)
                else:durable.regular(fd)
                self.fault('scratch_created_'+self.field)
                _effect(self.check,os.fsync,parent)
                self.entry(path,identity={'device':str(s.st_dev),'inode':str(s.st_ino)},phase='owned')
                self.fault('scratch_owned_'+self.field)
                return fd
            except BaseException:os.close(fd);raise

    def mkdirs(self,path):
        relative=self.relative(path)
        for i in range(1,len(PurePosixPath(relative).parts)+1):
            fd=self.create('/'.join(PurePosixPath(relative).parts[:i]),'directory');os.close(fd)

    def open(self,path):
        relative=self.relative(path)
        fd=self.create(relative,'file')
        return _ScratchStream(self,relative,fd)

    def verify(self,fd,entry):
        if entry['kind']=='file':s=durable.regular(fd)
        else:s=durable.directory(fd)
        if not _same(s,entry['identity']):raise Pending('execution_workspace_entry_changed')

    def preflight(self):
        if self.fd is None:raise Pending('execution_workspace_unavailable')
        expected={e['path']:e for e in self.spec['entries']}
        actual=set()
        for root,dirs,files,parent in os.fwalk('.',follow_symlinks=False,dir_fd=self.fd):
            for name in dirs+files:
                self.check();path=(PurePosixPath(root)/name).as_posix();actual.add(path)
                e=expected.get(path)
                if e is None or e['identity'] is None or e['phase']=='deleted':raise Pending('execution_workspace_unknown_entry')
                fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=parent)
                try:self.verify(fd,e)
                finally:os.close(fd)
        for path,e in expected.items():
            if path not in actual and e['phase']=='owned':raise Pending('execution_workspace_entry_missing')

    def clear(self):
        self.preflight()
        if self.spec['phase']!='clearing':self.update(phase='clearing')
        for e in sorted(self.spec['entries'],key=lambda e:len(PurePosixPath(e['path']).parts),reverse=True):
            if e['phase']=='deleted':continue
            path=e['path']
            if e['phase']=='allocating':
                self.entry(path,phase='deleted');continue  # preflight proved absence
            if e['phase']=='owned':self.entry(path,phase='deleting')
            with self.parent(path) as (parent,name):
                try:fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=parent)
                except FileNotFoundError:pass  # only a durable deleting intent permits absence
                else:
                    dispatched=False
                    def bound():
                        try:named=os.stat(name,dir_fd=parent,follow_symlinks=False)
                        except FileNotFoundError:
                            if dispatched:return
                            raise Pending('execution_workspace_entry_missing') from None
                        self.verify(fd,e)
                        if not _same(named,e['identity']):raise Pending('execution_workspace_entry_changed')
                    def remove():
                        nonlocal dispatched
                        dispatched=True
                        (os.unlink if e['kind']=='file' else os.rmdir)(name,dir_fd=parent)
                    self.check.bindings.append(bound)
                    try:
                        self.verify(fd,e)
                        _effect(self.check,remove)
                        self.fault('scratch_unlinked_'+self.field)
                    finally:
                        self.check.bindings.remove(bound)
                        os.close(fd)
                _effect(self.check,os.fsync,parent)
            self.entry(path,phase='deleted')
        self.update(entries=[],phase='owned')

    def cleanup(self):
        if self.spec['phase']=='retired':return
        if self.spec['phase']!='retiring':
            self.clear();self.update(phase='retiring')
        try:os.stat(self.spec['name'],dir_fd=self.journal,follow_symlinks=False)
        except FileNotFoundError:pass
        else:
            self.bound()
            _effect(self.check,os.rmdir,self.spec['name'],dir_fd=self.journal)
            self.fault('scratch_root_unlinked_'+self.field)
        _effect(self.check,os.fsync,self.journal)
        self.update(phase='retired')


def _scratch_path(value):
    if not isinstance(value,str) or not value or len(value.encode())>4096 or PurePosixPath(value).is_absolute() or str(PurePosixPath(value))!=value or any(p in ('','..','.') for p in value.split('/')) or '\\' in value or '\0' in value:
        raise execution.ExecutionError('execution_workspace_path_invalid')


class _ScratchStream:
    def __init__(self,storage,path,fd):
        self.storage,self.path,self.fd=storage,path,fd
        self.stream=os.fdopen(fd,'wb',buffering=0)
    def bound(self):
        held=durable.regular(self.fd)
        with self.storage.parent(self.path,admit=False) as (parent,name):
            named=os.stat(name,dir_fd=parent,follow_symlinks=False)
            if (held.st_dev,held.st_ino)!=(named.st_dev,named.st_ino):raise Pending('execution_workspace_entry_changed')
    def __enter__(self):
        self.storage.check.bindings.append(self.bound)
        return self
    def write(self,data):
        n=_effect(self.storage.check,self.stream.write,data)
        self.storage.fault('scratch_written_'+self.storage.field)
        return n
    def __exit__(self,*exc):
        self.storage.check.bindings.remove(self.bound)
        self.stream.close()


def _sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def identity(fd):
    """Public metadata only; caller independently qualifies the expected root."""
    s = durable.directory(fd)
    return {'device': str(s.st_dev), 'inode': str(s.st_ino)}


def _identity(value):
    return (isinstance(value, dict) and set(value) == {'device', 'inode'} and
            execution._uint(value['device']) and execution._uint(value['inode'], positive=True))


def _same(s, value):
    return value == {'device': str(s.st_dev), 'inode': str(s.st_ino)}


def _business(intent):
    return {k: v for k, v in intent.items() if k != 'authority'}


def _name(req):
    return req.value['import_key'] + '.json'


def _checked(journal, home, req, roots, check):
    if not callable(check) or not isinstance(roots, dict) or set(roots) != {'journal', 'home'} or not all(_identity(x) for x in roots.values()):
        raise execution.ExecutionError('execution_root_context_invalid')
    def guard():
        check()
        if identity(journal) != roots['journal'] or identity(home) != roots['home']:
            raise execution.ExecutionError('execution_root_changed')
        v = req.value
        cwd = os.open('/', os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            for part in PurePosixPath(v['cwd']).parts[1:]:
                nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=cwd)
                os.close(cwd)
                cwd = nxt
            s = os.fstat(cwd)
            if s.st_uid != os.getuid() or str(s.st_dev) != v['cwd_device'] or str(s.st_ino) != v['cwd_inode']:
                raise execution.ExecutionError('execution_cwd_changed')
        finally:
            os.close(cwd)
        for binding in tuple(guard.bindings):binding()
        check()
        for binding in tuple(guard.bindings):binding()
    guard.bindings=[]
    guard()
    return guard


def _noreplace(fd, source, target, check):
    # Linux-only private profile. Never fall back to link/unlink or overwrite.
    libc = ctypes.CDLL(None, use_errno=True)
    rename = getattr(libc, 'renameat2', None)
    if rename is None:
        raise execution.ExecutionError('execution_atomic_rename_unavailable')
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    check()  # Dynamic loader work above may block; never dispatch on its old check.
    result = rename(fd, os.fsencode(source), fd, os.fsencode(target), 1)
    error = ctypes.get_errno()
    check()
    if result != 0:
        raise OSError(error, 'execution_metadata_rename_failed')


def _effect(check, operation, *args, **kwargs):
    check()
    try:
        return operation(*args, **kwargs)
    finally:
        check()  # Known authority loss also wins over syscall error/unknown.


def _open(check, *args, **kwargs):
    check()
    try:
        fd=os.open(*args,**kwargs)
    except BaseException:
        check()
        raise
    try:
        check()
    except BaseException:
        os.close(fd)
        raise
    return fd


@contextmanager
def _final_check(check):
    try:
        yield
    finally:
        check()


def _save(fd, req, old, row, check, fault):
    """Caller holds operation.lock. No transient metadata hard link exists."""
    raw = canonical(row) + b'\n'
    if len(raw) > MAX_JOURNAL:
        raise execution.ExecutionError('execution_journal_limit')
    name = _name(req)
    stage = '.' + req.value['import_key'] + '.' + str(row['revision']) + '.tmp'
    check()
    # A pre-rename orphan stays pending; its filename/hash is not ownership.
    try:
        out = _open(check,stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=fd)
    except FileExistsError:
        raise Pending('execution_metadata_staging_unknown') from None
    try:
        held = durable.regular(out)
        offset = 0
        while offset < len(raw):
            check()
            n = _effect(check,os.write,out, raw[offset:])
            if n <= 0:
                raise Pending('execution_metadata_short_write')
            offset += n
        check()
        _effect(check,os.fsync,out)
        fault('metadata_' + row['state'] + '_written')
        check()
        if not _same(os.stat(stage, dir_fd=fd, follow_symlinks=False), {'device': str(held.st_dev), 'inode': str(held.st_ino)}):
            raise Pending('execution_metadata_staging_changed')
        durable.regular(out)
        if old is not None:
            _current(fd,req,old,check)
        row.fd=row.custody.keep(os.dup(out))
        row.custody.transition=(old,row)
        check()
        if old is None:
            _noreplace(fd, stage, name, check)
        else:
            _effect(check,os.replace,stage, name, src_dir_fd=fd, dst_dir_fd=fd)
        row.custody.adopt(row)
        _current(fd,req,row,check)
        fault('metadata_' + row['state'] + '_renamed')
        check()
        _effect(check,os.fsync,fd)
        fault('metadata_' + row['state'] + '_fsynced')
        check()
    finally:
        os.close(out)  # Unknown staging/publication is NEVER rolled back.


def _path(value):
    p = PurePosixPath(value) if isinstance(value, str) else None
    if p is None or p.is_absolute() or str(p) != value or len(value.encode()) > 4096 or any(x in ('', '.', '..') for x in value.split('/')) or '\\' in value or '\0' in value:
        raise execution.ExecutionError('execution_member_path_invalid')
    if len(p.parts) != 5 or p.parts[0] not in ('sessions', 'archived_sessions') or not re.fullmatch(r'\d{4}/\d\d/\d\d', '/'.join(p.parts[1:4])) or not p.name.endswith('.jsonl') or not history.identity(p):
        raise execution.ExecutionError('execution_member_path_invalid')
    return p


@contextmanager
def _parent(home, path, check, *, create=False):
    parts = _path(path).parts
    current = os.dup(home)
    prefix=[]
    def bound():
        walked=os.dup(home)
        try:
            for part in prefix:
                nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=walked)
                os.close(walked)
                walked=nxt
                durable.directory(walked)
            expected,held=os.fstat(walked),os.fstat(current)
            if (expected.st_dev,expected.st_ino)!=(held.st_dev,held.st_ino):
                raise Pending('execution_destination_parent_changed')
        except (FileNotFoundError,NotADirectoryError):
            raise Pending('execution_destination_parent_changed') from None
        finally:os.close(walked)
    check.bindings.append(bound)
    try:
        for part in parts[:-1]:
            _dispatch(current, check)
            if create:
                try:
                    _effect(check,os.mkdir,part, 0o700, dir_fd=current)
                    check()
                    _effect(check,os.fsync,current)
                except FileExistsError:
                    pass
            nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=current)
            os.close(current)
            current = nxt
            prefix.append(part)
            durable.directory(current)
        _dispatch(current, check)
        yield current, parts[-1]
    finally:
        check.bindings.remove(bound)
        os.close(current)


def _dispatch(parent, check):
    check()
    durable.directory(parent)
    check()


def _file(parent, name, expected, check, links):
    _dispatch(parent, check)
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=parent)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink not in links or not _same(before, expected['identity']):
            raise Pending('execution_member_identity_conflict')
        digest, size = hashlib.sha256(), 0
        while True:
            check()
            body = os.read(fd, min(1024 * 1024, expected['size'] + 1 - size))
            check()
            if not body:
                break
            size += len(body)
            if size > expected['size']:
                raise Pending('execution_member_bytes_changed')
            digest.update(body)
        after = os.fstat(fd)
        named = os.stat(name, dir_fd=parent, follow_symlinks=False)
        stable = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_nlink)
        if stable(before) != stable(after) or not _same(named, expected['identity']) or size != expected['size'] or digest.hexdigest() != expected['sha256']:
            raise Pending('execution_member_bytes_changed')
        _dispatch(parent, check)
        return after
    finally:
        os.close(fd)


def _receipt(row):
    v, plan = row['intent'], row['plan']
    inventory = [{k: m[k] for k in MEMBER_KEYS if k not in ('phase', 'keeper','scratch_path','scratch_identity')} for m in row['members']]
    return dict(schema_version=1, kind='execution_import_result', state='imported',
                validation='native_unverified', execution_id=v['execution_id'], import_key=v['import_key'],
                request_sha256=_sha(_business(v)), package_size=v['package_size'], package_sha256=v['package_sha256'],
                provider='codex', imported_native_id=plan['mapping']['owners'][v['source_id']],
                mapping_sha256=_sha(plan['mapping']), target_sha256=_sha({k: v[k] for k in ('owner','host_id','assignment_id','generation','boot_id')}),
                cwd_sha256=_sha({k: v[k] for k in ('cwd','cwd_device','cwd_inode')}), inventory_sha256=_sha({'root':row['roots']['home'],'members':inventory}))


def _validate(row, req):
    try:
        return _validate_row(row, req)
    except execution.ExecutionError:
        raise
    except (TypeError, KeyError, ValueError, OverflowError):
        raise execution.ExecutionError('execution_journal_conflict') from None


def _validate_row(row, req):
    if isinstance(row, dict) and row.get('v') == 1:
        if set(row) != {'v','revision','intent','plan','state'} or type(row['v']) is not int or type(row['revision']) is not int or row['revision'] != 1 or row['state'] != 'reserved':
            raise execution.ExecutionError('execution_journal_conflict')
    elif not isinstance(row, dict) or set(row) != ROW_KEYS or type(row['v']) is not int or row['v'] != 3 or type(row['revision']) is not int or not 1 <= row['revision'] < 2**63 or row['state'] not in STATES:
        raise execution.ExecutionError('execution_journal_conflict')
    intent = row['intent']
    execution.request(canonical(dict(intent, operation='import')))
    if _business(intent) != _business(execution._intent_key(req)):
        raise execution.ExecutionError('execution_journal_conflict')
    if row['state']!='planning':execution._plan(row['plan'], req)
    if row['v'] == 1:
        return row
    if intent['provider'] != 'codex':
        raise execution.ExecutionError('execution_publication_unsupported')
    if not isinstance(row['roots'], dict) or set(row['roots']) != {'journal','home'} or not all(_identity(x) for x in row['roots'].values()) or not isinstance(row['rewrite_at'], str):
        raise execution.ExecutionError('execution_journal_conflict')
    stamp = dt.datetime.strptime(row['rewrite_at'], '%Y-%m-%dT%H:%M:%SZ')
    if stamp.strftime('%Y-%m-%dT%H:%M:%SZ') != row['rewrite_at']:
        raise execution.ExecutionError('execution_journal_conflict')
    planning=row['planning']
    _validate_scratch(planning,'planning',req)
    workspace=row['workspace']
    _validate_scratch(workspace,'workspace',req)
    if row['receipt'] is None and any(spec is not None and spec['phase'] in ('retiring','retired') for spec in (planning,workspace)):
        raise execution.ExecutionError('execution_workspace_conflict')
    members = row['members']
    if not isinstance(members, list) or len(members) > MAX_MEMBERS:
        raise execution.ExecutionError('execution_journal_conflict')
    if row['state'] in ('planning','reserved'):
        if members or row['receipt'] is not None:
            raise execution.ExecutionError('execution_journal_conflict')
        if row['state']=='planning' and (row['plan'] is not None or workspace is not None):
            raise execution.ExecutionError('execution_journal_conflict')
        return row
    mapping = row['plan']['mapping']
    if workspace is None or workspace['phase'] not in (('owned',) if row['receipt'] is None else ('owned','clearing','retiring','retired')):
        raise execution.ExecutionError('execution_workspace_conflict')
    if not members or len(members) != len(mapping['rollouts']) or {m.get('source_rollout') for m in members if isinstance(m,dict)} != set(mapping['rollouts']) or len({m.get('path') for m in members if isinstance(m,dict)}) != len(members):
        raise execution.ExecutionError('execution_journal_conflict')
    for i, m in enumerate(members):
        if not isinstance(m, dict) or set(m) != MEMBER_KEYS or m['phase'] not in PHASES or m['role'] != ('root' if i == len(members)-1 else 'ancestor') or m['source_owner'] not in mapping['owners'] or m['native_id'] != mapping['rollouts'].get(m['source_rollout']) or m['owner_id'] != mapping['owners'][m['source_owner']]:
            raise execution.ExecutionError('execution_journal_conflict')
        p = _path(m['path'])
        if history.identity(p) != (m['owner_id'], m['native_id']) or p.parts[0] != ('sessions' if m['role']=='root' else 'archived_sessions') or m['keeper'] != '.csl-' + req.value['import_key'] + '-' + str(i) or type(m['size']) is not int or not 0 < m['size'] <= history.MAX_FILE or not isinstance(m['sha256'], str) or not execution.SHA.fullmatch(m['sha256']):
            raise execution.ExecutionError('execution_journal_conflict')
        if (m['identity'] is None) != (m['phase'] in ('planned','allocating')) or (m['identity'] is not None and not _identity(m['identity'])):
            raise execution.ExecutionError('execution_journal_conflict')
        if m['scratch_path'] not in ('singleton.jsonl','rewritten/'+m['path']) or not _identity(m['scratch_identity']):
            raise execution.ExecutionError('execution_workspace_conflict')
    if sum(m['size'] for m in members) > history.MAX_TOTAL or members[-1]['owner_id'] != mapping['owners'][intent['source_id']]:
        raise execution.ExecutionError('execution_journal_conflict')
    phases = [m['phase'] for m in members]
    if row['state'] == 'preparing' and any(p not in PHASES[:4] for p in phases):
        raise execution.ExecutionError('execution_journal_conflict')
    if row['state'] == 'prepared' and any(p != 'kept' for p in phases):
        raise execution.ExecutionError('execution_journal_conflict')
    if row['state'] == 'publishing' and any(p not in ('kept','publishing','published') for p in phases):
        raise execution.ExecutionError('execution_journal_conflict')
    if row['state'] in ('receipted','retiring_keepers','complete'):
        allowed = {'receipted':('published',),'retiring_keepers':('published','retiring','retired'),'complete':('retired',)}[row['state']]
        if any(p not in allowed for p in phases) or canonical(row['receipt']) != canonical(_receipt(row)):
            raise execution.ExecutionError('execution_receipt_conflict')
    elif row['receipt'] is not None:
        raise execution.ExecutionError('execution_receipt_conflict')
    if row['state'] in ('preparing','publishing','retiring_keepers'):
        order = {'preparing':('kept','writing','allocating','planned'),
                 'publishing':('published','publishing','kept'),
                 'retiring_keepers':('retired','retiring','published')}[row['state']]
        positions=[order.index(p) for p in phases]
        if positions != sorted(positions) or sum(p in ('writing','allocating','publishing','retiring') for p in phases)>1:
            raise execution.ExecutionError('execution_member_order_conflict')
    return row


def _validate_scratch(spec,field,req):
    if spec is None:return
    name=('.planning-' if field=='planning' else '.publisher-')+req.value['import_key']
    if not isinstance(spec,dict) or set(spec)!={'name','identity','phase','entries'} or spec['name']!=name or spec['phase'] not in ('allocating','owned','clearing','retiring','retired'):
        raise execution.ExecutionError('execution_workspace_conflict')
    if (spec['identity'] is None)!=(spec['phase']=='allocating') or spec['identity'] is not None and not _identity(spec['identity']):
        raise execution.ExecutionError('execution_workspace_conflict')
    entries=spec['entries']
    if not isinstance(entries,list) or len(entries)>512 or spec['phase']=='allocating' and entries:
        raise execution.ExecutionError('execution_workspace_conflict')
    seen={}
    for e in entries:
        if not isinstance(e,dict) or set(e)!={'path','kind','identity','phase'} or e['kind'] not in ('file','directory') or e['phase'] not in ('allocating','owned','deleting','deleted'):
            raise execution.ExecutionError('execution_workspace_conflict')
        _scratch_path(e['path'])
        if e['path'] in seen or e['phase']=='allocating' and e['identity'] is not None or e['identity'] is None and e['phase'] not in ('allocating','deleted') or e['identity'] is not None and not _identity(e['identity']):
            raise execution.ExecutionError('execution_workspace_conflict')
        parent=str(PurePosixPath(e['path']).parent)
        if parent!='.' and (parent not in seen or seen[parent]['kind']!='directory'):
            raise execution.ExecutionError('execution_workspace_conflict')
        seen[e['path']]=e
    if spec['phase'] in ('retiring','retired') and any(e['phase']!='deleted' for e in entries):
        raise execution.ExecutionError('execution_workspace_conflict')


def _load(fd, req, check, custody):
    check()
    try:
        raw, held = durable.read_owned(fd, _name(req), MAX_JOURNAL)
    except durable.MissingMetadata:
        custody.absent=True
        return None
    custody.keep(held)
    row = _Row(_validate(strict_json(raw), req),custody,held)
    custody.adopt(row)
    check()
    return row


def _current(fd,req,row,check):
    check()
    row.custody.check()
    raw,held=durable.read_owned(fd,_name(req),MAX_JOURNAL)
    try:
        expected=os.fstat(row.fd)
        observed=os.fstat(held)
        if (expected.st_dev,expected.st_ino)!=(observed.st_dev,observed.st_ino) or raw != canonical(row)+b'\n':
            raise Pending('execution_journal_changed')
    finally:os.close(held)
    check()


def _advance(fd, req, row, check, fault, **changes):
    next_row = _Row(strict_json(canonical(row)),row.custody)
    next_row.update(changes)
    next_row['revision'] += 1
    _validate(next_row, req)
    _save(fd, req, row, next_row, check, fault)
    return next_row


def _member(fd, req, row, index, check, fault, **changes):
    members = strict_json(canonical(row['members']))
    members[index].update(changes)
    return _advance(fd, req, row, check, fault, members=members)


def _hash_file(path, check):
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as src:
        while True:
            check()
            chunk = src.read(1024*1024)
            check()
            if not chunk:
                break
            size += len(chunk)
            if size > history.MAX_FILE:
                raise execution.ExecutionError('execution_member_limit')
            digest.update(chunk)
    return size, digest.hexdigest()


def _rewrite(captured, req, row, scratch, check,storage):
    if captured.sha != req.value['package_sha256'] or captured.size != req.value['package_size']:
        raise execution.ExecutionError('execution_capture_conflict')
    with zipfile.ZipFile(captured.rewind()) as archive:
        manifest = inspect_bundle(archive, check=check)
        v = req.value
        if manifest['mode'] != 'raw' or len(manifest['sessions']) != 1:
            raise execution.ExecutionError('execution_package_profile_conflict')
        entry = manifest['sessions'][0]
        if entry.get('provider','codex') != 'codex' or entry['id'] != v['source_id']:
            raise execution.ExecutionError('execution_package_profile_conflict')
        mapping = row['plan']['mapping']
        root = mapping['owners'][v['source_id']]
        if entry.get('codex_history'):
            if any(n['file_name'].endswith('.zst') for n in entry['codex_history']['nodes']):
                raise execution.ExecutionError('execution_compressed_graph_unsupported')
            nodes = history.unpack(archive, entry, scratch, check=check,storage=storage)
            paths = history.restore(nodes, v['cwd'], root, scratch, '/unused', mapping=mapping, check=check,storage=storage)
            rewritten = [(local, target.relative_to('/unused').as_posix(), n.rollout_id, n.owner) for (local,target,_),n in zip(paths,nodes)]
        else:
            if set(mapping['rollouts']) != {v['source_id']} or set(mapping['owners']) != {v['source_id']}:
                raise execution.ExecutionError('execution_plan_conflict')
            if manifest['version'] == 3:
                if len(entry['files']) != 1:
                    raise execution.ExecutionError('execution_codex_companions_unsupported')
                member = entry['primary']
            else:
                member = entry['file']
            stamp = row['rewrite_at'].replace(':','-')[:-1]
            path = 'sessions/' + stamp[:4] + '/' + stamp[5:7] + '/' + stamp[8:10] + '/rollout-' + stamp + '-' + root + '.jsonl'
            local = scratch/'singleton.jsonl'
            metadata = False
            with archive.open(member) as src, storage.open(local) as dst:
                length=0
                for record in read_records(src):
                    check()
                    if record.get('type') == 'session_meta':
                        payload = record.get('payload')
                        if metadata or not isinstance(payload,dict) or payload.get('id') != v['source_id'] or payload.get('history_base'):
                            raise execution.ExecutionError('execution_native_primary_invalid')
                        metadata = True
                    record = rewrite_import_record(record, root, entry, v['cwd'], row['rewrite_at'])
                    if record.get('type') == 'session_meta':
                        record['csl_source_session_id'] = v['source_id']
                    body = memoryview(json_line(record))
                    length+=len(body)
                    if length>history.MAX_FILE:
                        raise execution.ExecutionError('execution_member_limit')
                    while body:
                        check()
                        n = dst.write(body)
                        if not n:
                            raise Pending('execution_rewrite_short_write')
                        body = body[n:]
                check()
            if not metadata:
                raise execution.ExecutionError('execution_native_primary_invalid')
            rewritten = [(local,path,v['source_id'],v['source_id'])]
        members = []
        for i,(local,path,rollout,owner) in enumerate(rewritten):
            size, sha = _hash_file(local,check)
            src=os.open(local,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK)
            try:s=durable.regular(src)
            finally:os.close(src)
            members.append(dict(path=path, role='root' if i==len(rewritten)-1 else 'ancestor', source_rollout=rollout, source_owner=owner,
                                native_id=mapping['rollouts'][rollout], owner_id=mapping['owners'][owner], size=size, sha256=sha,
                                keeper='.csl-'+v['import_key']+'-'+str(i), identity=None, phase='planned',
                                scratch_path=local.relative_to(scratch).as_posix(),scratch_identity={'device':str(s.st_dev),'inode':str(s.st_ino)}))
        _sync_workspace(scratch,check)
        check()
        return rewritten, members


def _sync_workspace(path,check):
    # Only a newly acknowledged EMPTY workspace is ever populated. A killed
    # partial rewrite is accounted and left Pending, never adopted or removed.
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:
        for root,dirs,files,parent in os.fwalk('.',topdown=False,follow_symlinks=False,dir_fd=fd):
            for name in dirs+files:
                check()
                item=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=parent)
                try:
                    if name in files:durable.regular(item)
                    else:_scratch_directory(item)
                    _effect(check,os.fsync,item)
                finally:os.close(item)
        _effect(check,os.fsync,fd)
    finally:os.close(fd)


def _scratch_directory(fd):
    # Path.mkdir(parents=True) uses0755 for intermediate directories inside our
    # exact0700 workspace. Do not relax the public private-root checker.
    s=os.fstat(fd)
    if not stat.S_ISDIR(s.st_mode) or s.st_uid!=os.getuid() or stat.S_IMODE(s.st_mode)!=0o700:
        raise Pending('execution_workspace_directory_changed')


@contextmanager
def _source(workspace,member,check):
    parts=PurePosixPath(member['scratch_path']).parts
    parent=os.dup(workspace)
    try:
        for part in parts[:-1]:
            check()
            nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
            os.close(parent);parent=nxt
            _scratch_directory(parent)
        expected=dict(identity=member['scratch_identity'],size=member['size'],sha256=member['sha256'])
        _file(parent,parts[-1],expected,check,(1,))
        fd=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=parent)
        try:
            if not _same(durable.regular(fd),member['scratch_identity']):
                raise Pending('execution_workspace_member_changed')
            with os.fdopen(fd,'rb',buffering=0) as stream:
                fd=-1
                yield stream
        finally:
            if fd>=0:os.close(fd)
    finally:os.close(parent)


def _verify_installed(home, row, check):
    for m in row['members']:
        with _parent(home,m['path'],check) as (parent,name):
            target = _file(parent,name,m,check,(1,) if m['phase']=='retired' else (1,2))
            try:
                keeper = os.stat(m['keeper'],dir_fd=parent,follow_symlinks=False)
            except FileNotFoundError:
                if m['phase'] not in ('retiring','retired') or target.st_nlink != 1:
                    raise Pending('execution_keeper_missing') from None
            else:
                if m['phase']=='retired' or not _same(keeper,m['identity']) or target.st_nlink != 2:
                    raise Pending('execution_keeper_conflict')
                _file(parent,m['keeper'],m,check,(2,))
    check()


def lookup(journal, home, req, check, *, roots):
    """Read-only. Never creates a lock, stage, destination, receipt or new IDs."""
    req = execution.request(req.encoded)
    guard = _checked(journal,home,req,roots,check)
    with _Custody(journal,req) as custody:
        row = _load(journal,req,guard,custody)
        for attempt in range(3):
            if row is None:
                guard()
                return {'state':'not_prepared','receipt':None}
            try:
                if row['v']==3:
                    if row['roots'] != roots:
                        raise execution.ExecutionError('execution_root_changed')
                    if row['receipt'] is not None:
                        _verify_installed(home,row,guard)
                _current(journal,req,row,guard)
                guard()
                return {'state':row['state'],'receipt':row.get('receipt')}
            except (Pending,FileNotFoundError,durable.DurableError) as failure:
                guard()  # Same current read attempt, never a renewed budget.
                next_row=_load(journal,req,guard,custody)
                if not _read_progress(row,next_row):
                    raise failure
                row=next_row
        raise Pending('execution_lookup_advancing')


def _read_progress(old,new):
    """Bounded metadata-only progress, including before a receipt exists."""
    if new is None or old['v']!=3 or new['v']!=3 or new['revision']<=old['revision']:
        return False
    for field in ('intent','roots','rewrite_at'):
        if canonical(old[field])!=canonical(new[field]):return False
    if STATES.index(new['state'])<STATES.index(old['state']):return False
    if old['plan'] is not None and canonical(old['plan'])!=canonical(new['plan']):return False
    if old['receipt'] is not None and canonical(old['receipt'])!=canonical(new['receipt']):return False
    if old['members']:
        if len(old['members'])!=len(new['members']):return False
        for prior,current in zip(old['members'],new['members']):
            if {k:v for k,v in prior.items() if k not in ('phase','identity')}!={k:v for k,v in current.items() if k not in ('phase','identity')} or PHASES.index(current['phase'])<PHASES.index(prior['phase']):return False
            if prior['identity'] is not None and prior['identity']!=current['identity']:return False
    for field in ('planning','workspace'):
        a,b=old[field],new[field]
        if a is not None:
            if b is None or a['name']!=b['name'] or a['identity'] is not None and a['identity']!=b['identity']:return False
    return True


def publish(journal, home, req, captured, check, *, roots, fault=None):
    """Actual private publication; borrowed FDs, required original authority.

    fault is a test observation hook AFTER real operations, never an authority.
    The caller must retain custody after any raised/unknown dispatched outcome.
    """
    req = execution.request(req.encoded)
    if req.value['operation'] != 'import' or req.value['provider'] != 'codex':
        raise execution.ExecutionError('execution_publication_unsupported')
    guard = _checked(journal,home,req,roots,check)
    fault = fault or (lambda point: None)
    with durable.locked(journal,guard) as lock_check, _Custody(journal,req) as custody:
        guard.bindings.extend((lock_check,custody.check))
        with _final_check(guard):
            return _publish_locked(journal,home,req,captured,roots,guard,fault,custody)


def _publish_locked(journal,home,req,captured,roots,guard,fault,custody):
    row = _load(journal,req,guard,custody)
    if row is not None and row['intent'] != execution._intent_key(req):
        raise execution.ExecutionError('execution_original_bound_changed')
    if row is None or row['v']==1:
        prior=row
        if row is None:
            try:os.stat('.'+req.value['import_key']+'.1.tmp',dir_fd=journal,follow_symlinks=False)
            except FileNotFoundError:pass
            else:raise Pending('execution_metadata_staging_unknown')
        row=_Row(dict(v=3,revision=1 if prior is None else prior['revision']+1,
            intent=execution._intent_key(req),plan=None if prior is None else prior['plan'],
            state='planning' if prior is None else 'reserved',roots=roots,
            rewrite_at=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            planning=None,workspace=None,members=[],receipt=None),custody)
        _validate(row,req);_save(journal,req,prior,row,guard,fault)
    if row['state']=='planning':
        with _Scratch(journal,req,custody,'planning',guard,fault) as storage:
            storage.clear()
            prepared=execution.plan(captured,req,guard,journal,graph_stage=storage.path,storage=storage)
            _sync_workspace(storage.path,guard)
            fault('planning_fsynced')
            row=_advance(journal,req,custody.current,guard,fault,state='reserved',plan=prepared)
            fault('reserved')
    if row['roots'] != roots:
        raise execution.ExecutionError('execution_root_changed')
    if row['receipt'] is None:
        # Rewriting is private/volatile; published files are never parser input.
        guard()
        with _Scratch(journal,req,custody,'workspace',guard,fault) as storage:
            workspace,scratch=storage.fd,storage.path
            if row['state']=='reserved':
                storage.clear()
                rewritten,members = _rewrite(captured,req,custody.current,scratch,guard,storage)
                fault('workspace_rewritten')
                row = _advance(journal,req,custody.current,guard,fault,state='preparing',members=members)
            if row['state']=='preparing':
                for i in range(len(row['members'])):
                    m = row['members'][i]
                    with _parent(home,m['path'],guard,create=True) as (parent,name):
                        if m['phase']=='planned':
                            row = _member(journal,req,row,i,guard,fault,phase='allocating')
                            m = row['members'][i]
                        if m['phase']=='allocating':
                            _dispatch(parent,guard)
                            try:
                                os.stat(m['keeper'],dir_fd=parent,follow_symlinks=False)
                            except FileNotFoundError:
                                pass
                            else:
                                raise Pending('execution_keeper_uncommitted')
                            try:
                                dst = _open(guard,m['keeper'],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=parent)
                            except FileExistsError:
                                raise Pending('execution_keeper_uncommitted') from None
                            try:
                                s = durable.regular(dst)
                                fault('keeper_created_'+str(i))
                                guard()
                                _effect(guard,os.fsync,parent)
                                row = _member(journal,req,row,i,guard,fault,phase='writing',identity={'device':str(s.st_dev),'inode':str(s.st_ino)})
                            finally:
                                os.close(dst)
                            m = row['members'][i]
                        if m['phase']=='allocating':
                            raise Pending('execution_keeper_uncommitted')
                        if m['phase']=='writing':
                            _dispatch(parent,guard)
                            dst = os.open(m['keeper'],os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=parent)
                            try:
                                s = durable.regular(dst)
                                if not _same(s,m['identity']):
                                    raise Pending('execution_keeper_conflict')
                                guard()
                                _effect(guard,os.ftruncate,dst,0)
                                with _source(workspace,m,guard) as src:
                                    while True:
                                        guard()
                                        chunk = src.read(1024*1024)
                                        guard()
                                        if not chunk:break
                                        view=memoryview(chunk)
                                        while view:
                                            guard()
                                            n=_effect(guard,os.write,dst,view)
                                            if n<=0:raise Pending('execution_keeper_short_write')
                                            view=view[n:]
                                guard()
                                _effect(guard,os.fsync,dst)
                                fault('keeper_fsynced_'+str(i))
                            finally:
                                os.close(dst)
                            _file(parent,m['keeper'],m,guard,(1,))
                            row = _member(journal,req,row,i,guard,fault,phase='kept')
                        else:
                            _file(parent,m['keeper'],m,guard,(1,))
                row = _advance(journal,req,row,guard,fault,state='prepared')
                fault('prepared')
            # Every keeper is qualified BEFORE the first target publication.
            if row['state']=='prepared':
                for m in row['members']:
                    with _parent(home,m['path'],guard) as (parent,name):
                        _file(parent,m['keeper'],m,guard,(1,))
                row = _advance(journal,req,row,guard,fault,state='publishing')
            if row['state']=='publishing':
                for i in range(len(row['members'])):
                    m=row['members'][i]
                    with _parent(home,m['path'],guard) as (parent,name):
                        if m['phase']=='kept':
                            row=_member(journal,req,row,i,guard,fault,phase='publishing')
                            m=row['members'][i]
                        if m['phase']=='publishing':
                            try:
                                os.stat(name,dir_fd=parent,follow_symlinks=False)
                            except FileNotFoundError:
                                _file(parent,m['keeper'],m,guard,(1,))
                                _dispatch(parent,guard)
                                _effect(guard,os.link,m['keeper'],name,src_dir_fd=parent,dst_dir_fd=parent,follow_symlinks=False)
                                fault('linked_'+str(i))
                            _file(parent,name,m,guard,(2,))
                            _file(parent,m['keeper'],m,guard,(2,))
                            guard()
                            _effect(guard,os.fsync,parent)
                            fault('linked_fsynced_'+str(i))
                            row=_member(journal,req,row,i,guard,fault,phase='published')
                        else:
                            _file(parent,name,m,guard,(2,))
                            _file(parent,m['keeper'],m,guard,(2,))
                _verify_installed(home,row,guard)
                row=_advance(journal,req,row,guard,fault,state='receipted',receipt=_receipt(row))
                fault('receipted')
    _verify_installed(home,row,guard)
    if row['state']!='complete':
        if row['state']=='receipted':
            row=_advance(journal,req,row,guard,fault,state='retiring_keepers')
        for i in range(len(row['members'])):
            m=row['members'][i]
            with _parent(home,m['path'],guard) as (parent,name):
                if m['phase']=='published':
                    row=_member(journal,req,row,i,guard,fault,phase='retiring')
                    m=row['members'][i]
                if m['phase']=='retiring':
                    try:
                        os.stat(m['keeper'],dir_fd=parent,follow_symlinks=False)
                    except FileNotFoundError:
                        _file(parent,name,m,guard,(1,))
                    else:
                        _file(parent,name,m,guard,(2,))
                        _file(parent,m['keeper'],m,guard,(2,))
                        _dispatch(parent,guard)
                        _effect(guard,os.unlink,m['keeper'],dir_fd=parent)
                        fault('keeper_unlinked_'+str(i))
                    guard()
                    _effect(guard,os.fsync,parent)
                    row=_member(journal,req,row,i,guard,fault,phase='retired')
        row=_advance(journal,req,row,guard,fault,state='complete')
        fault('provider_complete')
    for field in ('planning','workspace'):
        if custody.current[field] is not None and custody.current[field]['phase']!='retired':
            with _Scratch(journal,req,custody,field,guard,fault) as storage:
                storage.cleanup()
    row=custody.current
    _verify_installed(home,row,guard)
    _current(journal,req,row,guard)
    guard()
    return row['receipt']
