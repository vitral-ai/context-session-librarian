"""Source-only synthetic FD9 evidence runner; explicitly label executor host."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import zipfile


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--go',required=True)
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--host-role',choices=('workstation','guest'),required=True)
    args=parser.parse_args()
    repo=Path(__file__).resolve().parents[3]
    args.out.mkdir(mode=0o700,parents=True,exist_ok=True)
    if args.out.is_symlink() or args.out.stat().st_mode&0o777!=0o700:
        raise SystemExit('evidence directory must be owned private0700')
    if args.out.stat().st_uid!=os.getuid():raise SystemExit('evidence directory owner mismatch')
    go=Path(args.go).resolve(strict=True)
    version=subprocess.check_output([str(go),'version'],text=True).strip()
    if 'go1.27.4 ' not in version:raise SystemExit('requires retained Go1.27.4')
    sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
    files=sorted([p for p in repo.glob('*.py')]+[p for p in (repo/'tests/fixtures/execution_fd9').iterdir() if p.suffix in ('.py','.go','.json')])
    record=dict(host_role=args.host_role,host_role_supplied_by_executor=True,hostname=socket.gethostname(),
                platform=platform.uname()._asdict(),python=sys.version,python_executable=sys.executable,
                python_executable_sha256=sha(Path(sys.executable).resolve()),
                go_executable=str(go),go_version=version,go_sha256=sha(go),
                source_files={str(p.relative_to(repo)):sha(p) for p in files},scenarios={})
    def receipt():(args.out/'provenance.json').write_text(json.dumps(record,sort_keys=True,indent=2)+'\n')
    receipt()
    sid='11111111-1111-4111-8111-111111111111'
    body=(json.dumps({'type':'session_meta','payload':{'id':sid,'cwd':'/synthetic'}})+'\n').encode()
    manifest=dict(format='codex-session-librarian',version=1,mode='raw',sessions=[dict(id=sid,provider='codex',title='synthetic',file='sessions/'+sid+'.jsonl',bytes=len(body),sha256=hashlib.sha256(body).hexdigest())])
    stream=io.BytesIO()
    with zipfile.ZipFile(stream,'w') as archive:
        archive.writestr('manifest.json',json.dumps(manifest));archive.writestr('sessions/'+sid+'.jsonl',body)
    with tempfile.TemporaryDirectory(prefix='.fd9-evidence-',dir=Path.home()) as temporary:
        root=Path(temporary);root.chmod(0o700);binary=root/'parent'
        build_env=dict(PATH=str(go.parent)+':'+os.environ.get('PATH',''),HOME=str(Path.home()),LANG='C.UTF-8')
        build=subprocess.run([str(go),'build','-o',str(binary),str(repo/'tests/fixtures/execution_fd9/parent.go')],env=build_env,capture_output=True,timeout=90)
        (args.out/'build.stdout').write_bytes(build.stdout);(args.out/'build.stderr').write_bytes(build.stderr)
        if build.returncode:raise SystemExit('Go fixture build failed; see raw stderr')
        record['fixture_binary_sha256']=sha(binary)
        record['build_env_keys']=sorted(build_env)
        receipt()
        for scenario in ('publisher','death-publisher-effect'):
            case=root/scenario;case.mkdir(mode=0o700)
            (case/'publisher.zip').write_bytes(stream.getvalue())
            for name in ('h','c','a','t'):(case/name).mkdir(mode=0o700)
            env=dict(PATH=str(go.parent)+':'+os.environ.get('PATH',''),HOME=str(case/'h'),CODEX_HOME=str(case/'c'),CLAUDE_CONFIG_DIR=str(case/'a'),TMPDIR=str(case/'t'),PYTHONDONTWRITEBYTECODE='1',LANG='C.UTF-8')
            command=[str(binary),sys.executable,str(repo/'tests/fixtures/execution_fd9/publisher.py'),str(repo),str(case),scenario]
            result=subprocess.run(command,env=env,capture_output=True,timeout=20)
            (args.out/(scenario+'.stdout')).write_bytes(result.stdout)
            (args.out/(scenario+'.stderr')).write_bytes(result.stderr)
            record['scenarios'][scenario]=dict(command=command,exit_code=result.returncode,stdout_sha256=hashlib.sha256(result.stdout).hexdigest(),stderr_sha256=hashlib.sha256(result.stderr).hexdigest())
            record['scenarios'][scenario]['env_keys']=sorted(env)
            receipt()
            if result.returncode:raise SystemExit('actual Go fixture failed; see raw output')
            value=json.loads(result.stdout)
            if scenario=='publisher':
                assert value['state']=='imported' and value['validation']=='native_unverified'
                assert len(list((case/'codex').rglob('*.jsonl')))==1
                assert not list(case.glob('.planning-*')) and not list(case.glob('.publisher-*'))
            else:
                assert value=={'effect':False,'reaped':True,'signal':9}
                assert not list((case/'codex').rglob('*.jsonl'))
    receipt()
    print(json.dumps(dict(host_role=args.host_role,scenarios=record['scenarios']),sort_keys=True))


if __name__=='__main__':main()
