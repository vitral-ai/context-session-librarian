"""Portable light enrollment. Pool lifecycle/authentication remain external hooks."""
import argparse
import base64
import fcntl
import json
import hashlib
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import time
import uuid

from csl_bridge_store import Store, encode, identifier, name

OPS = ['agents', 'followup', 'mail', 'receipt', 'send_message', 'status', 'watch']


def atomic(path, data):
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.pending')
    with temporary.open('xb') as stream:
        os.chmod(temporary, 0o600)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def install_service(home, source):
    """Own user unit only. Never creates or restarts a native runtime."""
    command = shlex.join([sys.executable, '-I', '-B', str(source / 'csl_light.py'), '--home', str(home), 'run'])
    unit = ('[Unit]\nDescription=CSL persistent light transport\n[Service]\nType=simple\nExecStart=' +
            command.replace('%', '%%').replace('$', '$$') + '\nRestart=always\nRestartSec=1\n'
            'KillMode=control-group\nTimeoutStopSec=5\nUMask=0077\n[Install]\nWantedBy=default.target\n')
    atomic(Path.home() / '.config/systemd/user/csl-light.service', unit.encode())
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True)
    subprocess.run(['systemctl', '--user', 'enable', '--now', 'csl-light.service'], check=True)


def initialize(home, node, bridge_homes, agents, router=False, service=False):
    from csl_light import private_dir
    home = private_dir(Path(home).expanduser().absolute())
    name(node)
    if (home / 'config.json').exists():
        raise ValueError('Light already initialized; edit its links to enroll peers')
    homes = [bridge_homes] if isinstance(bridge_homes, (str, Path)) else bridge_homes
    bridges, found = {}, set()
    for bridge_home in homes:
        bridge_home = Path(bridge_home).expanduser().absolute()
        store = Store(bridge_home)
        try:
            selected = sorted(set(agents) & set(store.agents()))
            if selected:
                if set(selected) & found or store.setting('identity') in bridges:
                    raise ValueError('Bridge identities and agent names must be unique')
                bridges[store.setting('identity')] = {'home': str(bridge_home), 'agents': selected}
                found.update(selected)
        finally:
            store.close()
    if found != set(agents):
        raise ValueError('Every selected agent must already be registered')
    config = {'node': node, 'home': str(home), 'csl_source': str(Path(__file__).resolve().parent),
              'default_agent': agents[0], 'router': router, 'bridges': bridges, 'links': {}}
    atomic(home / 'config.json', (encode(config) + '\n').encode())
    if service:
        install_service(home, Path(__file__).resolve().parent)
    return {'configured': str(home), 'node': node, 'service': service}


def check_brief(agent, registered, marker, expected_thread=None):
    if agent not in registered or marker.get('agent') != agent or marker.get('thread') != registered[agent]:
        raise ValueError('First brief must match an already registered real agent thread')
    identifier(registered[agent])
    if expected_thread and expected_thread != registered[agent]:
        raise ValueError('Registered thread changed')
    return registered[agent]


# Executed only through the owner-selected installation channel, never a wire opcode.
REMOTE_INSTALL = r'''
import base64,json,os,sys,shlex,subprocess,time
from pathlib import Path
os.umask(0o077)
p=json.load(sys.stdin)
home=Path(p['light_home']).expanduser().absolute()
source=home/'source'
source.mkdir(mode=0o700,parents=True,exist_ok=True)
for filename,content in p['files'].items():
 if Path(filename).name!=filename or not filename.endswith('.py'):raise ValueError('Source name')
 target=source/filename
 target.write_bytes(base64.b64decode(content))
 target.chmod(0o600)
sys.path.insert(0,str(source))
from csl_bridge_store import Store,encode
from enroll_light import check_brief,atomic,install_service,OPS
bridge=Path(p['bridge_home']).expanduser().absolute()
store=Store(bridge)
try:
 registered=store.agents()
 marker=json.loads(Path(p['first_brief_marker']).expanduser().read_text())
 thread=check_brief(p['agent'],registered,marker,p.get('expected_thread'))
 identity=store.setting('identity')
 rows={r['name']:r['config']for r in store.rows('SELECT name,config FROM peers')}
 snapshot=home/('enrollment-before-'+str(time.time_ns())+'.json')
 atomic(snapshot,encode({'peers':rows,'config':(home/'config.json').read_text()if(home/'config.json').exists()else None}).encode())
 config={'node':p['peer'],'home':str(home),'csl_source':str(source),'default_agent':p['agent'],
  'router':False,'upstream':p['host_node'],'proxy_executable':p.get('proxy_executable'),
  'bridges':{identity:{'home':str(bridge),'agents':[p['agent']]}},
  'links':{p['host_node']:{'agents':p['host_agents'],'trusted_router':True}}}
 atomic(home/'config.json',(encode(config)+'\n').encode())
 marker_key=home/'keys/workstation.pub'
 atomic(marker_key,(p['public_key']+'\n').encode())
 attach=shlex.join([sys.executable,'-I','-B',str(source/'csl_light.py'),'--home',str(home),'attach',
  '--peer',p['host_node'],'--key-file',str(marker_key)])
 if p['authorize_key']:
  public=p['public_key'].split()
  if len(public)<2 or public[0]not in('ssh-ed25519','ssh-rsa')or '\n'in p['public_key']:raise ValueError('Public key')
  keyline='restrict,command="'+attach.replace('\\','\\\\').replace('"','\\"')+'" '+' '.join(public[:2])+' csl-light-'+p['peer']
  authorized=Path.home()/'.ssh/authorized_keys'
  prior=authorized.read_text()if authorized.exists()else''
  kept=[line for line in prior.splitlines() if not line.endswith(' csl-light-'+p['peer'])]
  atomic(authorized,('\n'.join(kept+[keyline])+'\n').encode())
 with store.transaction()as db:
  for origin,actors in p['host_bridges'].items():
   row={'transport':'light','home':str(home),'peer':p['host_node'],'allow_agents':[p['agent']],
    'allow_sources':actors,'allow_ops':OPS,'send_session':True}
   db.execute('INSERT INTO peers VALUES(?,?) ON CONFLICT(name)DO UPDATE SET config=excluded.config',(origin,encode(row)))
 launcher=Path.home()/'.local/bin/csl-light'
 atomic(launcher,('#!/bin/sh\nexec '+shlex.join([sys.executable,'-I','-B',str(source/'csl_light_cli.py'),'--home',str(home)])+' "$@"\n').encode())
 launcher.chmod(0o700)
 if p['service']:install_service(home,source)
 print(encode({'peer':p['peer'],'agent':p['agent'],'thread':thread,'light_home':str(home),
  'attach_command':attach,'snapshot':str(snapshot),'identity':identity}))
finally:store.close()
'''


def adopt_peers(config, peer, agent, audit=None):
    """Atomic row adoption per home; callers back up and restore exact prior rows."""
    before = {}
    try:
        for identity, bridge in config['bridges'].items():
            store = Store(bridge['home'])
            try:
                value = {'transport': 'light', 'home': config['home'], 'peer': peer,
                         'allow_agents': bridge['agents'], 'allow_sources': [agent],
                         'allow_ops': OPS, 'send_session': True}
                with store.transaction() as db:
                    row = db.execute('SELECT config FROM peers WHERE name=?', (peer,)).fetchone()
                    prior = row[0] if row else None
                    record = {'prior': prior, 'installed': encode(value)}
                    # Persist the exact rollback row before its transaction commits.
                    if audit is not None:
                        atomic(Path(audit) / (identity + '-peer-before.json'), encode(record).encode())
                    db.execute('INSERT INTO peers VALUES(?,?) ON CONFLICT(name)DO UPDATE SET config=excluded.config', (peer, encode(value)))
                before[identity] = record
            finally:
                store.close()
    except BaseException:
        restore_peers(config, peer, before)
        raise
    return before


def restore_peers(config, peer, before):
    for identity, record in before.items():
        prior, installed = record['prior'], record['installed']
        store = Store(config['bridges'][identity]['home'])
        try:
            with store.transaction() as db:
                if prior is None:
                    changed = db.execute('DELETE FROM peers WHERE name=? AND config=?', (peer, installed)).rowcount
                else:
                    changed = db.execute('UPDATE peers SET config=? WHERE name=? AND config=?', (prior, peer, installed)).rowcount
                if changed != 1:
                    raise ValueError('Peer changed concurrently; exact rollback refused')
        finally:
            store.close()


def command(value):
    argv = json.loads(value)
    if not isinstance(argv, list) or not argv or any(not isinstance(x, str) or not x for x in argv):
        raise argparse.ArgumentTypeError('Supply a nonempty JSON argv array')
    return argv


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', type=Path, default=Path.home() / '.local/share/csl-light')
    parser.add_argument('peer', type=name)
    parser.add_argument('--agent', required=True, type=name)
    channel = parser.add_mutually_exclusive_group(required=True)
    channel.add_argument('--ssh-alias')
    channel.add_argument('--bootstrap-command', type=command, help='Owner installer hook: JSON {script,payload} stdin, install proof stdout')
    parser.add_argument('--connect-command', type=command, help='Owner stream hook argv; {attach} expands to fixed receiver command')
    parser.add_argument('--guest-bridge-home', required=True)
    parser.add_argument('--guest-light-home', default='~/.local/share/csl-light')
    parser.add_argument('--first-brief-marker', required=True)
    parser.add_argument('--thread')
    parser.add_argument('--key', type=Path)
    parser.add_argument('--known-hosts', type=Path)
    parser.add_argument('--proxy-executable')
    parser.add_argument('--no-services', action='store_true', help='Install config/source for a separately managed foreground broker')
    args = parser.parse_args(argv)
    home = args.home.expanduser().absolute()
    config = json.loads((home / 'config.json').read_text())
    if not config.get('router'):
        raise ValueError('Enroll pool peers on the router')
    reserved = {a for b in config['bridges'].values() for a in b['agents']}
    reserved.update(a for peer,b in config['links'].items() if peer != args.peer for a in b.get('agents',[]))
    if args.agent in reserved:
        raise ValueError('Agent name already belongs to another endpoint')
    existing = config['links'].get(args.peer)
    if existing and existing.get('agents') != [args.agent]:
        raise ValueError('Existing link belongs to another agent')
    audit = home / 'enrollments' / (args.peer + '-' + uuid.uuid4().hex)
    audit.mkdir(mode=0o700, parents=True)
    key = (args.key or home / 'keys' / args.peer).expanduser().absolute()
    key.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not key.exists():
        subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)], check=True)
    source = Path(__file__).resolve().parent
    selected = list(source.glob('csl_bridge*.py')) + [source / n for n in ('csl_light.py', 'csl_light_cli.py', 'enroll_light.py', 'csl_secrets.py')]
    payload = {'peer': args.peer, 'agent': args.agent, 'expected_thread': args.thread,
               'bridge_home': args.guest_bridge_home, 'light_home': args.guest_light_home,
               'first_brief_marker': args.first_brief_marker, 'public_key': key.with_suffix('.pub').read_text().strip(),
               'host_node': config['node'], 'host_agents': sorted({a for b in config['bridges'].values() for a in b['agents']}),
               'host_bridges': {n: b['agents'] for n, b in config['bridges'].items()},
               'authorize_key': not bool(args.connect_command), 'proxy_executable': args.proxy_executable,
               'service': not args.no_services, 'files': {p.name: base64.b64encode(p.read_bytes()).decode() for p in selected}}
    atomic(audit / 'request.json', encode({**payload, 'files': {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in selected}}).encode())
    if args.ssh_alias:
        from csl_bridge_transport import ssh_argv
        install_argv = ssh_argv(args.ssh_alias)[:-1] + ['python3 -I -B -c ' + shlex.quote(REMOTE_INSTALL)]
        data = encode(payload).encode()
    else:
        install_argv = args.bootstrap_command
        data = encode({'script': REMOTE_INSTALL, 'payload': payload}).encode()
    response = subprocess.run(install_argv, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120, check=False)
    if response.returncode:
        raise RuntimeError('Owner installation failed; no host configuration changed')
    installed = json.loads(response.stdout)
    if installed['peer'] != args.peer or installed['agent'] != args.agent:
        raise ValueError('Installation identity differs')
    identifier(installed['thread'])
    atomic(audit / 'installed.json', encode(installed).encode())
    link = {'endpoint': args.ssh_alias or args.peer, 'key': str(key), 'agents': [args.agent],
            'agent_threads': {args.agent: installed['thread']}}
    if args.known_hosts:
        link['known_hosts'] = str(args.known_hosts.expanduser().absolute())
    if args.connect_command:
        link['connect_command'] = [part.replace('{attach}', installed['attach_command']) for part in args.connect_command]
    with (home / 'enrollment.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        before = (home / 'config.json').read_bytes()
        fresh = json.loads(before)
        if fresh['links'].get(args.peer) != existing:
            raise ValueError('Link changed during enrollment')
        atomic(audit / 'config-before.json', before)
        fresh['links'][args.peer] = link
        rows = adopt_peers(fresh, args.peer, args.agent, audit)
        try:
            atomic(home / 'config.json', (encode(fresh) + '\n').encode())
        except BaseException:
            restore_peers(fresh, args.peer, rows)
            raise
    (home / ('revoked-' + args.peer)).unlink(missing_ok=True)
    print(encode({'configured': args.peer, 'agent': args.agent, 'thread': installed['thread'],
                  'audit': str(audit), 'existing_streams': 'unchanged'}))


if __name__ == '__main__':
    main()
