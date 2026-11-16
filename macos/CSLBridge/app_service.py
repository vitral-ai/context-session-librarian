"""Local app backend. No model starts, native servers, login or network setup."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shlex
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid

RESOURCE = Path(__file__).resolve().parent
SOURCE = RESOURCE / 'bridge'
if not SOURCE.exists():
    SOURCE = RESOURCE.parents[1]
sys.path.insert(0, str(SOURCE))
from csl_bridge_store import Store, BridgeError, encode, decode, name, identifier
from csl_bridge_transport import local_call, exchange
from csl_bridge_rpc import Runtime, RuntimeUnavailable, RuntimeRejected
from csl_bridge_mcp import MCP_TOOL_TIMEOUT


class AppError(ValueError):
    pass


def json_file(path, value):
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix='.csl-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


class Service:
    def __init__(self, state_root=None, bridge_home=None, codex_home=None):
        self.state = Path(state_root or Path.home() / 'Library/Application Support/CSLBridge')
        self.settings_path = self.state / 'connection.json'
        self.settings = json.loads(self.settings_path.read_text()) if self.settings_path.exists() else {}
        existing = Path.home() / '.cslb-mac'
        default = existing if (existing / 'bridge.sqlite').exists() else Path.home() / '.cslb-app'
        self.home = Path(bridge_home or self.settings.get('bridge_home') or default)
        self.codex_home = Path(codex_home or self.settings.get('codex_home') or os.environ.get('CODEX_HOME', str(Path.home()/'.codex'))).expanduser()

    def endpoint(self):
        path = self.codex_home / 'app-server-control/app-server-control.sock'
        try:
            resolved = path.resolve(strict=True)
            info = resolved.stat()
        except OSError:
            raise AppError('No encontramos un runtime local. Abre tu conversación existente en Codex y vuelve a detectar.') from None
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise AppError('El endpoint no pertenece a este usuario o no es privado.')
        if sys.platform == 'darwin' and len(os.fsencode(str(resolved))) >= 104:
            raise AppError('La ruta del socket es demasiado larga para macOS. El dueño del runtime debe resolverla.')
        return resolved

    def registered(self):
        if not (self.home/'bridge.sqlite').exists():
            return {}
        store = Store(self.home)
        try:
            return store.agents()
        finally:
            store.close()

    def discover(self):
        path = self.endpoint()
        runtime = Runtime(path)
        try:
            loaded, cursor = [], None
            for _ in range(10):
                page = runtime.call('thread/loaded/list', {'limit':100, 'cursor':cursor})
                loaded.extend(page['data']); cursor = page.get('nextCursor')
                if not cursor: break
        finally:
            runtime.close()
        registered = self.registered()
        sessions = [{'id': value, 'label': ('Agente Mac conectado' if value in registered.values() else 'Sesión cargada')+' · '+value[-12:]} for value in loaded]
        selected = self.settings.get('thread_id') or next(iter(registered.values()), None)
        return {'sessions':sessions, 'selected':selected, 'socket':str(path), 'complete':not bool(cursor),
                'note':'Sólo conversaciones ya cargadas. No se lee historia ni se crea otra sesión.'}

    def owned(self):
        path = self.state/'owned-process.json'
        if not path.exists(): return None
        row = json.loads(path.read_text())
        if row.get('bridge_home') != str(self.home): return None
        try:
            result = subprocess.run(['ps','-p',str(row['pid']),'-o','args='],capture_output=True,text=True,timeout=3)
        except (OSError,subprocess.TimeoutExpired): return None
        return row if result.returncode==0 and str(self.home) in result.stdout and 'bridge' in result.stdout and 'run' in result.stdout else None

    def status(self, verify_peer=False):
        if verify_peer and self.registered():
            try:self.peer_check()
            except (AppError,BridgeError,OSError,ValueError):
                json_file(self.state/'peer-proof.json',{'ready':False,'phase':'peer_unreachable','observed_at':time.time(),
                    'note':'No hay respuesta autenticada del peer. Se conserva la tarea/ID; no se reenvía trabajo.'})
        peer={'ready':False,'phase':'not_verified','note':'Sin respuesta autenticada reciente del peer.'}
        proof=self.state/'peer-proof.json'
        if proof.exists():
            saved=json.loads(proof.read_text());age=time.time()-saved.get('observed_at',0)
            if 0<=age<=45:
                peer={**saved,'age_seconds':round(age,1)}
        channel=None
        if self.settings.get('connection_mode')=='relay':
            path=self.state/'relay/relay-status.json'
            channel=json.loads(path.read_text()) if path.exists() else {'phase':'not_started'}
            if channel.get('phase')!='relay_connected_peer_unverified' or peer.get('observed_at',0)<channel.get('connected_at',0):
                peer={'ready':False,'phase':'relay_peer_unverified','note':'Relay disconnected or rejoined; an authenticated peer response is required again.'}
        try:
            data = local_call(self.home, {'local_op':'agents'}, timeout=3)
            agents = data['agents']; active = any(x.get('status')=='active' for x in agents)
            connected=bool(agents) and all(x.get('status')!='disconnected' for x in agents)
            ready=connected and peer['ready']
            return {'connected':ready,'runtime_ready':connected,'connector_running':True,'peer':peer,
                    'channel':channel,'task_phase':self.task_phase(),'headline':('Listo · turno activo' if active else 'Listo · intercambio verificado') if ready else (('Sesión lista · enlace remoto no responde' if peer.get('phase')=='peer_unreachable' else 'Sesión disponible · peer sin verificar') if connected else 'Conector esperando al runtime'),
                    'agents':agents,'owned':bool(self.owned()),'adopted':not bool(self.owned()),
                    'note':peer.get('note','La conversación original sigue en su runtime.') if not ready else 'Respuesta autenticada reciente; la conversación original sigue en su runtime.'}
        except (BridgeError,OSError):
            return {'connected':False,'runtime_ready':False,'connector_running':False,'peer':peer,'headline':'Sin conexión local','agents':[], 'owned':False,
                    'note':'Selecciona una sesión existente para iniciar el conector.'}

    def configure(self, thread, coordinator='manunruks'):
        identifier(thread)
        if coordinator is not None:name(coordinator)
        discovery = self.discover()
        if thread not in [x['id'] for x in discovery['sessions']]:
            raise AppError('La sesión ya no está cargada. Vuelve a detectar; no iniciaremos otra.')
        registered = self.registered()
        if registered and list(registered.values()) != [thread]:
            raise AppError('Este conector pertenece a otra sesión. Conservamos su estado; deténlo con su dueño antes de cambiarla.')
        if not registered:
            store=Store(self.home,create=True)
            try:
                with store.transaction() as db:
                    db.executemany('INSERT INTO config VALUES(?,?)',[('identity',encode('device-'+uuid.uuid4().hex[:12])),('runtime_socket',encode(discovery['socket']))])
                    db.execute('INSERT INTO agents VALUES(?,?)',('worker',thread))
                    if coordinator is not None:
                        db.execute('INSERT INTO peers VALUES(?,?)',(coordinator,encode({'transport':'receive-only','allow_agents':['worker'],'allow_ops':['followup','send_message','status','interrupt','receipt','watch','agents']})))
            finally:store.close()
        self.settings.update(thread_id=thread,bridge_home=str(self.home),codex_home=str(self.codex_home))
        if coordinator is not None:self.settings['coordinator']=coordinator
        json_file(self.settings_path,self.settings)
        return {'configured':True,'thread_id':thread,'note':'No se inició generación ni se cambió login.'}

    def start(self, thread=None, coordinator='manunruks'):
        if thread:self.configure(thread,coordinator)
        current=self.status()
        if current.get('runtime_ready',current['connected']):
            return {**current,'started_by_this_call':False,'note':'Conector existente adoptado. No se inició otro proceso ni se cambió su código.'}
        if current.get('connector_running'):
            raise AppError('Ya hay un conector esperando al runtime. No iniciaremos otro: revisa la sesión local o deténlo desde su dueño.')
        if not self.registered():
            selected=self.settings.get('thread_id') or os.environ.get('CODEX_THREAD_ID')
            if not selected:
                choices=self.discover()['sessions']
                if len(choices)==1:selected=choices[0]['id']
            if not selected:raise AppError('Hay varias sesiones. Abre la app y elige la conversación existente.')
            self.configure(selected,coordinator)
        self.state.mkdir(parents=True,mode=0o700,exist_ok=True)
        log=(self.state/'connector.log').open('a');os.chmod(self.state/'connector.log',0o600)
        process=subprocess.Popen([sys.executable,'-I','-S','-B',str(SOURCE/'csl'),'bridge','--home',str(self.home),'run'],stdin=subprocess.DEVNULL,stdout=log,stderr=log,start_new_session=True)
        json_file(self.state/'owned-process.json',{'pid':process.pid,'bridge_home':str(self.home)})
        end=time.monotonic()+8
        while time.monotonic()<end:
            current=self.status()
            if current.get('runtime_ready',current['connected']):return {**current,'started_by_this_call':True}
            if process.poll() is not None:break
            time.sleep(.1)
        raise AppError('El conector no pudo unirse. El runtime y tu conversación no se reiniciaron.')

    def stop(self):
        row=self.owned()
        if not row:raise AppError('El conector existente fue iniciado fuera de esta app; no lo detendremos automáticamente.')
        os.kill(row['pid'],signal.SIGTERM)
        return {'stopping':True,'note':'Se detiene sólo el conector de esta app, no Codex ni su conversación.'}

    def events(self, after=0):
        if type(after) is not int or after<0:raise AppError('Cursor inválido.')
        store=Store(self.home)
        try:
            rows=store.rows('SELECT seq,peer,body FROM outbound WHERE seq>? ORDER BY seq LIMIT 30',(after,))
            return {'events':[{'cursor':r['seq'],'peer':r['peer'],**decode(r['body'])} for r in rows]}
        finally:store.close()

    def peer_check(self, remote_agent=None):
        if not self.registered():raise AppError('Configura una sesión existente primero.')
        store=Store(self.home)
        try:
            peers=store.peers();identity=store.setting('identity')
        finally:store.close()
        selected=self.settings.get('coordinator') or next(iter(peers),None)
        resident_peer=None
        mode=self.settings.get('connection_mode')
        if mode in ('lan','relay'):
            if mode=='relay':
                from csl_bridge_relay_client import session_control
                active=session_control(self.state/'relay','status').get('peers',[])
            else:
                from csl_bridge_lan import control
                active=control(self.state/'lan','status').get('peers',[])
            preferred=self.settings.get('coordinator')
            resident_peer=next((entry for entry in active if entry['identity']==preferred),None)
            if resident_peer is None:
                if len(active)==1:resident_peer=active[0]
                elif not active:raise AppError('No confirmed peer in this session channel; pair first. Historical peers were not selected.')
                else:raise AppError('Multiple session peers are paired; select the coordinator explicitly. No arbitrary peer was queried.')
            selected=resident_peer['identity']
        config=peers.get(selected,{})
        if config.get('transport') in (None,'receive-only'):
            pending={'ready':False,'phase':'receive_only_not_verified','observed_at':time.time(),'note':'Este perfil recibe al coordinador. Su proceso vivo no acredita la ruta de vuelta; falta verificarla desde el otro equipo.'}
            json_file(self.state/'peer-proof.json',pending)
            return pending
        if remote_agent is None and resident_peer is not None:remote_agent=resident_peer['agent']
        if remote_agent is None and config.get('transport') in ('lan','relay'):
            from csl_bridge_lan import private_json
            remote_agent=private_json(self.state/('relay' if config['transport']=='relay' else 'lan')/'peers.json').get(selected)
        remote_agent=remote_agent or self.settings.get('remote_agent','coordinator')
        name(remote_agent)
        result=exchange(config,identity,{'version':1,'op':'status','id':str(uuid.uuid4()),'target':remote_agent})
        valid=result.get('agent')==remote_agent and result.get('status') in ('idle','active','tool-client') and isinstance(result.get('thread_id'),str)
        if not valid:raise AppError('La respuesta del peer no confirma el agente seleccionado; no se marca Listo.')
        proof={'ready':True,'phase':'authenticated_application_response','observed_at':time.time(),'peer':selected,
               'agent':remote_agent,'remote_status':result['status'],'note':'Respuesta de aplicación por el transporte configurado, no sólo PID o banner SSH.'}
        json_file(self.state/'peer-proof.json',proof)
        return proof

    def relay_profile_path(self):
        # Operator provisions this private file; no host/pin/token typing in the UI.
        local=self.state/'relay-profile.json'
        bundled=RESOURCE/'relay-profile.json'
        if local.exists():return local
        if bundled.exists():return bundled
        return None

    def connection_mode(self, requested='auto'):
        if requested not in ('auto','lan','relay'):raise AppError('Choose auto, lan or relay')
        if requested=='auto':return 'relay' if self.relay_profile_path() else 'lan'
        if requested=='relay' and self.relay_profile_path() is None:raise AppError('Relay profile not provisioned. Install the owner-provided private profile; no manual host/key input is required.')
        return requested

    def relay_share(self, thread=None):
        from csl_bridge_relay_client import ensure_session, session_control
        connector=self.start(thread,None)
        session=ensure_session(self.state/'relay',self.home,self.relay_profile_path())
        self.settings['connection_mode']='relay';json_file(self.settings_path,self.settings)
        result=session_control(self.state/'relay','share')
        return {**result,'lan_session_owned':session['started_by_caller'],'connector_owned':connector.get('started_by_this_call',False)}

    def relay_connect(self, code, thread=None):
        from csl_bridge_relay_client import ensure_session, session_control
        connector=self.start(thread,None)
        session=ensure_session(self.state/'relay',self.home,self.relay_profile_path())
        self.settings['connection_mode']='relay';json_file(self.settings_path,self.settings)
        result=session_control(self.state/'relay','connect',code=code.strip())
        ownership={'lan_session_owned':session['started_by_caller'],'connector_owned':connector.get('started_by_this_call',False)}
        if result.get('state')!='paired':
            json_file(self.state/'peer-proof.json',{'ready':False,'phase':'pairing_uncertain','observed_at':time.time(),'note':'Relay pairing not confirmed; reconcile before retrying.'})
            return {**result,**ownership}
        self.settings.update(coordinator=result['paired'],remote_agent=result['remote_agent']);json_file(self.settings_path,self.settings)
        # A relay lease is not a peer response or model progress.
        proof=self.peer_check(result['remote_agent'])
        return {**result,**ownership,'verified':proof['ready'],'note':'Peer application response authenticated. Accepted, model activity and final remain separate states.'}

    def lan_share(self, thread=None, bind=None):
        from csl_bridge_lan import ensure_session, control
        connector=self.start(thread,None)
        session=ensure_session(self.state/'lan',self.home,bind)
        self.settings['connection_mode']='lan';json_file(self.settings_path,self.settings)
        invitation=control(self.state/'lan','invite')
        return {**invitation,'lan_session_owned':session['started_by_caller'],'connector_owned':connector.get('started_by_this_call',False),'note':'Comparte manualmente el código o enlace privado. Un uso, cinco minutos. Sin QR. Permite encargos, mensajes, avances y resultados; no shell/historial/aprobaciones.'}

    def lan_connect(self, invitation, thread=None, bind=None):
        from csl_bridge_lan import invitation_input, connect_code
        code,link=invitation_input(invitation)
        connector=self.start(thread,None)
        self.settings['connection_mode']='lan';json_file(self.settings_path,self.settings)
        result=connect_code(self.state/'lan',self.home,code,bind,link)
        if result.get('state')!='paired':
            json_file(self.state/'peer-proof.json',{'ready':False,'phase':'pairing_uncertain','observed_at':time.time(),'note':'Emparejamiento sin confirmar; consulta el recibo antes de repetir.'})
            return {**result,'connector_owned':connector.get('started_by_this_call',False)}
        self.settings.update(coordinator=result['paired'],remote_agent=result['remote_agent'])
        json_file(self.settings_path,self.settings)
        proof=self.peer_check(result['remote_agent'])
        return {**result,'connector_owned':connector.get('started_by_this_call',False),'verified':proof['ready'],'note':'Par autenticado. Esto no demuestra que un modelo haya avanzado o finalizado.'}

    def lan_status(self, verify_peer=False):
        from csl_bridge_lan import control, interfaces
        if self.settings.get('connection_mode')=='relay':
            from csl_bridge_relay_client import session_control
            try:channel={**session_control(self.state/'relay','status'),'interfaces':[]}
            except (BridgeError,OSError):channel={'running':False,'mode':'relay','interfaces':[]}
        else:
            try:channel=control(self.state/'lan','status')
            except (BridgeError,OSError):channel={'running':False}
            channel={**channel,'interfaces':interfaces()}
        status=self.status(verify_peer)
        return {**channel,'channel':channel,'peer':status['peer'],'peer_ready':status['connected'],
                'runtime_ready':status['runtime_ready'],'task_phase':status.get('task_phase','none'),
                'note':status['note']}

    def lan_reconcile(self):
        from csl_bridge_lan import control
        if self.settings.get('connection_mode')=='relay':
            from csl_bridge_relay_client import session_control
            result=session_control(self.state/'relay','reconcile')
        else:result=control(self.state/'lan','reconcile')
        if result.get('state')=='paired':
            self.settings.update(coordinator=result['paired'],remote_agent=result['remote_agent'])
            json_file(self.settings_path,self.settings)
            result['verified']=self.peer_check(result['remote_agent'])['ready']
        return result

    def lan_stop(self):
        relayed=self.settings.get('connection_mode')=='relay'
        path=self.state/('relay/relay-control.sock' if relayed else 'lan/session-control.sock')
        if not path.exists():
            return {'stopped':True,'already_stopped':True,'note':'No session channel is running; no state was created.'}
        try:
            if relayed:
                from csl_bridge_relay_client import session_control
                result=session_control(self.state/'relay','stop')
            else:
                from csl_bridge_lan import control
                result=control(self.state/'lan','stop')
        except (FileNotFoundError,ConnectionRefusedError):
            return {'stopped':True,'already_stopped':True,'note':'No session channel is running; no state was changed.'}
        json_file(self.state/'peer-proof.json',{'ready':False,'phase':'stopping','observed_at':time.time(),'note':'Desconexión solicitada; no es cancelación del trabajo ya aceptado.'})
        return result

    def retry_original(self, operation_id, confirmed=False):
        identifier(operation_id)
        if not confirmed:raise AppError('Use --confirm-same-request after reviewing this original operation; no automatic retry.')
        store=Store(self.home)
        try:rows=store.rows("SELECT hash FROM operations WHERE id=? AND peer LIKE 'outbound:%'",(operation_id,))
        finally:store.close()
        if not rows:raise AppError('Original outgoing operation not found; no replacement task was created.')
        return local_call(self.home,{'local_op':'retry_operation','operation_id':operation_id,'expected_hash':rows[0]['hash'],'confirmed':True})

    def lan_call(self, action, text_file=None, request_id=None, operation_id=None, after=0, timeout=20):
        from csl_bridge_lan import control
        source=next(iter(self.registered()),None)
        if not source:raise AppError('Selecciona una conversación existente primero.')
        if action=='wait':return local_call(self.home,{'local_op':'wait','agent':source,'after':after,'timeout':timeout})
        if self.settings.get('connection_mode')=='relay':
            from csl_bridge_relay_client import session_control
            peers=session_control(self.state/'relay','status')['peers']
        else:peers=control(self.state/'lan','status')['peers']
        selected=self.settings.get('coordinator')
        peer=next((p for p in peers if p['identity']==selected),None) or (peers[0] if len(peers)==1 else None)
        if not peer:raise AppError('Selecciona un par conectado; no se eligió un destino arbitrario.')
        request_id=identifier(request_id or str(uuid.uuid4()))
        request={'version':1,'op':action,'id':request_id}
        if action in ('followup','send_message'):
            from csl_bridge import reviewed_text
            if text_file is None:raise AppError('Selecciona el texto del encargo; no se lee historial.')
            request.update(source=source,target=peer['agent'],text=reviewed_text(text_file))
        elif action=='receipt':request['request_id']=identifier(operation_id)
        try:return local_call(self.home,{'local_op':'call','peer':peer['identity'],'request':request})
        except BridgeError:return {'state':'uncertain','request_id':request['id'],'note':'No cancelado. Consulta receipt con este ID; no recrees la tarea.'}

    def task_phase(self):
        if not (self.home/'bridge.sqlite').exists():return 'none'
        store=Store(self.home)
        try:
            # Only this bridge's durable task/event metadata; no native history.
            # Incoming and outgoing followups share this chronological reservation table.
            # Do not compare rowids from tasks and operations or prefer old inbound work.
            operations=[r for r in store.rows('SELECT id,request,state FROM operations ORDER BY rowid DESC LIMIT 256') if decode(r['request']).get('op')=='followup']
            task_id=operations[0]['id'] if operations else None
            if not task_id:return 'none'
            rows=store.rows('SELECT body FROM outbound ORDER BY seq DESC LIMIT 256')+store.rows('SELECT body FROM inbound ORDER BY seq DESC LIMIT 256')
            events=[decode(r['body']) for r in rows if decode(r['body']).get('task_id')==task_id]
            final=next((e for e in events if e.get('kind')=='final'),None)
            if final:return {'interrupted':'final_interrupted','failed':'final_failed','completed':'final_observed'}.get(final.get('status'),'final_unknown')
            if any(e.get('kind')=='progress' for e in events):return 'activity_observed'
            operation=store.rows('SELECT state FROM operations WHERE id=?',(task_id,))
            if operation and operation[0]['state'] in ('uncertain','invoking'):return 'uncertain'
            if operation and operation[0]['state']=='delivered':return 'accepted_only'
            return 'none'
        finally:store.close()

    def mcp_profile(self, delegation=False):
        from csl_bridge_mcp import render_profile
        agent=next(iter(self.registered()),'worker')
        grants=['send_progress']+(['followup_task','send_message'] if delegation else [])
        return render_profile(self.home,agent,grants,sys.executable,
            ['-I','-S','-B',str(SOURCE/'csl-bridge-mcp'),'--home',str(self.home),'--agent',agent])

    @staticmethod
    def replace_owned(text, replacement):
        import tomllib
        data=tomllib.loads(text);exists='csl_bridge' in data.get('mcp_servers',{})
        lines=text.splitlines(keepends=True);kept=[];owned=[];inside=False;found=False
        for line in lines:
            if line.lstrip().startswith('['):
                header=line.strip().split('#',1)[0].strip()
                inside=bool(re.fullmatch(r'\[mcp_servers\.csl_bridge(?:\.[A-Za-z0-9_-]+)*\]',header))
                if inside:found=True
            (owned if inside else kept).append(line)
        if exists and not found:raise AppError('La configuración existente usa un formato que requiere revisión local. No se cambió.')
        result=''.join(kept)+'\n'+replacement
        tomllib.loads(result)
        return result,''.join(owned)

    def mcp_plan(self):
        return {'profile':self.mcp_profile(),'tools':['Enviar avances de la tarea activa','Consultar estado y resultados'],
                'note':'Consentimiento local sólo para send_progress. No cambia aprobación global, modelo, login o permisos shell.'}

    def mcp_install(self, delegation=False):
        import tomllib
        thread=next(iter(self.registered().values()),None)
        if not thread:raise AppError('Selecciona una sesión primero.')
        if thread not in [x['id'] for x in self.discover()['sessions']]:
            raise AppError('La sesión debe seguir cargada para activar herramientas. No se reabrirá implícitamente.')
        path=self.codex_home/'config.toml'
        if path.is_symlink():raise AppError('La configuración está redirigida; requiere revisión de su dueño.')
        old=path.read_text() if path.exists() else ''
        new,previous=self.replace_owned(old,self.mcp_profile(delegation))
        rollback={'path':str(path),'previous_owned_section':previous,'applied_profile':self.mcp_profile(delegation)}
        json_file(self.state/'mcp-rollback.json',rollback)
        fd,temp=tempfile.mkstemp(prefix='.csl-mcp-',dir=path.parent)
        try:
            os.fchmod(fd,path.stat().st_mode&0o777 if path.exists() else 0o600)
            with os.fdopen(fd,'w') as stream:stream.write(new)
            if (path.read_text() if path.exists() else '')!=old:raise AppError('Configuración cambió durante la operación; no se reemplazó.')
            os.replace(temp,path)
        finally:
            if os.path.exists(temp):os.unlink(temp)
        runtime=Runtime(self.endpoint())
        try:
            runtime.call('config/mcpServer/reload',{})
            runtime.call('thread/resume',{'threadId':thread,'excludeTurns':True})
        finally:runtime.close()
        return {'installed':True,'note':'Sólo sección csl_bridge actualizada. Rollback local de esa sección disponible.'}

    def mcp_rollback(self):
        path=self.state/'mcp-rollback.json'
        if not path.exists():raise AppError('No hay un cambio de herramientas de esta app para revertir.')
        backup=json.loads(path.read_text());config=Path(backup['path']);current=config.read_text()
        _,owned=self.replace_owned(current,'')
        if owned.strip()!=backup['applied_profile'].strip():
            raise AppError('La sección cambió después; no se sobrescribirá una configuración posterior.')
        restored,_=self.replace_owned(current,backup['previous_owned_section'])
        fd,temp=tempfile.mkstemp(prefix='.csl-rollback-',dir=config.parent)
        try:
            os.fchmod(fd,config.stat().st_mode&0o777)
            with os.fdopen(fd,'w') as stream:stream.write(restored)
            os.replace(temp,config)
        finally:
            if os.path.exists(temp):os.unlink(temp)
        runtime=Runtime(self.endpoint())
        try:runtime.call('config/mcpServer/reload',{})
        finally:runtime.close()
        return {'restored_owned_section_only':True}

    def connection_info(self):
        return {'thread_id':next(iter(self.registered().values()),None),'bridge_home':str(self.home),
                'receiver_command':shlex.join(['env','CSL_BRIDGE_HOME='+str(self.home),'CSL_BRIDGE_PEER='+self.settings.get('coordinator','manunruks'),sys.executable,'-I','-S','-B',str(SOURCE/'csl-bridge-receiver')]),
                'note':'Requiere transporte ya autorizado. No habilita SSH ni puertos ni un servicio de pairing.'}


def main():
    parser=argparse.ArgumentParser(description='CSL Bridge app: existing sessions only, no model/server/login creation.')
    parser.add_argument('action',choices=['retry-original','lan-reconcile','lan-followup','lan-message','lan-receipt','lan-wait','lan-share','lan-connect','lan-status','lan-stop','discover','status','configure','connect','start','stop','events','peer-check','mcp-plan','mcp-install','mcp-rollback','connection-info','transport-run','transport-status','selftest'])
    parser.add_argument('--text-file',type=Path)
    parser.add_argument('--request-id')
    parser.add_argument('--operation-id')
    parser.add_argument('--confirm-same-request',action='store_true',help='Explicitly retry only the original stored ID/body/hash after an authenticated not_found receipt; never replaces work.')
    from csl_bridge import wait_seconds
    parser.add_argument('--timeout',type=wait_seconds,default=20)
    parser.add_argument('--mode',choices=['auto','lan','relay'],default='auto',help='Auto uses provisioned relay for mobility, otherwise LAN. No automatic task retransmission/fallback.')
    parser.add_argument('--bind','--interface',dest='bind',help='Select a local IPv4 interface, no firewall changes')
    parser.add_argument('--pair-code-stdin',action='store_true',help='Read one private manual code/link from stdin, never argv')
    parser.add_argument('--thread');parser.add_argument('--coordinator',default='manunruks');parser.add_argument('--after',type=int,default=0)
    parser.add_argument('--approve-delegation',action='store_true',help='Initial consent: followup_task can activate idle work and consume model tokens; send_message included, not interrupt.')
    parser.add_argument('--approve-communication',action='store_true',help='Explicit local consent to install only csl_bridge communication tools after connect.')
    parser.add_argument('--transport-profile',type=Path)
    parser.add_argument('--remote-agent')
    parser.add_argument('--verify-peer',action='store_true',help='Explicitly refresh application-level peer response; never starts a model/task.')
    parser.add_argument('--state-root');parser.add_argument('--bridge-home');parser.add_argument('--codex-home')
    args=parser.parse_args();service=Service(args.state_root,args.bridge_home,args.codex_home)
    def optional_communication(result):
        if args.approve_communication:
            if args.action=='lan-connect' and result.get('state')!='paired':return result
            try:result['communication_tools']=service.mcp_install(args.approve_delegation)
            except (AppError,BridgeError,RuntimeUnavailable,RuntimeRejected,OSError,ValueError):
                result['communication_tools']={'installed':False,'note':'Communication tools were not installed. Review the local configuration/policy; pairing alone does not enable agent tools.'}
        return result
    try:
        if args.approve_delegation and not args.approve_communication:raise AppError('Delegation requires explicit initial communication consent.')
        if args.action=='retry-original':result=service.retry_original(args.operation_id,args.confirm_same_request)
        elif args.action in ('lan-followup','lan-message','lan-receipt','lan-wait'):
            action={'lan-followup':'followup','lan-message':'send_message','lan-receipt':'receipt','lan-wait':'wait'}[args.action]
            result=service.lan_call(action,args.text_file,args.request_id,args.operation_id,args.after,args.timeout)
        elif args.action=='lan-share':
            mode=service.connection_mode(args.mode)
            result=service.relay_share(args.thread) if mode=='relay' else service.lan_share(args.thread,args.bind)
            result=optional_communication(result)
        elif args.action=='lan-connect':
            if not args.pair_code_stdin:raise AppError('Paste the private code/link through stdin; never command arguments.')
            invitation=sys.stdin.readline(4096)
            mode=service.connection_mode(args.mode)
            result=service.relay_connect(invitation,args.thread) if mode=='relay' else service.lan_connect(invitation,args.thread,args.bind)
            result=optional_communication(result)
        elif args.action=='transport-run':
            if not args.transport_profile:raise AppError('Selecciona un perfil SSH ya autorizado; no se configura red automáticamente.')
            sys.path.insert(0,str(RESOURCE))
            from ssh_session import run_profile
            run_profile(args.transport_profile,service.state/'transport-status.json')
            result={'stopped':True}
        elif args.action=='transport-status':
            path=service.state/'transport-status.json'
            result=json.loads(path.read_text()) if path.exists() else {'phase':'not_configured','end_to_end_verified':False}
        elif args.action=='selftest':
            import fcntl,curses,sqlite3,platform
            result={'python':platform.python_version(),'architecture':platform.machine(),'prefix':sys.prefix,'stdlib_imports':True,'source':str(SOURCE),'model_calls':0}
        elif args.action=='configure':result=service.configure(args.thread,args.coordinator)
        elif args.action in ('connect','start'):
            result=service.start(args.thread,args.coordinator)
            if args.action=='connect':
                result['peer_verification']=service.peer_check(args.remote_agent)
                result.update(service.status())
            result=optional_communication(result)
        elif args.action=='lan-status':result=service.lan_status(args.verify_peer)
        elif args.action=='peer-check':result=service.peer_check(args.remote_agent)
        elif args.action=='events':result=service.events(args.after)
        elif args.action=='status':result=service.status(args.verify_peer)
        else:result=getattr(service,args.action.replace('-','_'))()
        print(json.dumps({'ok':True,'data':result},ensure_ascii=False))
    except (AppError,BridgeError,RuntimeUnavailable,RuntimeRejected,OSError,ValueError,KeyError) as exc:
        message=str(exc) if isinstance(exc,(AppError,BridgeError)) else 'Operación local no disponible. Conserva IDs y consulta el recibo antes de repetir encargos o permisos.'
        if isinstance(exc,(RuntimeUnavailable,RuntimeRejected)):
            message='El runtime no aceptó la consulta. Conservamos tu sesión: comprueba que sigue abierta y que permite el acceso local.'
        print(json.dumps({'ok':False,'error':message},ensure_ascii=False));return 1
    return 0
if __name__=='__main__':raise SystemExit(main())
