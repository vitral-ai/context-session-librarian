# CSL Bridge — candidato privado Mac y CLI Linux

En la app: **selecciona tu conversación ya abierta → Compartir código**. En la otra
utilidad: selecciona su conversación, pega el código y conecta. El código aleatorio
es de26caracteres, se comparte MANUALMENTE copiar/pegar, caduca en5min y funciona una
vez. No QR. **Automático usa el relay privado provisionado por el owner si hay perfil;
sin perfil usa LAN directa.** Son modos distintos:

- **Relay privado:** los dos equipos abren conexiones salientes al endpoint TLS del
  owner (en este ensayo, puerto8443). Funciona entre redes distintas si ese endpoint
  es accesible. El perfil/pin y enrollment privado los instala el owner; el usuario
  comparte sólo el código. No necesita SSH ni una LAN común.
- **LAN directa (`--mode lan`):** requiere la misma LAN accesible y permisos nativos
  de red local/entrante. Si UDP no llega, copiar/pegar el enlace privado de la misma
  invitación. Se elige la interfaz de la ruta por defecto del OS, sin tráfico; si es
  ambigua, selección en app o `--interface NOMBRE`.

No se copian claves, huellas ni rutas Python en el recorrido normal. Se usa openssl
existente del OS; no se cambian firewall/router ni servicios globales. No hay selección
medida que prefiera LAN ni fallback automático LAN→relay después de un envío incierto.
No se ofrece un relay público de producto ni NAT traversal propio. SSH sigue siendo
un transporte avanzado previamente autorizado.

Contraparte CLI Linux/Mac, Python>=3.11 existente (Macapp incluye Python3.12 privado):

```sh
./csl-pair share --thread "$CODEX_THREAD_ID" --approve-communication
./csl-pair connect --thread "$CODEX_THREAD_ID" --pair-code-stdin --approve-communication
```

En el segundo comando pega el código/enlace y Enter. No va en argumentos/env/logs.
Los comandos se ejecutan en CADA sesión real que se quiere conservar; no crear un
thread para que se parezca. Detecta sólo UUID ya loaded; no carga historia ni login.
`--bridge-home` opcional permite al operador adoptar un puente existente. Si hay un
conector antiguo o peer SSH/receive-only, NO se sobrescribe: el operador debe preparar
migración local explícita con rollback. No arrancar otro escritor para evitarlo.

Consentimiento inicial: el perfil básico permite `send_progress` y consultas. Para
coordinación por herramientas añade explícitamente `--approve-delegation` (checkbox
en la app): añade SÓLO `followup_task` y `send_message`. Un encargo puede activar un
agente idle y consumir tokens/costo. No incluye interrupt automático, shell,
aprobaciones remotas, nuevos permisos/login/modelo. Si MCP no se instala, se informa;
un par válido por sí solo no acredita agentes autónomos. La vía CLI sigue disponible:

```sh
./csl-pair task --text-file tarea.txt
./csl-pair message --text-file aclaracion.txt
./csl-pair wait --after 0 --timeout 20
./csl-pair receipt --operation-id ID
./csl-pair status --verify-peer
./csl-pair reconcile
./csl-pair stop
```

`task`/`message` imprimen un ID/recibo. Usa `--request-id ID` para conservar el ID,
no recrear tareas inciertas. `wait` espera avances/finales de trabajos delegados;
una aclaración al trabajador entra por steer y wait puede devolver vacío. Aceptado
no significa que el modelo continuó; avance observado y final son estados distintos.
Final interrupted/failed no es éxito. Completed tampoco valida corrección del texto.

Si se pierde respuesta de pairing, muestra incierto: `reconcile` consulta un recibo
autenticado del MISMOgrant; no crea otro. No repetir invitaciones para ocultarlo.
Stop revoca acceso futuro de ese proceso de sesión e invalida tokens privados,
no cancela encargos ya aceptados ni para Codex. Una caída abrupta conserva estado,
puerto/identidad TLS y credenciales sin vencimiento artificial2h; recuperación exige
que la misma interfaz/puerto sigan disponibles. Re-pair explícito rota el token viejo.

TLS stdlib con certificado de sesión fijado; discovery HMAC liga nonce/endpoint/pin
al código. Endpoints de retorno ligados a IP del cliente y limitados a IPv4 privada.
Frames y tiempos acotados,6intentos por invitación,8respuestas discovery/s y16requests
poshandshake. TLS handshake es síncrono hasta10s: NO resistenciaDoS garantizada ni16
handshakes paralelos. Tokens/certkey sólo estado privado; no datos Codex/auth.

Build reproducible sobre fuente congelada tracked-only+manifest pineado:

```sh
python3.12 source/macos/build_app.py --source source --source-sha SHA \
  --source-manifest-sha256 MANIFEST_SHA256 --runtime-root PYTHON_STANDALONE \
  --output DIRECTORIO_NUEVO
```

Genera .app+ZIP+SHA, Python privado, selftest HOME/PATH vacío, linkage completo y
firma ad-hoc. No DeveloperID/notarización/publicación. El ZIP04487f7 anterior NO
incluye pairing. Este nuevo source debe compilar/renderizarse y probarse en dos
PC reales antes de declarar listo: tests TLS/UDP/IPC loopback y modelo fabricado no
son una prueba Mac/Linux LAN con agentes reales ni Internet de pairing propio.

## Relay privado integrado — candidato16Nov

Si el owner instaló `relay-profile.json` privado en el directorio de estado de esta
utilidad, Automático usa ese relay para movilidad entre redes. Sin perfil usa LAN.
`--mode lan` permite elegir directo explícitamente; `--mode relay` exige el perfil.
Esta versión NO mide LAN y cambia automáticamente a relay: evita repetir un envío
incierto por otro transporte. No ofrece NAT traversal propio ni relay público.

El perfil contiene host, port, fingerprint y enrollment_file (ruta local privada).
El enrollment de prueba es256bit, archivo600, nunca argumento/URL/Git. El owner lo
provisiona en ambos equipos; el usuario sólo selecciona conversación, comparte código
y conecta. No se distribuye ese secreto global en un ZIP público. El relay requiere
TLS exterior fijado ANTES de enviar enrollment, y dentro transporta TLS peer↔peer.
El relay no termina esa segunda capa: ve IPs/rutas/tamaños/tiempos, no mensajes,
tokens de los pares, código de invitación ni claves privadas del peer.

Discovery Internet es rendezvous por locator derivado del código aleatorio128bit.
Descriptor HMAC autenticado liga identidad, ruta, pin y caducidad. El código se
comparte manualmente; nunca se manda al relay. La invitación dura cinco minutos y
el endpoint receptor consume una vez. El relay no se vuelve autoridad del agente.

CLI se mantiene: `csl-pair share` / `connect --pair-code-stdin`, opcional
`--approve-communication --approve-delegation`. El perfil inicial se lee de
`~/Library/Application Support/CSLBridge/relay-profile.json` en Mac y
`${XDG_STATE_HOME:-~/.local/state}/csl-bridge/relay-profile.json` en Linux.
`--state-root` permite al operador instalarlo en otro estado explícito. No se
extraen credenciales Codex, no se arranca daemon nuevo ni se copia la conversación.

Un proceso de sesión mantiene accept/lease salientes con backoff1..30s. Lease180s
se renueva; un corte puede reanudar la MISMA ruta con su lease, sin código nuevo ni
vencimiento artificial2h. Stop revoca ruta/grants y no cancela modelos ya activos.
La pérdida del transporte de una tarea queda incierta: consulta receipt con el mismo
ID; no se reenvía el encargo automáticamente. Register/renew sólo significa relay
conectado: después de reconectar hace falta una respuesta de aplicación autenticada
para marcar peer listo. Aceptación, actividad y final siguen separados.

Servidor self-host de ensayo: Python>=3.11, openssl/cert TLS y enrollment privados;
`python3 csl_bridge_relay.py serve --bind IP --port PUERTO --certificate CERT
--private-key KEY --enrollment-file FILE`. El owner instala sólo relay+store en un
usuario dedicado, publica el pin confiablemente y decide límites del OS. Defaults:
32rutas,16conexiones (waiters incluidos; no32pares simultáneos), control4KiB,
60conexiones/min/IP y240/min global, tablaIPmáx1024, invitation300s, pending8/ruta, idle60s, túnel180s/8MiB.
No SLA/DoS garantizado; el límite externo de memoria/CPU corresponde al operador.
No logs de payload, código, tokens ni respuesta OAuth. El pin no es TOFU.

Los tests nuevos usan nestedTLS real/MemoryBIO y relay loopback. No sustituyen
prueba pública real, modelo entre redes o revisión nativa de esta UI. El owner
maneja esos gates; no confundirlos con el InternetSSH previo de ee74.

`status` separa `channel` (proceso/lease del transporte), `peer`/`peer_ready`
(respuesta de aplicación autenticada) y `task_phase`. `status --verify-peer` consulta
el peer del canal residente para renovar esa prueba; no selecciona un peer histórico
por orden de la base. Si hay varios pares y ninguno seleccionado, pide selección
explícita. Lease/register no acredita peer listo ni avance del modelo.

## Diagnóstico de recuperación y reenvío explícito

El heartbeat del cliente renueva su lease en un hilo independiente del accept y de
la llamada IPC/nativa. Normalmente renueva cada lease/3, máximo45s; la lease sigue
siendo renovable, no infinita ni una autorización nueva. Estado privado del cliente
registra heartbeat_stage, accept_stage, últimos intentos/éxitos y categorías de error.
No incluye IP/ruta/ID de operación/token/payload. No inferir bloqueo por un connected_at
antiguo: ahora hay observaciones periódicas. La reproducción local lease0.12s frente
a accept0.45s demuestra la fragilidad anterior y el arreglo; no determina por sí sola
la causa del fallo real con lease180s.

Servidor: `--diagnostics-file /DIRECTORIO_PRIVADO/diagnostics.json` escribe contadores
agregados por razón (lease_expired, route_missing, accept_busy, rate_ip/global,
capacity, etc.), sin IP/route/token/payload. Directorio propiedad del servicio0700;
archivo atómico0600. El rechazo exterior puede incluir reasoncode estático, nunca
texto de excepción del OS. No cambia la autenticación ni el cifrado.

Una tarea/mensaje con resultado incierto no se vuelve a emitir por repetir task.
Si el operador decide recuperar el MISMO contenido ya autorizado:

```sh
./csl-pair retry --operation-id ORIGINAL_ID --confirm-same-request
```

Carga el ID, cuerpo y hash de la reserva durable local; comprueba hash en el conector
y consulta primero un receipt autenticado. Delivered/pending/rejected recuperan su
recibo sin enviar; invoking/uncertain o falta de respuesta NO permiten envío. Sólo
not_found permite UN envío del ID/cuerpo exactos. Not_found es una foto: un frame viejo
podría llegar después; la reserva/hash del receptor deduplica esa carrera. No garantiza
exactly-once si un operador destruye la base receptora. Si vuelve a perderse el recibo,
sigue incierto. Sin IDs nuevos, cambios de payload, replay nativo, retry MCP automático
ni reenvío de interrupt: una interrupción exige una nueva decisión sobre el turno actual.
El presupuesto sólo de este control explícito es185s (dos exchanges90s+margen5);
wait0..20, IPC local25, operaciones remotas95 y hostMCP105 permanecen intactos.
No reintentar mensajes/interrupciones de tareas ya terminadas para rehacer el ensayo.

## Mantener disponible este Mac mientras el cliente está conectado

La GUI muestra una opción visible, marcada inicialmente, aplicada al compartir o
conectar mediante código: **Mantener este Mac disponible mientras está conectado
(usa energía)**. CLI es opt-in con `--keep-awake` en share/connect; sin el flag no
cambia una assertion ya existente. `--no-keep-awake` libera sólo la assertion de
este cliente. Linux no requiere ni ejecuta caffeinate.

En macOS el proceso residente inicia `/usr/bin/caffeinate -i -s -w CLIENT_PID`.
Evita idle sleep del sistema (la assertion-s sólo es efectiva con alimentación AC),
no impide que se apague la pantalla y no cambia pmset, ajustes globales, root o
servicios. Puede aumentar consumo. `status` incluye power.supported/enabled/error
del canal; enabled significa proceso de assertion activo, no garantía de conexión,
de respuesta del peer ni que el OS nunca suspenda por tapa cerrada/sleep explícito.

Stop libera inmediatamente su hijo caffeinate y el cierre normal también. Si el
cliente cae por SIGKILL, caffeinate observa CLIENT_PID con-w y termina al desaparecer
ese proceso: la assertion no queda atada a la duración de una consola o key2h. No
se mata ni altera un caffeinate creado por el owner u otra aplicación. Una caída
seguida de recovery requiere seleccionar --keep-awake de nuevo en la acción local
si se desea; no se reconstruye una autorización global ni se cambian políticas.

Pruebas del módulo son mocks locales, no ensayo de sleep real. Owner observó Sleep/
DarkWake en el Mac con ACsleep1min y heartbeat congelado; después obtuvo recuperación
asistida usando una assertion propia temporal. Esa evidencia separada fortalece la
hipótesis de suspensión, pero no convierte todo fallo previo en causa única probada.
