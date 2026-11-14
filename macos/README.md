# CSL Bridge — candidato privado Mac y CLI Linux

En la app: **selecciona tu conversación ya abierta → Compartir código**. En la otra
utilidad: selecciona su conversación, pega el código y conecta. El código aleatorio
es de26caracteres, se comparte MANUALMENTE copiar/pegar, caduca en5min y funciona una
vez. No QR. Si UDP no llega, copiar/pegar el enlace privado de la misma invitación.
No copiar claves, huellas, rutas Python ni configurar SSH para este recorrido.

Requiere misma LAN accesible, openssl existente del OS y permisos nativos de red
local/entrante. No cambia firewall/router ni instala servicios globales. Se elige la
interfaz de la ruta por defecto del OS, sin tráfico; si hay ambigüedad puede elegirse
en la app o con `--interface NOMBRE`. No hay relay público ni pairing Internet/NAT.
SSH queda disponible como transporte avanzado previamente autorizado.

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
./csl-pair status
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
