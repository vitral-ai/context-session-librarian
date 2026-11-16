import AppKit
import Foundation

let bundle = Bundle.main
let resources = bundle.resourceURL ?? URL(fileURLWithPath: CommandLine.arguments[0]).deletingLastPathComponent().appendingPathComponent("../Resources").standardizedFileURL

func backend(_ arguments: [String], input: Data? = nil) -> (Int32, Data) {
    let process = Process()
    process.executableURL = resources.appendingPathComponent("python-runtime/bin/python3.12")
    process.arguments = ["-I", "-S", "-B", resources.appendingPathComponent("app_service.py").path] + arguments
    let output = Pipe(), errors = Pipe()
    process.standardOutput = output; process.standardError = errors
    let incoming = Pipe()
    if input != nil { process.standardInput = incoming }
    var environment = ProcessInfo.processInfo.environment
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    process.environment = environment
    do {
        try process.run()
        if let body=input { incoming.fileHandleForWriting.write(body);try? incoming.fileHandleForWriting.close() }
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        if data.isEmpty { return (process.terminationStatus, Data("{\"ok\":false,\"error\":\"No se pudo ejecutar el runtime privado.\"}".utf8)) }
        return (process.terminationStatus, data)
    } catch {
        return (1, Data("{\"ok\":false,\"error\":\"Runtime privado no disponible. Revisa el paquete de la app.\"}".utf8))
    }
}

final class App: NSObject, NSApplicationDelegate {
    var window: NSWindow!
    var pages: NSSegmentedControl!
    var connectView = NSView(), activityView = NSView()
    var sessions = NSPopUpButton()
    var coordinator = NSTextField(string: "manunruks")
    var pairCode = NSSecureTextField(string: "")
    var network = NSPopUpButton()
    var transportMode = NSPopUpButton()
    var allowDelegation = NSButton(checkboxWithTitle: "Permitir encargos automáticos (puede consumir tokens)", target: nil, action: nil)
    var networkIDs: [String] = []
    var statusLabel = NSTextField(labelWithString: "Detectando sesión local…")
    var noteLabel = NSTextField(wrappingLabelWithString: "Tu conversación y login se conservan.")
    var runtimeState = NSTextField(labelWithString: "Sesión: pendiente")
    var connectorState = NSTextField(labelWithString: "Conector: pendiente")
    var peerState = NSTextField(labelWithString: "Peer: sin verificar")
    var activity = NSTextView()
    var sessionIDs: [String] = []
    var stopButton: NSButton!
    var stateText = ""
    var cursor = 0
    var busy = false
    var beganOwnedConnector = false
    var beganOwnedLAN = false
    var monitoringPeer = false
    var renderDirectory: String?

    func label(_ text: String, bold: Bool = false, size: CGFloat = 14) -> NSTextField {
        let field = NSTextField(wrappingLabelWithString: text)
        field.font = bold ? NSFont.boldSystemFont(ofSize: size) : NSFont.systemFont(ofSize: size)
        return field
    }
    func button(_ title: String, _ action: Selector) -> NSButton {
        let value = NSButton(title: title, target: self, action: action)
        value.bezelStyle = .rounded
        return value
    }
    func stack(_ views: [NSView], spacing: CGFloat = 14) -> NSStackView {
        let view = NSStackView(views: views)
        view.orientation = .vertical; view.alignment = .leading; view.spacing = spacing
        return view
    }
    func horizontal(_ views: [NSView]) -> NSStackView {
        let view = NSStackView(views: views); view.orientation = .horizontal; view.spacing = 10
        return view
    }
    func applicationDidFinishLaunching(_ notification: Notification) {
        window = NSWindow(contentRect: NSRect(x: 0,y: 0,width: 800,height: 960), styleMask: [.titled,.closable,.miniaturizable,.resizable], backing: .buffered, defer: false)
        window.title = "CSL Bridge · privado"
        window.center(); window.minSize = NSSize(width: 760,height: 920)
        let root = window.contentView!
        let title = label("CSL Bridge", bold: true, size: 27)
        let subtitle = label("Tu agente, en su conversación de siempre", size: 15)
        subtitle.textColor = .secondaryLabelColor
        pages = NSSegmentedControl(labels: ["Sesión y conexión", "Actividad"], trackingMode: .selectOne, target: self, action: #selector(changePage))
        pages.selectedSegment = 0
        statusLabel.font = .boldSystemFont(ofSize: 15)
        noteLabel.textColor = .secondaryLabelColor
        let header = stack([title, subtitle, pages],spacing: 8)
        let footer = stack([statusLabel,horizontal([runtimeState,connectorState,peerState]),noteLabel],spacing: 5)
        [header,connectView,activityView,footer].forEach { $0.translatesAutoresizingMaskIntoConstraints = false; root.addSubview($0) }
        NSLayoutConstraint.activate([
            header.leadingAnchor.constraint(equalTo: root.leadingAnchor,constant: 28), header.topAnchor.constraint(equalTo: root.topAnchor,constant: 24), header.trailingAnchor.constraint(equalTo: root.trailingAnchor,constant: -28),
            connectView.leadingAnchor.constraint(equalTo: header.leadingAnchor),connectView.trailingAnchor.constraint(equalTo: header.trailingAnchor),connectView.topAnchor.constraint(equalTo: header.bottomAnchor,constant: 24),connectView.bottomAnchor.constraint(equalTo: footer.topAnchor,constant: -20),
            activityView.leadingAnchor.constraint(equalTo: connectView.leadingAnchor),activityView.trailingAnchor.constraint(equalTo: connectView.trailingAnchor),activityView.topAnchor.constraint(equalTo: connectView.topAnchor),activityView.bottomAnchor.constraint(equalTo: connectView.bottomAnchor),
            footer.leadingAnchor.constraint(equalTo: header.leadingAnchor),footer.trailingAnchor.constraint(equalTo: header.trailingAnchor),footer.bottomAnchor.constraint(equalTo: root.bottomAnchor,constant: -22)
        ])
        sessions.setContentHuggingPriority(.defaultLow, for: .horizontal)
        let find = button("Detectar", #selector(detect))
        let connect = button("Conectar",#selector(connectSelected)); connect.keyEquivalent = "\r"
        let intro = label("Elige una sesión que ya esté abierta en Codex. La app no crea otro agente ni inicia sesión por ti.")
        let connectionTitle = label("Conexión guardada",bold: true)
        let connectionNote = label("Se reutiliza el acceso del coordinador ya autorizado. La app no habilita SSH ni abre puertos.")
        connectionNote.textColor = .secondaryLabelColor
        let tools = button("Herramientas de comunicación…",#selector(installTools))
        let info = button("Copiar conexión",#selector(copyConnection))
        transportMode.addItems(withTitles:["Automático · relay provisionado, si existe", "Sólo LAN directa", "Relay Internet provisionado"])
        pairCode.placeholderString="Pega el código de 26 caracteres o enlace privado"
        network.addItem(withTitle:"Detectar red local automáticamente")
        let pairNote=label("Comparte el código manualmente: copiar y pegar. Un uso, cinco minutos; sin QR. Automático usa el relay privado provisionado para redes distintas; sin perfil, usa LAN. No cambia de canal ni repite tareas inciertas automáticamente.")
        pairNote.textColor = .secondaryLabelColor
        let share=button("Compartir código…",#selector(shareLAN))
        let join=button("Conectar con código…",#selector(joinLAN))
        let reconcile=button("Revisar recibo",#selector(reconcileLAN))
        let body = stack([label("Sesión existente",bold: true),intro,horizontal([sessions,find]),label("Canal de conexión",bold: true),transportMode,network,pairNote,label("Código o enlace privado"),pairCode,allowDelegation,horizontal([share,join,reconcile]),label("Conexión avanzada ya configurada",bold: true),horizontal([connect,tools,info])],spacing: 12)
        body.translatesAutoresizingMaskIntoConstraints = false;connectView.addSubview(body)
        NSLayoutConstraint.activate([body.leadingAnchor.constraint(equalTo: connectView.leadingAnchor),body.trailingAnchor.constraint(equalTo: connectView.trailingAnchor),body.topAnchor.constraint(equalTo: connectView.topAnchor),sessions.widthAnchor.constraint(greaterThanOrEqualToConstant: 390),coordinator.widthAnchor.constraint(equalToConstant: 280)])
        activity.isEditable = false;activity.isSelectable = true;activity.font = .systemFont(ofSize: 13)
        activity.textContainerInset = NSSize(width: 12,height: 12)
        let scroll = NSScrollView();scroll.documentView = activity;scroll.hasVerticalScroller = true;scroll.borderType = .bezelBorder
        stopButton = button("Detener conector de esta app",#selector(stopOwned));stopButton.isEnabled = false
        let refresh = button("Actualizar",#selector(refresh))
        let activityStack = stack([label("Avances y resultados",bold: true),label("Mensajes de la tarea recibida. Su contenido no concede permisos adicionales."),scroll,horizontal([refresh,stopButton])],spacing: 10)
        activityStack.translatesAutoresizingMaskIntoConstraints=false;activityView.addSubview(activityStack)
        NSLayoutConstraint.activate([activityStack.leadingAnchor.constraint(equalTo: activityView.leadingAnchor),activityStack.trailingAnchor.constraint(equalTo: activityView.trailingAnchor),activityStack.topAnchor.constraint(equalTo: activityView.topAnchor),activityStack.bottomAnchor.constraint(equalTo: activityView.bottomAnchor),scroll.heightAnchor.constraint(greaterThanOrEqualToConstant: 210),scroll.widthAnchor.constraint(equalTo: activityStack.widthAnchor)])
        activityView.isHidden=true
        if renderDirectory == nil { window.makeKeyAndOrderFront(nil); NSApplication.shared.activate(ignoringOtherApps: true) }
        if let dir=renderDirectory {
            renderPreviews(dir)
        } else {
            detect()
            Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { _ in if !self.busy { self.refresh() } }
        }
    }
    func perform(_ args: [String], input: String? = nil, _ completion: @escaping ([String:Any]) -> Void) {
        if busy { return };busy=true
        DispatchQueue.global(qos: .userInitiated).async {
            let (_,data)=backend(args,input: input.map {Data(($0+"\n").utf8)})
            let object=(try? JSONSerialization.jsonObject(with:data)) as? [String:Any] ?? ["ok":false,"error":"Respuesta local no disponible."]
            DispatchQueue.main.async { self.busy=false;completion(object) }
        }
    }
    func unwrap(_ object: [String:Any]) -> [String:Any]? {
        if object["ok"] as? Bool != true {
            statusLabel.stringValue="Necesita atención";noteLabel.stringValue=object["error"] as? String ?? "Operación no disponible."
            return nil
        }
        return object["data"] as? [String:Any]
    }
    func updateStatus(_ value: [String:Any]) {
        statusLabel.stringValue=value["headline"] as? String ?? "Estado local"
        runtimeState.stringValue=(value["runtime_ready"] as? Bool ?? false) ? "Sesión: lista" : "Sesión: pendiente"
        connectorState.stringValue=(value["connector_running"] as? Bool ?? false) ? "Conector: activo" : "Conector: detenido"
        let peer=value["peer"] as? [String:Any] ?? [:]
        peerState.stringValue=(peer["ready"] as? Bool ?? false) ? "Peer: verificado" : "Peer: sin verificar"
        let phase=value["task_phase"] as? String ?? "none"
        let names=["none":"sin tarea observada","accepted_only":"aceptada; continuación no observada","activity_observed":"actividad observada por avance","final_observed":"final completed observado; no es validación","final_interrupted":"interrumpido confirmado","final_failed":"falló; no completado correctamente","final_unknown":"estado final no reconocido","uncertain":"resultado incierto; consultar recibo"]
        noteLabel.stringValue=(value["note"] as? String ?? "")+"\nÚltimo encargo del puente: "+(names[phase] ?? "sin evidencia suficiente")
        stopButton.isEnabled=value["owned"] as? Bool ?? false
    }
    @objc func detect() {
        perform(["discover"]) { object in
            guard let value=self.unwrap(object) else { return }
            let rows=value["sessions"] as? [[String:Any]] ?? []
            self.sessions.removeAllItems();self.sessionIDs=[]
            for row in rows {self.sessions.addItem(withTitle:row["label"] as? String ?? "Sesión cargada");self.sessionIDs.append(row["id"] as? String ?? "")}
            if let chosen=value["selected"] as? String,let index=self.sessionIDs.firstIndex(of:chosen){self.sessions.selectItem(at:index)}
            self.perform(["lan-status"]) { metadata in
                if let data=metadata["data"] as? [String:Any],let addresses=data["interfaces"] as? [String] {
                    self.networkIDs=addresses;self.network.removeAllItems()
                    self.network.addItem(withTitle:"Detectar red local automáticamente")
                    for address in addresses {self.network.addItem(withTitle:address)}
                }
                self.refresh()
            }
        }
    }
    @objc func connectSelected() {
        let index=sessions.indexOfSelectedItem
        guard index>=0 && index<sessionIDs.count else {noteLabel.stringValue="Abre tu conversación existente en Codex y pulsa Detectar.";return}
        perform(["connect","--thread",sessionIDs[index],"--coordinator",coordinator.stringValue]) { object in
            if let value=self.unwrap(object){self.updateStatus(value);self.beganOwnedConnector=value["started_by_this_call"] as? Bool ?? false;self.monitoringPeer=true}
        }
    }
    func lanArguments(_ action: String) -> [String]? {
        let index=sessions.indexOfSelectedItem
        guard index>=0 && index<sessionIDs.count else {noteLabel.stringValue="Selecciona tu conversación existente.";return nil}
        let modes=["auto","lan","relay"]
        var args=[action,"--thread",sessionIDs[index],"--approve-communication","--mode",modes[max(0,transportMode.indexOfSelectedItem)]]
        if allowDelegation.state == .on {args += ["--approve-delegation"]}
        let selected=network.indexOfSelectedItem-1
        if selected>=0 && selected<networkIDs.count {args += ["--bind",networkIDs[selected]]}
        return args
    }
    func communicationConsent() -> Bool {
        let alert=NSAlert();alert.messageText="Conectar estos agentes"
        alert.informativeText="Permite encargos, aclaraciones, avances, estado y resultados entre este par. Activa sólo herramientas de comunicación de esta conversación. No concede shell, historial, login ni aprobaciones remotas. El sistema puede solicitar permiso de red; relay sólo abre conexiones salientes. Si seleccionaste encargos automáticos, followup puede activar al agente inactivo y consumir tokens/costo; no incluye interrupt automático."
        alert.addButton(withTitle:"Permitir y continuar");alert.addButton(withTitle:"Cancelar")
        return alert.runModal() == .alertFirstButtonReturn
    }
    @objc func shareLAN() {
        guard let args=lanArguments("lan-share"),communicationConsent() else {return}
        perform(args) { object in
            guard let data=self.unwrap(object),let code=data["code"] as? String else {return}
            self.beganOwnedLAN=self.beganOwnedLAN || (data["lan_session_owned"] as? Bool ?? false)
            self.beganOwnedConnector=self.beganOwnedConnector || (data["connector_owned"] as? Bool ?? false)
            self.monitoringPeer=true
            let alert=NSAlert();alert.messageText="Código privado: "+code
            let tools=data["communication_tools"] as? [String:Any]
            let warning=(tools?["installed"] as? Bool == false) ? "ATENCIÓN: herramientas del agente no instaladas; revisa la configuración local. CLI sigue disponible.\n\n" : ""
            alert.informativeText=warning+"Copia y pega el código en la otra utilidad. Caduca en cinco minutos y funciona una vez. Si usas LAN y UDP está bloqueado, copia el enlace privado. Relay usa el mismo código y requiere el perfil privado provisionado. Emparejar no demuestra que el modelo haya avanzado."
            let relayed=data["mode"] as? String == "outgoing-relay"
            alert.addButton(withTitle:"Copiar código");alert.addButton(withTitle:relayed ? "Cerrar" : "Copiar enlace")
            let response=alert.runModal()
            if relayed && response != .alertFirstButtonReturn {return}
            let selected=response == .alertFirstButtonReturn ? code : data["link"] as? String ?? code
            NSPasteboard.general.clearContents();NSPasteboard.general.setString(selected,forType:.string)
            self.noteLabel.stringValue="Invitación copiada manualmente. Esperando al otro equipo; emparejar no es un encargo."
        }
    }
    @objc func reconcileLAN() {
        perform(["lan-reconcile"]) { object in if let data=self.unwrap(object) {self.noteLabel.stringValue=data["note"] as? String ?? "Recibo de emparejamiento consultado; no se creó otro permiso.";if data["state"] as? String == "paired" {self.monitoringPeer=true;self.refresh()}} }
    }
    @objc func joinLAN() {
        guard let args=lanArguments("lan-connect"),communicationConsent() else {return}
        let invitation=pairCode.stringValue
        pairCode.stringValue=""
        perform(args+["--pair-code-stdin"],input:invitation) { object in
            if let data=self.unwrap(object) {self.beganOwnedLAN=self.beganOwnedLAN || (data["lan_session_owned"] as? Bool ?? false);self.beganOwnedConnector=self.beganOwnedConnector || (data["connector_owned"] as? Bool ?? false);if data["state"] as? String != "paired" {self.statusLabel.stringValue="Emparejamiento sin confirmar";self.noteLabel.stringValue=data["note"] as? String ?? "Consulta el recibo antes de reintentar.";return};self.beganOwnedLAN=self.beganOwnedLAN || (data["lan_session_owned"] as? Bool ?? false);self.beganOwnedConnector=self.beganOwnedConnector || (data["connector_owned"] as? Bool ?? false);self.monitoringPeer=true;self.noteLabel.stringValue=data["note"] as? String ?? "Par autenticado.";let tools=data["communication_tools"] as? [String:Any];if tools?["installed"] as? Bool == false {self.statusLabel.stringValue="Par autenticado; herramientas sin instalar";self.noteLabel.stringValue="Revisa configuración/política local. Pairing no acredita coordinación automática."} else {self.refresh()}}
        }
    }
    @objc func refresh() {
        perform(monitoringPeer ? ["status","--verify-peer"] : ["status"]) { object in
            if let value=self.unwrap(object){self.updateStatus(value)}
            self.loadEvents()
        }
    }
    func loadEvents() {
        perform(["events","--after",String(cursor)]) { object in
            guard object["ok"] as? Bool == true,let data=object["data"] as? [String:Any],let rows=data["events"] as? [[String:Any]] else { return }
            for row in rows {
                self.cursor=max(self.cursor,row["cursor"] as? Int ?? self.cursor)
                let kind=row["kind"] as? String ?? "evento",status=row["status"] as? String ?? ""
                self.activity.string += "\(kind) · \(status)\n\(row["text"] as? String ?? "")\n\n"
            }
        }
    }
    @objc func changePage() {connectView.isHidden=pages.selectedSegment==1;activityView.isHidden=pages.selectedSegment==0;if pages.selectedSegment==1 {refresh()}}
    @objc func stopOwned() {perform(["stop"]) { object in if let data=self.unwrap(object){self.noteLabel.stringValue=data["note"] as? String ?? "Deteniendo conector…"}}}
    @objc func copyConnection() {
        perform(["connection-info"]) { object in
            guard let value=self.unwrap(object),let data=try? JSONSerialization.data(withJSONObject:value,options:[.prettyPrinted,.sortedKeys]),let text=String(data:data,encoding:.utf8) else{return}
            NSPasteboard.general.clearContents();NSPasteboard.general.setString(text,forType:.string)
            self.noteLabel.stringValue="Configuración de conexión copiada; no contiene claves ni login."
        }
    }
    @objc func installTools() {
        let alert=NSAlert();alert.messageText="Permitir comunicación del agente"
        alert.informativeText="Añade o actualiza sólo csl_bridge: enviar avances de su tarea activa y consultas. No cambia login, modelo ni permisos shell. Guarda rollback local de esa sección."
        alert.addButton(withTitle:"Activar herramientas");alert.addButton(withTitle:"Cancelar")
        if alert.runModal() != .alertFirstButtonReturn {return}
        perform(["mcp-install"]) { object in if let value=self.unwrap(object){self.noteLabel.stringValue=value["note"] as? String ?? "Herramientas configuradas."}}
    }
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool { return true }
    func applicationWillTerminate(_ notification: Notification) {
        if renderDirectory == nil && beganOwnedLAN { _ = backend(["lan-stop"]) }
        if renderDirectory == nil && beganOwnedConnector { _ = backend(["stop"]) }
    }
    func renderPreviews(_ path: String) {
        window.contentView!.wantsLayer = true
        window.contentView!.layer?.backgroundColor = NSColor.white.cgColor
        let directory=URL(fileURLWithPath:path);try? FileManager.default.createDirectory(at:directory,withIntermediateDirectories:true)
        // Render actual AppKit controls without screen-capture or Accessibility permissions.
        let (_,discoveryData)=backend(["discover"])
        if let object=(try? JSONSerialization.jsonObject(with:discoveryData)) as? [String:Any],let data=object["data"] as? [String:Any],let rows=data["sessions"] as? [[String:Any]] {
            sessions.removeAllItems();for row in rows{sessions.addItem(withTitle:row["label"] as? String ?? "Sesión")}
        }
        let (_,statusData)=backend(["status"])
        if let object=(try? JSONSerialization.jsonObject(with:statusData)) as? [String:Any],let data=object["data"] as? [String:Any]{updateStatus(data)}
        activity.string="Sin mensajes nuevos en esta vista.\nActualiza para consultar los eventos del conector."
        for index in 0...1 {
            pages.selectedSegment=index;connectView.isHidden=index==1;activityView.isHidden=index==0
            window.contentView!.layoutSubtreeIfNeeded();window.displayIfNeeded()
            if let bitmap=window.contentView!.bitmapImageRepForCachingDisplay(in:window.contentView!.bounds){
                window.contentView!.cacheDisplay(in:window.contentView!.bounds,to:bitmap)
                if let png=bitmap.representation(using:.png,properties:[:]){try? png.write(to:directory.appendingPathComponent(index==0 ? "session.png":"activity.png"))}
            }
        }
        NSApplication.shared.terminate(nil)
    }
}

let arguments=Array(CommandLine.arguments.dropFirst())
if !arguments.isEmpty && arguments[0] != "--render-previews" {
    let body=arguments.contains("--pair-code-stdin") ? FileHandle.standardInput.readData(ofLength: 4097) : nil
    let (status,data)=backend(arguments,input:body);FileHandle.standardOutput.write(data);exit(status)
}
let application=NSApplication.shared
if arguments.first == "--render-previews" { application.appearance=NSAppearance(named: .aqua) }
let delegate=App()
if arguments.first == "--render-previews",arguments.count==2{delegate.renderDirectory=arguments[1]}
application.delegate=delegate;application.setActivationPolicy(.regular);application.run()
