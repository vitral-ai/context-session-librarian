"""Public command coverage registry. Keep docs/PROVIDER_MATRIX.md in sync."""

import argparse
import json

import csl_claude


CODEX_ONLY = ("index", "search", "recent", "show", "related", "find-work", "patches", "pinpoint", "recall",
              "why", "summarize", "annotate", "queue", "cockpit", "timeline", "cut", "native-cut", "native-clone",
              "native-sanitize", "stats")
SHARED = ("export", "import", "share", "receive", "inbox", "team", "sessions", "capabilities", "doctor")


def coverage():
    commands = {name: {"codex": "supported", "claude": "not-integrated"} for name in CODEX_ONLY}
    commands.update({name: {"codex": "supported", "claude": "supported"} for name in SHARED})
    commands["convert"] = {"codex": "experimental", "claude": "experimental"}
    commands["verify-live"] = {"codex": "experimental", "claude": "experimental"}
    commands["migrate"] = {"codex": "experimental", "claude": "not-integrated"}
    commands["backup"] = {"codex": "experimental", "claude": "not-integrated"}
    commands["bridge"] = {"codex": "experimental", "claude": "experimental"}
    commands["sync"] = {"codex": "experimental", "claude": "experimental"}
    for name in ("native-cut", "native-clone", "native-sanitize"):
        commands[name]["codex"] = "experimental"
    for status in commands.values():
        status['gemini'] = 'not-integrated'
    commands['sessions']['gemini'] = 'experimental'
    commands['export']['gemini'] = 'experimental'
    commands['import']['gemini'] = 'experimental'
    commands['providers'] = {provider:'experimental' for provider in ('codex','claude','gemini')}
    # The collector syncs whatever the provider interface inventories, Gemini included.
    commands['sync']['gemini'] = 'experimental'
    return {"schema_version": 1, "providers": ["codex", "claude", "gemini"], "commands": commands,
            "sync_providers": {"interface_version":1,"module":"csl_sync_providers",
                               "operations":["list_sessions","raw_files","view_records","package_descriptor"],
                               "network":False,"model_calls":False,"native_package_version":3,
                               "gemini_native_resume":"unverified; CLI unavailable at implementation start"},
            "source_lookup": {"default": "auto", "commands": ["export", "share", "convert", "doctor"],
                              "cross_cli_ambiguity": "require --provider codex or --provider claude",
                              "within_cli_ambiguity": "require a more specific UUID or file path"},
            "routes": {"codex->codex": "native", "claude->claude": "native",
                       "codex->claude": "experimental-multimodal", "claude->codex": "experimental-multimodal"},
            "canonical": {"format": "csl-conversation", "version": 2, "profile": "text-tools-references-images-pdf",
                          "assets": "embedded-only-optional-decoder", "source_policy": "quarantined"},
            "continuity_checks": {"default_import": "structural-and-static-context",
                                  "doctor_native": "isolated-localhost-request-no-generation",
                                  "live_continuation": "opt-in-bounded-synthetic-suite"},
            "native_picker_index": {"provider": "codex", "manual": "index --native",
                "automatic": "best-effort after import/receive; once per migration attempt with completed imports",
                "method": "native thread/list scan-and-repair; no inference or direct native SQLite edits"},
            "backup": {"provider": "codex", "status": "experimental-development",
                "manual_full_checkpoints": True, "incremental_reuse": True, "durable_resume": True,
                "multiple_sources": True, "partial_checkpoints": True, "catalog_reader_schemas": [1, 2],
                "catalog_writer_schema": 2, "evidence_policy": "light-each-run-full-30d-v1",
                "future_recipient_rotation": "explicit source-scoped keys plan/apply; public epoch map",
                "historical_reencryption": False, "native_sanitized_profile": False,
                "scheduling": False, "pruning": False,
                "profiles": ["native-age-v1", "portable-age-v1"], "encryption": "age-v1-x25519; objects and detailed catalogs",
                "custody": "explicit manual stdin or system-keyring reference; external recovery identity required",
                "scope": "history only; not projects/auth, model continuity or a durability SLA"},
            "native_clone_visibility": {"provider": "codex", "default": "interactive",
                                        "agent": "persistent-exec-source-excluded-from-default-resume-picker",
                                        "archived": "new-clone-in-archived_sessions",
                                        "scope": "experimental; presentation filter, not access control"},
            "legacy_compaction_recovery": {"provider": "codex", "default_enabled": False,
                "commands": ["export", "share", "migrate upload"], "mode": "portable-only",
                "scope": "explicit recognized orphaned-summary-metadata recovery; exact hidden context not restored"},
            "authentication": "Recipient CLI owns authentication; CSL never copies credentials or selects billing accounts.",
            "bridge_messaging": {"status": "experimental-development", "message_version": 1,
                "providers": ["codex", "claude"], "operations": ["send", "receive", "ack", "reply", "inbox-status"],
                "notification_metadata": "own-endpoint unacknowledged counts and bounded id/from/kind headers only; no wake or acknowledgement",
                "grant_inspection": "CLI and own-endpoint MCP list/show include descriptive scope, expiry and revocation",
                "grant_mutation": "local owner CLI; MCP requires explicit owner --grant-admin startup capability, scoped to bound endpoint",
                "boundary_hook": "Session-bound SessionStart/UserPromptSubmit/PostToolUse/Stop adapter; metadata default, body explicit owner opt-in; no acknowledgement or idle wake",
                "delivery": "durable local inbox; explicit receive in the existing session",
                "model_activation": "mail operations never activate; separate owner notify --wake-codex requires recipient-local activate grant and reviewed Codex executable",
                "executes_work": False,
                "authority": "recipient-owner registry, separate from inbox admission; none without a matching grant",
                "identity": "operator registration and authenticated peer binding, not process attestation",
                "validation": "real reciprocal Codex/Claude CLI mailbox exchange and scoped agent actions; synthetic fault injection around real connectors; hook harness evidence tracked separately in docs/AGENT_COMMUNICATION.md",
                "legacy_coordination": "Codex only; unchanged"},
            "continuation": "Native file transfer is not a guarantee of model availability, context fit, or universal CLI-version compatibility."}


def markdown():
    lines = ["| Command | Codex | Claude Code | Gemini CLI |", "| --- | --- | --- | --- |"]
    for name, status in sorted(coverage()["commands"].items()):
        lines.append(f"| `{name}` | {status['codex']} | {status['claude']} | {status['gemini']} |")
    return "\n".join(lines)


def capabilities(args):
    print(json.dumps(coverage(), indent=2) if args.json else markdown())


def sessions(args, csl):
    from csl_transfer import read_records, safe_text
    if args.provider in ("claude", "gemini"):
        from csl_sync_providers import list_sessions, raw_files
        rows=[]
        for entry in list_sessions(args.provider):
            paths=raw_files(args.provider,entry['native_id'])
            rows.append({**entry,'id':entry['native_id'],'path':paths[0]['path']})
    else:
        conn = csl.init_db(args.db.expanduser())
        try:
            rows = [dict(row) for row in conn.execute(
                "SELECT session_id AS id,thread_name AS title,path FROM sessions "
                "ORDER BY coalesce(updated_at,started_at) DESC")]
        finally:
            conn.close()
        rows = [{**row, "provider": "codex"} for row in rows]
    if args.query:
        rows = [row for row in rows if args.query.casefold() in (str(row["title"]) + " " + row["id"]).casefold()]
    rows = rows[:args.limit]
    if args.json:
        print(json.dumps({"provider": args.provider, "sessions": rows}, ensure_ascii=False))
    else:
        for row in rows:
            print(f"{row['id']}  [{args.provider}]  {safe_text(row['title'])}\n  {row['path']}")


def positive_limit(value):
    number = int(value)
    if not 1 <= number <= 1000:
        raise argparse.ArgumentTypeError("limit must be between 1 and 1000")
    return number


def add_commands(subparsers, csl):
    caps = subparsers.add_parser("capabilities", help="Show the maintained command/provider coverage matrix.")
    caps.add_argument("--json", action="store_true", help="Machine-readable command and transfer-route coverage.")
    caps.set_defaults(func=capabilities)
    listing = subparsers.add_parser("sessions", help="List sessions for one CLI; Codex uses its index, Claude reads transcripts.")
    listing.add_argument("--provider", choices=("codex", "claude", "gemini"), default="codex")
    listing.add_argument("--limit", type=positive_limit, default=20)
    listing.add_argument("--query", help="Filter session title or UUID; not full-history search.")
    listing.add_argument("--json", action="store_true")
    listing.set_defaults(func=lambda args: sessions(args, csl))
    provider = subparsers.add_parser('providers',help='Read local provider metadata, raw-file inventory and incremental views; no network.')
    provider.add_argument('operation',choices=('list','files','view','package'))
    provider.add_argument('--provider',choices=('codex','claude','gemini'),required=True)
    provider.add_argument('--session')
    provider.add_argument('--home',type=__import__('pathlib').Path)
    provider.add_argument('--position',help='Opaque JSON position returned by a previous view page.')
    provider.add_argument('--limit',type=positive_limit,default=500)
    provider.set_defaults(func=provider_command)


def provider_command(args):
    import csl_sync_providers as p
    if args.operation=='list': result=p.list_sessions(args.provider,home=args.home)
    else:
        if not args.session:raise ValueError('Specify --session for this provider operation')
        if args.operation=='files': result=p.raw_files(args.provider,args.session,home=args.home)
        elif args.operation=='package':result=p.package_descriptor(args.provider,args.session,home=args.home)
        else:result=p.view_records(args.provider,args.session,position=json.loads(args.position) if args.position else None,limit=args.limit,home=args.home)
    print(json.dumps(result,ensure_ascii=False))
