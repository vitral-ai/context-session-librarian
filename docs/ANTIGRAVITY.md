# Antigravity CLI provider (experimental)

The observed profile is **Antigravity CLI 1.5.9 on Linux x64**. App, IDE,
remote-agent histories, other versions and operating systems are not verified.
Gemini CLI remains a separate provider.

## Operations

```bash
csl sessions --provider antigravity --json
csl providers list --provider antigravity
csl providers files --provider antigravity --session UUID
csl providers view --provider antigravity --session UUID
csl providers package --provider antigravity --session UUID
csl export UUID --provider antigravity --raw --to /private/session.csl.zip
csl import /private/session.csl.zip --list
csl import /private/session.csl.zip --cwd /local/project
agy --mode plan --conversation NEW_UUID
```

Storage defaults to `~/.gemini/antigravity-cli`. `CSL_ANTIGRAVITY_HOME` or the
provider API/CLI's `home`/`--home` overrides that storage root directly. Reading
requires neither authentication nor an installed native executable. Opening
requires `agy` and the user's authentication. The provider layer never makes
network calls, logs transcripts or invokes a model.

## Raw files and views

The real CLI generated `conversations/<UUID>.db` with `trajectory_meta`, `steps`,
`trajectory_metadata_blob`, `gen_metadata` and `executor_metadata`. Step blobs
use protobuf. The separate `conversation_summaries.db` is a global index, not
the history; it, credentials and global settings are excluded from packages.

Inventory preserves the main DB (`main`, role `transcript`) and an existing WAL
(`sqlite-wal`, role `context`), both `mode=rewrite`. WAL ends at its last
checksum-valid committed frame; partial/uncommitted tails are excluded.
Nonempty rollback journals cause an explicit refusal. SQL reads use private
copies with inventory/hash checks around the operation, never writes,
checkpoints, migrations or new SHM files on the origin. SHM is regenerated in
the private copy and is not packaged. Busy sources report
`antigravity_source_changing` for retry.

Views project user/assistant text, tool calls, and supported view-file,
run-command and MCP results. Native error status is marked. Unknown steps become
notices; thinking and other opaque information remain raw. Tools are historical
evidence and are not executed by reading or importing.

For SQLite, `source_files` binds all DB/WAL inputs to the inventory. A collector
must compare every entry before claiming a complete view. `source_file` remains
for compatibility. `raw_offset={file_key:main,offset:0}` identifies the whole DB;
`native_pointer=/steps/N` supplies the logical row. This is not a physical byte
offset. Cursor/sequence/reset semantics remain unchanged.

Titles currently derive from the first user text, not custom titles in the
global index. Unknown model enums produce `model=null`. Recently written means
`active`; it does not prove that a model process is running.

## Packages and native copies

V3 uses primary `main.db` and optional `files/session/session.db-wal`. Public
v0.3.0 cannot import v3. Import checks hashes, applies WAL in private staging,
creates new conversation/trajectory IDs and local cwd, then publishes an
independent DB. Archive bytes stay unchanged; installed bytes deliberately change.

Stored source executor/configuration/permission snapshots and executable agent
metadata are removed from the installed copy; the archive retains the evidence.
Historical project references in message text remain historical. This is not a
full environment restore or an identical-system-prompt guarantee.

Parent/subtrajectory-dependent and queued/running/waiting sessions remain
collectible but have an explicit unavailable resume descriptor. Title override
and static context-character limits on import are rejected as unsupported.
Portable conversion, team exchange, backup and bridge integration are not added.

## Verification and limitations

The real CLI created a small session using a localhost Gemini-compatible fixture,
then resumed an independent CSL-imported copy and sent prior user/assistant
content in its next request. Against a source-copy baseline, the two historical
messages match after excluding the known synthetic `USER_SETTINGS_CHANGE`
reminder, relocated because source permission configuration was removed. New
turn metadata includes fresh clocks and background notices, which are not
compared as historical content. Full requests
are **not byte-identical**.

This proves bounded native loading/request reconstruction, not model recall.
Endpoint responses and API keys were synthetic; no commercial model was called.
That automated fixture remains distinct from the authenticated check below.
Compaction-rich/subagent sessions, lazy
auxiliary reads, large DBs and cross-platform restoration remain unverified.

### Authenticated owner round trip (2026-11-15)

A separate manual check used CLI 1.5.9 with an existing sign-in and requested
`gemini-3.8-flash-low`. Two short real model turns were made. The first recorded
a random verification tag and a port. CSL listed, viewed, exported and imported
that native session with a new UUID in another disposable home. The resumed
model returned both earlier values correctly although neither was in the new
question. Source-file hashes stayed unchanged.

The second home referenced the same desktop keyring namespace; no credential
bytes were copied into a package, repository or report. Authentication remains
the user's responsibility, separate from the history package. This was a
same-owner text-only check, not cross-account portability or universal recall.

The real source used the same seven native tables and user/assistant step types
as the localhost fixture. A later resumed turn also recorded a native system
step (101), retained as an opaque notice in the view. No extra history-parent
dependency was observed for this example. Completed CLI runs checkpointed their
WAL; a loaded idle native session exposed an empty WAL, which CSL inventoried
alongside the DB. Nonempty committed-WAL coverage remains automated fixture
evidence, not a claim about that idle-session observation.

An additional restored copy loaded the real archive with network egress disabled
at the OS level and the desktop keyring hidden. Its request to a localhost
fixture contained the earlier tag and port without contacting Google. Thus
these basic historical messages were available from local files, not only an
opaque server identifier. This does not prove that authenticated mode never
consults a backend for history or that all attachments/opaque state are local.
Online authentication and real model generation still require external services.

```bash
python3 -m unittest discover -s tests -p test_antigravity.py -v
CSL_RUN_ANTIGRAVITY_NATIVE_TESTS=1 \
CSL_ANTIGRAVITY_BINARY=/private/runtime/antigravity \
python3 -m unittest discover -s tests -p test_antigravity.py -v
```

Tests do not download software or sign in; the native fixture uses disposable
homes and preserves origins. The core uses only Python's standard library.

## Source pins

- [Official Linux x64 1.5.9 build 4515810895593472](https://storage.googleapis.com/antigravity-public/antigravity-cli/1.5.9-4515810895593472/linux-x64/cli_linux_x64.tar.gz).
  SHA-512: `d01eb13ba7750eed18907f1ae8860864371e0ad9064415e8d033d22b4bd7b9b4e4c7f962058520b452d67db1c33163d9a889d55a7dffa159596ec6cd91a87b79`.
- [Official authentication](https://antigravity.google/docs/install?tab=cli),
  [resume](https://antigravity.google/docs/cli/commands/resume/) and
  [SQLite WAL format](https://www.sqlite.org/fileformat2.html#walformat), consulted
  2026-11-15 UTC.

The small protobuf field map was checked against the official binary's embedded
descriptors and generated fixture. It is not a stable Google format contract.
