"""Check sanitized A8 fixture observations measured by one monotonic clock."""
import json
import sys

from check_producer_stop import DRAIN_BUDGET_NS, healthy_observation


ACK_BUDGET_NS = 3_000_000_000
UI_BUDGET_NS = 2_000_000_000


def check(document, expected_markers):
    """Return assertion names only, without transcript data or identity values."""
    failures = set()
    groups = {}
    seen = set()
    native_sessions = {}
    markers = document['markers']
    if not markers:
        failures.add('markers_missing')
    for row in markers:
        marker = row['id']
        if (marker in seen or type(row['visible_count']) is not int
                or row['visible_count'] != 1):
            failures.add('duplicate_or_missing_marker')
        seen.add(marker)
        written, acknowledged, visible = (row[field] for field in
                                         ('complete_write_ns', 'registry_ack_ns', 'ui_visible_ns'))
        if any(type(value) is not int or value < 0 for value in (written, acknowledged, visible)):
            failures.add('invalid_timing')
            continue
        if not 0 <= acknowledged - written <= ACK_BUDGET_NS:
            failures.add('registry_ack_budget')
        if not 0 <= visible - acknowledged <= UI_BUDGET_NS:
            failures.add('foreground_ui_budget')
        if not 0 <= visible - written <= ACK_BUDGET_NS + UI_BUDGET_NS:
            failures.add('end_to_end_budget')
        replicas = groups.setdefault(row['workload_group'], set())
        replica = row['replica']
        replicas.add(replica)
        pair = (row['workload_group'], row['synthetic_native_id'])
        native_sessions.setdefault(pair, {}).setdefault(replica, set()).add(row['opaque_session_id'])
        if row['view_only'] is not True:
            failures.add('view_only')
    if (type(expected_markers) is not list or not expected_markers
            or any(type(marker) is not str or not marker for marker in expected_markers)
            or len(set(expected_markers)) != len(expected_markers)
            or seen != set(expected_markers)):
        failures.add('expected_marker_inventory')
    if len(groups) != 1 or not any(len(replicas) >= 2 for replicas in groups.values()):
        failures.add('replica_grouping')
    if not any(len(replicas) >= 2 for replicas in native_sessions.values()):
        failures.add('native_id_collision_not_exercised')
    for replicas in native_sessions.values():
        if (any(len(session_ids) != 1 for session_ids in replicas.values())
                or len(set().union(*replicas.values())) != len(replicas)):
            failures.add('replica_namespace_or_retry')
    for name in ('opt_out_uploads', 'denied_uploads', 'other_account_records',
                 'off_origin_hits'):
        if type(document[name]) is not int or document[name] != 0:
            failures.add(name)
    leaks = document['canary_hits']
    for surface in ('content', 'metadata', 'view', 'package', 'ui', 'logs'):
        if type(leaks[surface]) is not int or leaks[surface] != 0:
            failures.add('canary_' + surface)
    for case in ('healthy', 'outage'):
        row = document['drain'][case]
        elapsed = row['elapsed_ns']
        if type(elapsed) is not int or not 0 <= elapsed <= DRAIN_BUDGET_NS:
            failures.add(case + '_drain_budget')
        if case == 'healthy':
            if not healthy_observation(row):
                failures.add('healthy_drain_unconfirmed')
        else:
            byte_ack, view_ack = row['all_final_bytes_acked'], row['all_final_view_acked']
            if (row['outcome'] not in ('pending', 'incomplete') or type(byte_ack) is not bool
                    or type(view_ack) is not bool or (byte_ack and view_ack)):
                failures.add('outage_false_completion')
            if (row['reconciled_after_restart'] is not True
                    or row['final_visible_count_after_restart'] != 1):
                failures.add('outage_reconciliation')
    if document['external_watch_outcome'] not in ('pending', 'incomplete'):
        failures.add('external_watch_false_completion')
    for receipt in ('content', 'view'):
        replay = document['lost_ack'][receipt]
        if (replay['committed_before_drop'] is not True
                or replay['same_replica_restarted'] is not True
                or replay['final_receipt_matches'] is not True
                or type(replay['visible_count']) is not int
                or replay['visible_count'] != 1):
            failures.add(receipt + '_lost_ack_reconciliation')
    return sorted(failures)


if __name__ == '__main__':
    try:
        with open(sys.argv[1], encoding='utf-8') as inventory:
            failures = check(json.load(sys.stdin), json.load(inventory))
    except (IndexError, KeyError, OSError, TypeError, ValueError):
        failures = ['invalid_fixture_observations']
    print(json.dumps({'classification': 'fixture-observations', 'failures': failures}))
    raise SystemExit(bool(failures))
