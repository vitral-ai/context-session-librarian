from copy import deepcopy
import unittest

from check_fixture import check as check_evidence


EXPECTED = ['fixture-1', 'fixture-2']


def check(evidence):
    return check_evidence(evidence, EXPECTED)


def sample():
    first = {'id': 'fixture-1', 'visible_count': 1, 'complete_write_ns': 100,
             'registry_ack_ns': 3_000_000_100, 'ui_visible_ns': 5_000_000_100,
             'workload_group': 'fixture-group', 'replica': 'replica-a', 'view_only': True,
             'synthetic_native_id': 'native-collision', 'opaque_session_id': 'opaque-a'}
    replay = {'committed_before_drop': True, 'same_replica_restarted': True,
              'final_receipt_matches': True, 'visible_count': 1}
    capture = {'run': 'run', 'source_generation': 'source', 'content_revision': 'revision',
               'length': 14, 'sha256': 'f' * 64, 'view_generation': 'view', 'view_last_seq': 2}
    timeline = {'started_at': 10, 'deadline': 1000, 'eof_at': 30, 'capture': capture,
                'stop': {'run': 'run', 'source_generation': 'source', 'stopped_at': 20},
                'content_ack': {'kind': 'content', 'capture': dict(capture), 'observed_at': 90},
                'view_ack': {'kind': 'view', 'capture': dict(capture), 'observed_at': 110}}
    return {'external_watch_outcome': 'pending',
            'lost_ack': {'content': dict(replay), 'view': dict(replay)},
            'markers': [first, first | {'id': 'fixture-2', 'replica': 'replica-b', 'opaque_session_id': 'opaque-b'}],
            'opt_out_uploads': 0, 'denied_uploads': 0, 'other_account_records': 0,
            'off_origin_hits': 0,
            'canary_hits': dict.fromkeys(('content', 'metadata', 'view', 'package', 'ui', 'logs'), 0),
            'drain': {'healthy': {'elapsed_ns': 100, 'outcome': 'complete',
                                  'producer_mode': 'sole_supervised', 'timeline': timeline},
                      'outage': {'elapsed_ns': 20_000_000_000, 'outcome': 'pending',
                                 'all_final_bytes_acked': False, 'all_final_view_acked': False,
                                 'reconciled_after_restart': True, 'final_visible_count_after_restart': 1}}}


class EvidenceCheckerTests(unittest.TestCase):
    def test_exact_budgets_and_two_replicas(self):
        self.assertEqual(check(sample()), [])

    def test_records_over_budgets_fail_without_averaging(self):
        evidence = sample()
        evidence['markers'][0]['registry_ack_ns'] += 1
        evidence['markers'][0]['ui_visible_ns'] += 1
        self.assertEqual(check(evidence), ['end_to_end_budget', 'registry_ack_budget'])

    def test_duplicate_visibility_and_outage_success_fail(self):
        evidence = deepcopy(sample())
        evidence['markers'][0]['visible_count'] = 2
        evidence['drain']['outage']['outcome'] = 'complete'
        self.assertEqual(check(evidence), ['duplicate_or_missing_marker', 'outage_false_completion'])

    def test_same_native_id_requires_distinct_server_ids(self):
        evidence = sample()
        evidence['markers'][1]['opaque_session_id'] = evidence['markers'][0]['opaque_session_id']
        self.assertEqual(check(evidence), ['replica_namespace_or_retry'])

    def test_final_view_ack_and_reconciliation_are_independent(self):
        evidence = sample()
        evidence['drain']['healthy']['timeline']['view_ack']['kind'] = 'content'
        evidence['drain']['outage']['final_visible_count_after_restart'] = 2
        evidence['canary_hits']['metadata'] = 1
        self.assertEqual(check(evidence), ['canary_metadata', 'healthy_drain_unconfirmed',
                                           'outage_reconciliation'])

    def test_lost_ack_cases_are_independent_and_external_watch_stays_pending(self):
        evidence = sample()
        evidence['lost_ack']['view']['same_replica_restarted'] = False
        evidence['external_watch_outcome'] = 'complete'
        self.assertEqual(check(evidence), ['external_watch_false_completion',
                                           'view_lost_ack_reconciliation'])

    def test_omitted_expected_marker_fails_even_with_two_observed_replicas(self):
        self.assertEqual(check_evidence(sample(), EXPECTED + ['missing']),
                         ['expected_marker_inventory'])

    def test_summary_refuses_conflicting_original_timeline(self):
        for change in ({'deadline': 31_000_000_010}, {'started_at': -1}, {'started_at': 11}):
            with self.subTest(change=change):
                evidence = sample()
                evidence['drain']['healthy']['timeline'].update(change)
                self.assertEqual(check(evidence), ['healthy_drain_unconfirmed'])
