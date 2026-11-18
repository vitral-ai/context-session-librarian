from dataclasses import replace
import unittest

from check_producer_stop import Capture, ContentAck, SupervisedStop, ViewAck, outcome


class FinalOutcomeFixtures(unittest.TestCase):
    capture = Capture('invocation', 'source-generation', 'revision', 14, 'f' * 64,
                      'view-generation', 2)
    stop = SupervisedStop('invocation', 'source-generation', 10)
    content = ContentAck(capture, 12)
    view = ViewAck(capture, 12)

    def test_supervised_stop_eof_and_two_current_acks_complete(self):
        self.assertEqual(outcome(self.capture, self.stop, 11, self.content, self.view, 0, 30), 'complete')

    def test_external_sigterm_prefix_acks_or_marker_do_not_close(self):
        for witness in (None, True, {'run': 'invocation', 'stopped_at': 10}):
            with self.subTest(witness=witness):
                self.assertEqual(outcome(self.capture, witness, 11, self.content, self.view, 0, 30), 'pending')

    def test_old_or_missing_ack_never_closes_new_final_capture(self):
        stale_content = ContentAck(replace(self.capture, length=13), 12)
        stale_view = ViewAck(replace(self.capture, view_generation='old'), 12)
        for content, view in ((stale_content, self.view), (self.content, stale_view),
                              (self.content, None), (self.content, ViewAck(self.capture, 10)),
                              (self.content, ViewAck(self.capture, 30)),
                              (self.content, self.content), (self.view, self.view)):
            with self.subTest(content=content, view=view):
                self.assertEqual(outcome(self.capture, self.stop, 11, content, view, 0, 30), 'pending')

    def test_wrong_invocation_and_incomplete_eof_are_refused(self):
        wrong = replace(self.stop, run='old-replica')
        self.assertEqual(outcome(self.capture, wrong, 11, self.content, self.view, 0, 30), 'pending')
        for eof in (None, 9, 30):
            self.assertEqual(outcome(self.capture, self.stop, eof, self.content, self.view, 0, 30), 'incomplete')

    def test_reject_overlong_and_invalid_original_windows(self):
        for start, end in ((0, 31_000_000_000), (-5, 30), (False, 30),
                           (0, 30.0), (11, 30), (30, 30)):
            with self.subTest(start=start, end=end):
                self.assertNotEqual(outcome(self.capture, self.stop, 11, self.content,
                                            self.view, start, end), 'complete')

    def test_reject_negative_float_and_boolean_observations(self):
        for at in (-5, 12.0, True):
            with self.subTest(at=at):
                self.assertNotEqual(outcome(self.capture, self.stop, 11, self.content,
                                            ViewAck(self.capture, at), 0, 30), 'complete')

    def test_old_false_completion_reproductions(self):
        late = 30_000_000_000
        self.assertEqual(outcome(self.capture, replace(self.stop, stopped_at=0), 1,
                                 ContentAck(self.capture, late), ViewAck(self.capture, late),
                                 0, 31_000_000_000), 'incomplete')
        self.assertEqual(outcome(self.capture, replace(self.stop, stopped_at=-5), -4,
                                 ContentAck(self.capture, -3), ViewAck(self.capture, -3),
                                 -5, -1), 'incomplete')
