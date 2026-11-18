"""A8 contract fixture for a stop witness and matching final acknowledgments."""
from dataclasses import dataclass
import re


@dataclass(frozen=True)
class Capture:
    run: str
    source_generation: str
    content_revision: str
    length: int
    sha256: str
    view_generation: str
    view_last_seq: int

    def valid(self):
        identifiers = (self.run, self.source_generation, self.content_revision, self.view_generation)
        return (all(type(value) is str and value for value in identifiers)
                and type(self.sha256) is str and bool(re.fullmatch('[0-9a-f]{64}', self.sha256))
                and all(type(value) is int and value >= 0
                        for value in (self.length, self.view_last_seq)))


@dataclass(frozen=True)
class SupervisedStop:
    """Constructed by the owned fixture supervisor, never decoded from input."""
    run: str
    source_generation: str
    stopped_at: int


@dataclass(frozen=True)
class ContentAck:
    capture: Capture
    observed_at: int


@dataclass(frozen=True)
class ViewAck:
    capture: Capture
    observed_at: int


DRAIN_BUDGET_NS = 20_000_000_000


def outcome(capture, stop, eof_at, content_ack, view_ack, started_at, deadline):
    if (any(type(value) is not int or value < 0 for value in (started_at, deadline))
            or not 0 < deadline - started_at <= DRAIN_BUDGET_NS):
        return 'incomplete'
    if type(capture) is not Capture or not capture.valid():
        return 'incomplete'
    if type(stop) is not SupervisedStop:
        return 'pending'
    if (stop.run, stop.source_generation) != (capture.run, capture.source_generation):
        return 'pending'
    if (any(type(value) is not int for value in (stop.stopped_at, eof_at))
            or not started_at <= stop.stopped_at <= eof_at < deadline):
        return 'incomplete'
    for receipt_type, receipt in ((ContentAck, content_ack), (ViewAck, view_ack)):
        if (type(receipt) is not receipt_type or type(receipt.capture) is not Capture
                or not receipt.capture.valid() or receipt.capture != capture
                or type(receipt.observed_at) is not int
                or not eof_at <= receipt.observed_at < deadline):
            return 'pending'
    return 'complete'


def healthy_observation(row):
    """Validate a trusted observer's serialized timeline, not a stop authority."""
    if row['producer_mode'] != 'sole_supervised':
        return False
    timeline = row['timeline']
    capture = Capture(**timeline['capture'])
    stop = SupervisedStop(**timeline['stop'])
    content = timeline['content_ack']
    view = timeline['view_ack']
    if content['kind'] != 'content' or view['kind'] != 'view':
        return False
    content_ack = ContentAck(Capture(**content['capture']), content['observed_at'])
    view_ack = ViewAck(Capture(**view['capture']), view['observed_at'])
    start = timeline['started_at']
    result = outcome(capture, stop, timeline['eof_at'], content_ack, view_ack,
                     start, timeline['deadline'])
    return (result == 'complete' and row['outcome'] == result
            and type(row['elapsed_ns']) is int
            and row['elapsed_ns'] == max(content_ack.observed_at, view_ack.observed_at) - start)
