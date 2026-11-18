"""A8 contract fixture for a stop witness and matching final acknowledgments."""
from dataclasses import dataclass


@dataclass(frozen=True)
class Capture:
    run: str
    source_generation: str
    content_revision: str
    length: int
    sha256: str
    view_generation: str
    view_last_seq: int


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


def outcome(capture, stop, eof_at, content_ack, view_ack, deadline):
    if not isinstance(stop, SupervisedStop):
        return 'pending'
    if (stop.run, stop.source_generation) != (capture.run, capture.source_generation):
        return 'pending'
    if eof_at is None or not stop.stopped_at <= eof_at < deadline:
        return 'incomplete'
    for receipt_type, receipt in ((ContentAck, content_ack), (ViewAck, view_ack)):
        if (type(receipt) is not receipt_type or receipt.capture != capture
                or not eof_at <= receipt.observed_at < deadline):
            return 'pending'
    return 'complete'
