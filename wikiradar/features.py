"""Event-time windowing and window features, shared by train, processor and evaluate."""

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import numpy as np

from wikiradar.schema import Edit

WINDOW = timedelta(seconds=10)
LATENESS = timedelta(seconds=5)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
FEATURE_NAMES = (
    "edit_count", "bot_ratio", "anon_ratio", "new_page_count", "mean_abs_delta", "min_delta",
    "revert_ratio", "top_title_share", "unique_users_ratio", "hour_sin", "hour_cos",
)


@dataclass
class ClosedWindow:
    start: datetime
    edits: list[Edit] = field(default_factory=list)

    @property
    def end(self) -> datetime:
        return self.start + WINDOW


def window_start(dt: datetime) -> datetime:
    return dt - (dt - EPOCH) % WINDOW


class WindowAggregator:
    """10 s tumbling windows; a window closes once its end <= watermark. No I/O.

    Watermark = 5 s before the slowest source's latest event time. The processor passes one source
    per Kafka partition, so catching up on one partition's backlog cannot close windows the others
    are still filling, and calls idle() for a partition that has gone quiet. Files are one source.
    """

    def __init__(self, sources: int = 1):
        self.buckets: dict[datetime, list[Edit]] = {}
        self.waiting = set(range(sources))
        self.source_max: dict[int, datetime] = {}
        self.closed_until: datetime | None = None
        self.late_dropped = 0

    def add(self, edit: Edit, source: int = 0) -> list[ClosedWindow]:
        start = window_start(edit.dt)
        if self.closed_until and start < self.closed_until:
            self.late_dropped += 1
            return []
        self.buckets.setdefault(start, []).append(edit)
        self.waiting.discard(source)
        self.source_max[source] = max(self.source_max.get(source, edit.dt), edit.dt)
        return self._advance()

    def idle(self, source: int) -> list[ClosedWindow]:
        """Stop holding the watermark for a quiet source until it sends again."""
        self.waiting.discard(source)
        self.source_max.pop(source, None)
        return self._advance()

    def flush(self) -> list[ClosedWindow]:
        return self._close_until(max(self.buckets) + WINDOW) if self.buckets else []

    def _advance(self) -> list[ClosedWindow]:
        if self.waiting or not self.source_max:
            return []
        return self._close_until(min(self.source_max.values()) - LATENESS)

    def _close_until(self, watermark: datetime) -> list[ClosedWindow]:
        """Emit every window ending at or before the watermark, empty ones included."""
        closed = []
        start = self.closed_until or min(self.buckets)
        while start + WINDOW <= watermark:
            closed.append(ClosedWindow(start, self.buckets.pop(start, [])))
            start += WINDOW
            self.closed_until = start
        return closed


def featurize(window: ClosedWindow) -> np.ndarray:
    """Feature vector in FEATURE_NAMES order (PLAN 3.7)."""
    edits = window.edits
    n = len(edits)
    angle = 2 * math.pi * window.start.hour / 24
    hour = [math.sin(angle), math.cos(angle)]
    if n == 0:
        return np.array([0.0] * (len(FEATURE_NAMES) - 2) + hour)
    min_delta = min(e.byte_delta for e in edits)
    return np.array([
        math.log1p(n),
        sum(e.is_bot for e in edits) / n,
        sum(e.is_anon for e in edits) / n,
        math.log1p(sum(e.is_new for e in edits)),
        math.log1p(sum(abs(e.byte_delta) for e in edits) / n),
        -math.log1p(-min_delta) if min_delta < 0 else math.log1p(min_delta),
        sum(e.is_revert for e in edits) / n,
        Counter(e.title for e in edits).most_common(1)[0][1] / n,
        len({e.user for e in edits}) / n,
        *hour,
    ])
