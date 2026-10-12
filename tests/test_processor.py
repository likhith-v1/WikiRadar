import importlib.util
from pathlib import Path

import pytest

from test_features import edit
from wikiradar.schema import to_json

spec = importlib.util.spec_from_file_location("processor", Path(__file__).parent.parent / "scripts" / "processor.py")
processor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(processor)


class Msg:
    def __init__(self, partition, offset, seconds):
        self._partition, self._offset = partition, offset
        self._value = to_json(edit(seconds, title=f"p{partition}-{offset}"))

    def error(self):
        return None

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value


class ScriptEnded(Exception):
    pass


class Consumer:
    """Hands out scripted (wall clock, messages) batches and moves the fake clock with them."""

    def __init__(self, batches, clock):
        self.batches, self.clock = iter(batches), clock

    def consume(self, n, timeout):
        try:
            self.clock[0], msgs = next(self.batches)
        except StopIteration:
            raise ScriptEnded
        return msgs

    def commit(self, offsets, asynchronous):
        pass


class Conn:
    def commit(self):
        pass


def test_partitions_are_not_idled_by_the_first_batch_after_a_stream_wide_silence(monkeypatch):
    clock, rows, late = [0.0], [], []
    monkeypatch.setattr(processor.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(processor.db, "last_window_end", lambda conn: None)
    monkeypatch.setattr(processor.db, "late_dropped", lambda conn: 0)
    monkeypatch.setattr(processor.db, "insert_edits", lambda conn, edits: None)
    monkeypatch.setattr(processor.db, "upsert_window", lambda conn, row: rows.append(row))
    monkeypatch.setattr(processor.db, "update_current_state", lambda conn, as_of, n_late, score: late.append(n_late))
    monkeypatch.setattr(processor.db, "purge_old_edits", lambda conn: None)
    monkeypatch.setattr(processor, "window_row", lambda w, artifact: {
        "window_start": w.start, "window_end": w.end, "n_events": len(w.edits), "is_anomaly": False, "score": 0.0})

    batches = [
        (0.0, [Msg(0, 0, 1), Msg(1, 0, 2), Msg(2, 0, 3)]),
        # The producer was paused for 20 s. Its backlog arrives one partition at a time.
        (20.0, [Msg(0, 1, 4), Msg(0, 2, 18)]),
        (20.1, [Msg(1, 1, 6), Msg(2, 1, 7)]),
        (40.0, [Msg(0, 3, 41), Msg(1, 2, 41), Msg(2, 2, 41)]),
    ]
    with pytest.raises(ScriptEnded):
        processor.run(Consumer(batches, clock), Conn(), artifact={}, partitions=3, assigned=[], msgs=[])

    assert late[-1] == 0
    assert rows[0]["n_events"] == 6  # every edit stamped in the first 10 s, including the two that arrived last
