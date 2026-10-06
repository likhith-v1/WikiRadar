from datetime import datetime, timedelta, timezone

import pytest

from wikiradar.features import FEATURE_NAMES, ClosedWindow, WindowAggregator, featurize, windows_from_edits
from wikiradar.schema import Edit

T0 = datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc)


def edit(seconds: float, **overrides) -> Edit:
    fields = dict(
        event_id=f"id-{seconds}-{overrides.get('title', 'A')}-{overrides.get('user', 'u')}",
        dt=T0 + timedelta(seconds=seconds),
        title="A",
        title_url="https://en.wikipedia.org/wiki/A",
        user="u",
        is_bot=False,
        is_anon=False,
        is_new=False,
        byte_delta=10,
        is_revert=False,
        rev_id=1,
    )
    fields.update(overrides)
    return Edit(**fields)


def test_windows_align_to_epoch_and_close_at_watermark():
    agg = WindowAggregator()
    assert agg.add(edit(3)) == []
    assert agg.add(edit(14.9)) == []  # watermark 9.9 < window end 10
    closed = agg.add(edit(15))  # watermark 10 closes [0, 10)
    assert [(w.start, w.end) for w in closed] == [(T0, T0 + timedelta(seconds=10))]
    assert [e.dt for e in closed[0].edits] == [T0 + timedelta(seconds=3)]


def test_late_event_for_closed_window_is_dropped_and_counted():
    agg = WindowAggregator()
    agg.add(edit(3))
    agg.add(edit(15))  # closes [0, 10)
    assert agg.add(edit(4)) == []
    assert agg.late_dropped == 1
    closed = agg.add(edit(25))  # closes [10, 20)
    assert [len(w.edits) for w in closed] == [1]


def test_out_of_order_event_within_watermark_lands_in_its_window():
    agg = WindowAggregator()
    agg.add(edit(12))
    agg.add(edit(8))  # older, but [0, 10) is still open (watermark 7)
    closed = agg.add(edit(25))
    assert [(w.start - T0).seconds for w in closed] == [0, 10]
    assert [[e.dt.second for e in w.edits] for w in closed] == [[8], [12]]


def test_gap_emits_empty_windows():
    agg = WindowAggregator()
    agg.add(edit(3))
    closed = agg.add(edit(45))  # watermark 40 closes [0,10) .. [30,40)
    assert [(w.start - T0).seconds for w in closed] == [0, 10, 20, 30]
    assert [len(w.edits) for w in closed] == [1, 0, 0, 0]


def test_flush_closes_remaining_windows():
    agg = WindowAggregator()
    agg.add(edit(3))
    agg.add(edit(12))  # watermark 7: nothing closed yet
    closed = agg.flush()
    assert [((w.start - T0).seconds, len(w.edits)) for w in closed] == [(0, 1), (10, 1)]
    assert agg.flush() == []


def test_features_of_a_hand_built_window():
    window = ClosedWindow(T0, [  # T0 is 12:00 UTC
        edit(1, user="a", is_bot=True, byte_delta=100),
        edit(2, user="b", is_anon=True, byte_delta=-300, is_revert=True),
        edit(3, user="b", is_anon=True, byte_delta=20),
        edit(4, user="c", title="B", is_new=True, byte_delta=500),
    ])
    expected = {
        "edit_count": 1.6094379,  # ln(5)
        "bot_ratio": 0.25,
        "anon_ratio": 0.5,
        "new_page_count": 0.6931472,  # ln(2)
        "mean_abs_delta": 5.4424177,  # ln(1 + 230)
        "min_delta": -5.7071103,  # -ln(301)
        "revert_ratio": 0.25,
        "top_title_share": 0.75,
        "unique_users_ratio": 0.75,
        "hour_sin": 0.0,
        "hour_cos": -1.0,
    }
    assert list(FEATURE_NAMES) == list(expected)
    assert featurize(window) == pytest.approx(list(expected.values()), abs=1e-6)


def test_empty_window_features_are_zero_except_hour():
    x = featurize(ClosedWindow(T0 + timedelta(hours=6)))  # 18:00 UTC
    assert x[:-2] == pytest.approx([0.0] * 9)
    assert x[-2:] == pytest.approx([-1.0, 0.0], abs=1e-9)


def test_watermark_follows_the_slowest_source():
    agg = WindowAggregator(sources=2)
    assert agg.add(edit(3), source=0) == []
    assert agg.add(edit(45), source=0) == []  # source 1 not seen yet: nothing closes
    closed = agg.add(edit(16), source=1)  # watermark min(45, 16) - 5 = 11
    assert [(w.start - T0).seconds for w in closed] == [0]
    assert agg.add(edit(14), source=1) == []  # still on time for source 1's window [10, 20)
    assert agg.late_dropped == 0


def test_idle_source_stops_holding_back_the_watermark():
    agg = WindowAggregator(sources=2)
    agg.add(edit(3), source=0)
    assert agg.add(edit(25), source=0) == []  # still waiting for source 1
    closed = agg.idle(1)  # watermark 20 from source 0 alone
    assert [(w.start - T0).seconds for w in closed] == [0, 10]
    assert agg.add(edit(36), source=1) == []  # back again: source 0 (at 25) is the slowest now
    assert [(w.start - T0).seconds for w in agg.add(edit(37), source=0)] == [20]


def test_windows_from_edits_sorts_and_flushes():
    windows = windows_from_edits([edit(24), edit(3), edit(12)])  # unsorted, like a merged attack stream
    assert [((w.start - T0).seconds, len(w.edits)) for w in windows] == [(0, 1), (10, 1), (20, 1)]
