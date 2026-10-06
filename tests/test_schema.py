import json
from datetime import datetime, timezone

import pytest

from wikiradar.schema import from_json, normalize, to_json


def raw_event(**overrides):
    event = {
        "meta": {"id": "92ed07a0-a37c-4f36-909d-6afbffa72246", "dt": "2026-10-03T10:34:22.066Z"},
        "type": "edit",
        "wiki": "enwiki",
        "title": "Example",
        "title_url": "https://en.wikipedia.org/wiki/Example",
        "comment": "copyedit",
        "user": "Graham87",
        "bot": False,
        "length": {"old": 1964, "new": 3155},
        "revision": {"old": 1378079680, "new": 1378187221},
    }
    event.update(overrides)
    return event


def test_normalizes_an_edit():
    edit = normalize(raw_event())
    assert edit.event_id == "92ed07a0-a37c-4f36-909d-6afbffa72246"
    assert edit.dt == datetime(2026, 10, 3, 10, 34, 22, 66000, tzinfo=timezone.utc)
    assert edit.title == "Example"
    assert edit.user == "Graham87"
    assert edit.byte_delta == 1191
    assert edit.rev_id == 1378187221
    assert not (edit.is_bot or edit.is_anon or edit.is_new or edit.is_revert or edit.synthetic)


@pytest.mark.parametrize(
    "user, anon",
    [
        ("203.0.113.7", True),
        ("2001:DB8:0:0:0:0:0:1", True),
        ("~2026-53324-07", True),
        ("Graham87", False),
        ("1.5 Million", False),
    ],
)
def test_detects_anonymous_users(user, anon):
    assert normalize(raw_event(user=user)).is_anon is anon


@pytest.mark.parametrize(
    "comment, revert",
    [
        ("Undid revision 1378079680 by Foo (talk)", True),
        ("Reverted 2 edits by Foo (talk) to last revision by Bar", True),
        ("Reverted edits by Foo (talk) to last version by Bar", True),
        ("rv vandalism", True),
        ("Rollback of unexplained removal", True),
        ("reverting test edit", True),
        ("copyedit", False),
        ("Reverend biography expanded", False),
        ("", False),
    ],
)
def test_detects_reverts(comment, revert):
    assert normalize(raw_event(comment=comment)).is_revert is revert


def test_new_page_delta_is_its_full_length():
    edit = normalize(raw_event(type="new", length={"new": 5910}, revision={"new": 42}))
    assert edit.is_new
    assert edit.byte_delta == 5910


@pytest.mark.parametrize("field", ["meta", "title", "user", "length", "revision"])
def test_missing_fields_give_none(field):
    event = raw_event()
    del event[field]
    assert normalize(event) is None


def test_missing_comment_is_not_a_revert():
    event = raw_event()
    del event["comment"]
    assert normalize(event).is_revert is False


def test_to_json_writes_iso_utc_datetime():
    record = json.loads(to_json(normalize(raw_event())))
    assert record["dt"] == "2026-10-03T10:34:22.066000+00:00"
    assert record["title"] == "Example"
    assert record["synthetic"] is False


def test_null_comment_is_not_a_revert():
    assert normalize(raw_event(comment=None)).is_revert is False


def test_from_json_round_trips_and_ignores_replay_orig_dt():
    edit = normalize(raw_event())
    record = json.loads(to_json(edit))
    record["orig_dt"] = "2026-10-01T00:00:00+00:00"
    assert from_json(json.dumps(record)) == edit
