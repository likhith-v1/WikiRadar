import pytest
import requests

from wikiradar import sse
from wikiradar.sse import SSEClient, parse


def test_parses_frames_and_skips_comments():
    lines = [
        ":ok",
        "",
        "event: message",
        'id: [{"offset":1}]',
        'data: {"a": 1}',
        "",
        "event: message",
        'id: [{"offset":2}]',
        'data: {"a": 2}',
        "",
    ]
    assert list(parse(lines)) == [('[{"offset":1}]', '{"a": 1}'), ('[{"offset":2}]', '{"a": 2}')]


def test_joins_multiline_data_and_keeps_last_id():
    lines = ["id: 7", "data: {", 'data: "a": 1}', "", "data: {}", ""]
    assert list(parse(lines)) == [("7", '{\n"a": 1}'), ("7", "{}")]


class RefusingResponse:
    def __init__(self, status_code=429):
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        raise requests.HTTPError(f"{self.status_code} error", response=self)


class StreamingResponse(RefusingResponse):
    def raise_for_status(self):
        pass

    def iter_lines(self, decode_unicode=False):
        return iter(["id: 1", "data: {}", ""])


def test_stops_after_two_refusals(monkeypatch):
    calls = []
    monkeypatch.setattr(sse.requests, "get", lambda *a, **kw: calls.append(1) or RefusingResponse())
    monkeypatch.setattr(sse.time, "sleep", lambda s: None)
    with pytest.raises(requests.HTTPError):
        next(iter(SSEClient("http://stream", "ua")))
    assert len(calls) == 2


def test_other_http_error_between_refusals_resets_the_count(monkeypatch):
    responses = iter([RefusingResponse(403), RefusingResponse(503), RefusingResponse(403), StreamingResponse(200)])
    monkeypatch.setattr(sse.requests, "get", lambda *a, **kw: next(responses))
    monkeypatch.setattr(sse.time, "sleep", lambda s: None)
    assert next(iter(SSEClient("http://stream", "ua"))) == "{}"
