import sys
import time
from collections.abc import Iterable, Iterator

import requests


def parse(lines: Iterable[str]) -> Iterator[tuple[str | None, str]]:
    """Yield (last event id, data) per SSE frame."""
    event_id, data = None, []
    for line in lines:
        if not line:
            if data:
                yield event_id, "\n".join(data)
            data = []
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "data":
            data.append(value)
        elif field == "id":
            event_id = value


class SSEClient:
    """Iterate data payloads forever, resuming from Last-Event-ID; raises after two 403/429s in a row."""

    def __init__(self, url: str, user_agent: str, since: str | None = None):
        self.url, self.user_agent, self.since = url, user_agent, since
        self.last_id: str | None = None
        self.reconnects = 0

    def __iter__(self) -> Iterator[str]:
        delay, refusals = 1, 0
        while True:
            headers = {"User-Agent": self.user_agent, "Accept": "text/event-stream"}
            params = None
            if self.last_id:
                headers["Last-Event-ID"] = self.last_id
            elif self.since:
                params = {"since": self.since}
            try:
                with requests.get(self.url, headers=headers, params=params, stream=True, timeout=(10, 60)) as resp:
                    resp.raise_for_status()
                    resp.encoding = "utf-8"
                    delay, refusals = 1, 0
                    for event_id, data in parse(resp.iter_lines(decode_unicode=True)):
                        self.last_id = event_id
                        yield data
            except requests.HTTPError as exc:
                # 403/429 mean the User-Agent was rejected or we are throttled: stop, don't hammer.
                refusals += exc.response.status_code in (403, 429)
                if refusals >= 2:
                    raise
                print(f"sse: {exc}; reconnecting in {delay}s", file=sys.stderr)
            except requests.RequestException as exc:
                print(f"sse: {exc}; reconnecting in {delay}s", file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 30)
            self.reconnects += 1
