"""Publish enwiki edits to Kafka (--live, --replay) or record them to JSONL (--record)."""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from wikiradar import config
from wikiradar.schema import normalize, to_json
from wikiradar.sse import SSEClient

KEEP_TYPES = {"edit", "new"}


class Stats:
    def __init__(self):
        self.count = self.dropped = self.last_count = 0
        self.last_print = time.monotonic()

    def tick(self, reconnects: int = 0):
        now = time.monotonic()
        if now - self.last_print >= 10:
            rate = (self.count - self.last_count) / (now - self.last_print)
            print(f"{self.count} events ({rate:.1f}/s), dropped {self.dropped}, reconnects {reconnects}", flush=True)
            self.last_print, self.last_count = now, self.count


def enwiki_edits(client: SSEClient, stats: Stats):
    for data in client:
        stats.tick(client.reconnects)
        try:
            raw = json.loads(data)
        except ValueError:
            stats.dropped += 1
            continue
        if raw.get("wiki") != "enwiki" or raw.get("type") not in KEEP_TYPES:
            continue
        edit = normalize(raw)
        if edit is None:
            stats.dropped += 1
            continue
        yield edit


def kafka_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "acks": "all",
        "linger.ms": 20,
        "enable.idempotence": True,
    })


def live():
    producer, stats = kafka_producer(), Stats()
    client = SSEClient(config.STREAM_URL, config.USER_AGENT)
    try:
        for edit in enwiki_edits(client, stats):
            producer.produce(config.KAFKA_TOPIC, key=edit.title.encode(), value=to_json(edit).encode())
            producer.poll(0)
            stats.count += 1
    finally:
        producer.flush()


def record(path: str, since: str | None, hours: float):
    stats = Stats()
    client = SSEClient(config.STREAM_URL, config.USER_AGENT, since)
    end = datetime.fromisoformat(since) + timedelta(hours=hours) if since else None
    with open(path, "w") as out:
        for edit in enwiki_edits(client, stats):
            end = end or edit.dt + timedelta(hours=hours)
            if edit.dt >= end:
                break
            out.write(to_json(edit) + "\n")
            stats.count += 1
    print(f"recorded {stats.count} edits to {path}, dropped {stats.dropped}")


def replay(path: str, speed: float):
    """Re-stamp event times to now + (orig_dt - first_dt) / speed, pacing sends to match."""
    producer, stats = kafka_producer(), Stats()
    start = datetime.now(timezone.utc)
    first = None
    with open(path) as lines:
        for line in lines:
            edit = json.loads(line)
            orig = datetime.fromisoformat(edit["dt"])
            first = first or orig
            new_dt = start + (orig - first) / speed
            time.sleep(max(0.0, (new_dt - datetime.now(timezone.utc)).total_seconds()))
            edit["orig_dt"], edit["dt"] = edit["dt"], new_dt.isoformat()
            producer.produce(config.KAFKA_TOPIC, key=edit["title"].encode(), value=json.dumps(edit).encode())
            producer.poll(0)
            stats.count += 1
            stats.tick()
    producer.flush()
    print(f"replayed {stats.count} edits from {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true")
    mode.add_argument("--record", metavar="PATH")
    mode.add_argument("--replay", metavar="PATH")
    parser.add_argument("--since", help="ISO8601 UTC start for --record backfill")
    parser.add_argument("--hours", type=float, default=24, help="event-time hours to --record")
    parser.add_argument("--speed", type=float, default=1, help="--replay speed multiplier")
    args = parser.parse_args()
    try:
        if args.live:
            live()
        elif args.record:
            record(args.record, args.since, args.hours)
        else:
            replay(args.replay, args.speed)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
