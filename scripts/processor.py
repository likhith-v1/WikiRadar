"""Consume wiki-edits, window them by event time, and write edits and window metrics to Postgres."""

import argparse
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException, TopicPartition

from wikiradar import config, db, model
from wikiradar.features import FEATURE_NAMES, ClosedWindow, WindowAggregator, featurize, window_start
from wikiradar.schema import from_json

BATCH_SIZE = 500
FLUSH_SECONDS = 1.0
PURGE_SECONDS = 600
IDLE_SECONDS = 3
LAG_LOG_SECONDS = 30


def window_row(window: ClosedWindow, artifact: dict) -> dict:
    x = featurize(window)
    score, is_anomaly, reason = model.score(artifact, x)
    features = dict(zip(FEATURE_NAMES, x.tolist()))
    titles = Counter(e.title for e in window.edits)
    return {
        **features,
        "window_start": window.start,
        "window_end": window.end,
        "n_events": len(window.edits),
        "top_title": titles.most_common(1)[0][0] if titles else None,
        "score": score,
        "is_anomaly": is_anomaly,
        "reason": reason,
        "has_synthetic": any(e.synthetic for e in window.edits),
    }


def run(consumer: Consumer, conn, artifact: dict, partitions: int, assigned: list, msgs: list) -> list:
    """Process until Kafka (re)assigns partitions, then return that batch so the caller replays it on fresh state.

    After a (re)assignment Kafka re-sends from the last commit, which is exactly what a restart sees.
    """
    resume = db.last_window_end(conn)
    late_base = db.late_dropped(conn)
    agg = WindowAggregator(sources=partitions)
    # Offsets are committed only up to the first event of a still-open window, so a restart
    # re-reads (and rebuilds) open windows; re-read edits of already-written windows are skipped.
    open_offsets: dict[int, dict] = defaultdict(dict)  # partition -> window start -> first offset
    next_offsets: dict[int, int] = {}
    last_seen, idle, first_seen = {}, set(), None
    edits, windows, dropped, skipped = [], [], 0, 0
    last_flush = last_purge = last_lag = time.monotonic()
    print(f"processor: {partitions} partitions, resuming after window ending {resume}", flush=True)

    while True:
        msgs = msgs or consumer.consume(BATCH_SIZE, timeout=FLUSH_SECONDS)
        if assigned:
            assigned.clear()
            return msgs
        if msgs and last_seen and time.monotonic() - max(last_seen.values()) > IDLE_SECONDS:
            # The whole stream was silent: restart every idle clock, or the first partition back idles the rest.
            first_seen = time.monotonic()
            last_seen = dict.fromkeys(last_seen, first_seen)
        for msg in msgs:
            if msg.error():
                print(f"kafka: {msg.error()}", flush=True)
                continue
            p = msg.partition()
            next_offsets[p] = msg.offset() + 1
            last_seen[p] = time.monotonic()
            first_seen = first_seen or last_seen[p]
            idle.discard(p)
            try:
                edit = from_json(msg.value())
            except (ValueError, KeyError, TypeError):
                dropped += 1
                continue
            edits.append(edit)
            start = window_start(edit.dt)
            if resume and start < resume:
                skipped += 1
                continue
            open_offsets[p].setdefault(start, msg.offset())
            windows += agg.add(edit, source=p)
        msgs = []

        now = time.monotonic()
        if now - last_lag >= LAG_LOG_SECONDS and agg.closed_until:
            lag = (datetime.now(timezone.utc) - agg.closed_until).total_seconds()
            print(f"lag {lag:.1f}s (now minus latest closed window end), open windows {len(agg.buckets)}", flush=True)
            last_lag = now
        # A partition is idle when the others kept delivering without it; total silence idles none.
        newest = max(last_seen.values(), default=None)
        for p in set(range(partitions)) - idle if newest else ():
            if newest - last_seen.get(p, first_seen) > IDLE_SECONDS:
                idle.add(p)
                windows += agg.idle(p)
        if agg.closed_until:
            for p, offsets in open_offsets.items():
                open_offsets[p] = {s: o for s, o in offsets.items() if s >= agg.closed_until}
        if not (next_offsets or windows) or (len(edits) < BATCH_SIZE and not windows and now - last_flush < FLUSH_SECONDS):
            continue
        db.insert_edits(conn, edits)
        rows = [window_row(w, artifact) for w in windows]
        for row in rows:
            db.upsert_window(conn, row)
            if row["is_anomaly"]:
                print(f"ANOMALY {row['window_start']:%H:%M:%S} score {row['score']:+.3f} {row['reason']}"
                      f"{' [synthetic]' if row['has_synthetic'] else ''}", flush=True)
        if rows:
            db.update_current_state(conn, rows[-1]["window_end"], late_base + agg.late_dropped, rows[-1]["score"])
            print(f"windows up to {rows[-1]['window_end']:%H:%M:%S}, late {late_base + agg.late_dropped}, "
                  f"dropped {dropped}, skipped on resume {skipped}", flush=True)
        if now - last_purge >= PURGE_SECONDS:
            db.purge_old_edits(conn)
            last_purge = now
        conn.commit()
        if next_offsets:
            try:
                consumer.commit(offsets=[  # only after the DB write: at-least-once
                    TopicPartition(config.KAFKA_TOPIC, p, min(open_offsets[p].values(), default=nxt))
                    for p, nxt in next_offsets.items()
                ], asynchronous=False)
            except KafkaException as exc:  # the next commit covers these offsets; an eviction triggers on_assign
                print(f"kafka commit failed: {exc}", flush=True)
        edits, windows, next_offsets, last_flush = [], [], {}, now


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--earliest", action="store_true", help="start a new consumer group at the earliest offset")
    args = parser.parse_args()

    artifact = model.load()
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.KAFKA_GROUP,
        "auto.offset.reset": "earliest" if args.earliest else "latest",
        "enable.auto.commit": False,
    })
    assigned = []  # set by Kafka on every (re)assignment, consumed by run()
    consumer.subscribe([config.KAFKA_TOPIC], on_assign=lambda c, parts: assigned.append(True))
    partitions = len(consumer.list_topics(config.KAFKA_TOPIC, timeout=10).topics[config.KAFKA_TOPIC].partitions)
    conn = db.connect()
    msgs = []
    try:
        while True:
            msgs = run(consumer, conn, artifact, partitions, assigned, msgs)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
