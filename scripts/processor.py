"""Consume wiki-edits, window them by event time, and write edits and window metrics to Postgres."""

import argparse
import time
from collections import Counter, defaultdict

from confluent_kafka import Consumer, TopicPartition

from wikiradar import config, db
from wikiradar.features import FEATURE_NAMES, ClosedWindow, WindowAggregator, featurize, window_start
from wikiradar.schema import from_json

BATCH_SIZE = 500
FLUSH_SECONDS = 1.0
PURGE_SECONDS = 600
IDLE_SECONDS = 10


def window_row(window: ClosedWindow) -> dict:
    features = dict(zip(FEATURE_NAMES, featurize(window).tolist()))
    titles = Counter(e.title for e in window.edits)
    return {
        **features,
        "window_start": window.start,
        "window_end": window.end,
        "n_events": len(window.edits),
        "top_title": titles.most_common(1)[0][0] if titles else None,
        "score": 0.0,  # model scoring arrives in Gate 3
        "is_anomaly": False,
        "reason": None,
        "has_synthetic": any(e.synthetic for e in window.edits),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--earliest", action="store_true", help="start a new consumer group at the earliest offset")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.KAFKA_GROUP,
        "auto.offset.reset": "earliest" if args.earliest else "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    partitions = len(consumer.list_topics(config.KAFKA_TOPIC).topics[config.KAFKA_TOPIC].partitions)
    conn = db.connect()
    resume = db.last_window_end(conn)
    late_base = db.late_dropped(conn)
    agg = WindowAggregator(sources=partitions)
    # Offsets are committed only up to the first event of a still-open window, so a restart
    # re-reads (and rebuilds) open windows; re-read edits of already-written windows are skipped.
    open_offsets: dict[int, dict] = defaultdict(dict)  # partition -> window start -> first offset
    next_offsets: dict[int, int] = {}
    last_seen, idle, first_seen = {}, set(), None  # idle clock starts once partitions are assigned and delivering
    edits, windows, dropped, skipped = [], [], 0, 0
    last_flush = last_purge = time.monotonic()
    print(f"processor: {partitions} partitions, resuming after window ending {resume}", flush=True)

    try:
        while True:
            for msg in consumer.consume(BATCH_SIZE, timeout=FLUSH_SECONDS):
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

            now = time.monotonic()
            for p in set(range(partitions)) - idle if first_seen else ():
                if now - last_seen.get(p, first_seen) > IDLE_SECONDS:
                    idle.add(p)
                    windows += agg.idle(p)
            if agg.closed_until:
                for p, offsets in open_offsets.items():
                    open_offsets[p] = {s: o for s, o in offsets.items() if s >= agg.closed_until}
            if not (next_offsets or windows) or (len(edits) < BATCH_SIZE and not windows and now - last_flush < FLUSH_SECONDS):
                continue
            db.insert_edits(conn, edits)
            for window in windows:
                db.upsert_window(conn, window_row(window))
            if windows:
                db.update_current_state(conn, windows[-1].end, late_base + agg.late_dropped, 0.0)
                print(f"windows up to {windows[-1].end:%H:%M:%S}, late {late_base + agg.late_dropped}, "
                      f"dropped {dropped}, skipped on resume {skipped}", flush=True)
            if now - last_purge >= PURGE_SECONDS:
                db.purge_old_edits(conn)
                last_purge = now
            conn.commit()
            if next_offsets:
                consumer.commit(offsets=[  # only after the DB write: at-least-once
                    TopicPartition(config.KAFKA_TOPIC, p, min(open_offsets[p].values(), default=nxt))
                    for p, nxt in next_offsets.items()
                ], asynchronous=False)
            edits, windows, next_offsets, last_flush = [], [], {}, now
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
