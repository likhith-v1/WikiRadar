"""Publish a synthetic attack (synthetic=true) to wiki-edits, starting now and ending in the current window."""

import argparse
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from wikiradar import config
from wikiradar.attacks import ATTACKS, fit_in_window, generate
from wikiradar.schema import to_json

DURATION = timedelta(seconds=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attack", required=True, choices=sorted(ATTACKS))
    parser.add_argument("--intensity", type=int, default=3, choices=range(1, 6))
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    edits = generate(args.attack, now, fit_in_window(now, DURATION), args.intensity)
    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    for edit in edits:
        producer.produce(config.KAFKA_TOPIC, key=edit.title.encode(), value=to_json(edit).encode())
    producer.flush()
    print(f"injected {args.attack} intensity {args.intensity}: {len(edits)} edits from {edits[0].dt:%H:%M:%S} UTC")


if __name__ == "__main__":
    main()
