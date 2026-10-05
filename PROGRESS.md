# PROGRESS

One block per gate, appended after its exit checks pass. See PLAN.md section 10 for the format.

## Gate 0: Repo and environment, DONE 2026-10-05
- Checks:
  - `uv run pytest`: 1 passed (smoke import test; Python 3.12.13 venv via uv)
  - `docker compose ps -a`: kafka, postgres, grafana healthy; kafka-init exited (0)
  - `kafka-topics.sh --describe --topic wiki-edits`: PartitionCount 3, ReplicationFactor 1
  - `psql -c '\dt'`: current_state, edits, window_metrics
  - Grafana `/d/wiki-radar`: HTTP 200 anonymously; datasource health "Database Connection OK"
  - Host Python (confluent-kafka) sees wiki-edits with 3 partitions at localhost:9092
- Notes: Docker runtime is OrbStack. Pins: apache/kafka 4.3.1, postgres 16.15, grafana-oss 12.4.3 (all arm64 verified). Python deps pinned by uv.lock. Dashboard JSON is a placeholder until Gate 4.
- Commit: 5230d43

## Gate 1: Producer, live and recording, DONE 2026-10-05
- Checks:
  - `uv run pytest`: 28 passed (test_schema: anon IPv4/IPv6/`~` temp accounts, revert regex, new-page delta, missing/null fields; test_sse: frame parsing, stop after two 403/429)
  - `--live` for 60 s: steady ~1.6 to 2 edits/s, 0 dropped; Kafka offsets rose across all 3 partitions; `kafka-console-consumer` shows enwiki edits keyed by title
  - `--record`: non-empty JSONL; 5 random lines checked against the MediaWiki API (title, user and comment match; anon/bot/revert flags correct)
  - `--replay data/sample_5m.jsonl --speed 10`: 606 edits published in 30 s, `dt` re-stamped to now (10:43:15 to 10:43:45 UTC), `orig_dt` kept
  - Backfill: `data/train_24h.jsonl` 226,343 edits (2026-10-03 10:00 to 10-04 10:00 UTC) and `data/holdout_24h.jsonl` 216,653 edits (10-04 10:00 to 10-05 10:00 UTC). Both cover the full 24.00 h, 0 dropped, 0 duplicate event_ids, max gap 7 s / 11 s, about 300 slightly out-of-order events each. 5 reconnects each, all resumed via Last-Event-ID
- Notes: enwiki averages about 155 edit/new events per minute (226k per day), so a 10 s window holds about 26 events on average (section 3 sizes hold). Rulings: the enwiki filter lives in the producer and `normalize` returns None only for malformed events (PLAN 3.3 "drop everything else in the producer"); `SSEClient` yields raw data strings so JSON errors count as dropped; replay adds `orig_dt` as an extra JSON key, not an `Edit` field; added `tests/test_sse.py` (not in 3.2). Deferred minors: naive `--since` (no `Z`) is not coerced to UTC; no Kafka delivery callback; `--record` killed by SIGTERM may lose its buffered tail.
- Commit: (pending)
