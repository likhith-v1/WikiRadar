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
- Commit: f7dcf0e

## Gate 2: Processor, windowing and storage, DONE 2026-10-06
- Checks:
  - `uv run pytest`: 38 passed (test_features: epoch alignment, watermark closing, late drop counted, empty windows, hand-computed feature values, out-of-order within watermark, slowest-partition watermark, idle partition)
  - Live run: `edits` 62 -> 117 -> 189 and `window_metrics` 5 -> 11 -> 17 over 3 minutes (6 windows per minute)
  - `kill -9` processor, 30 s down, restart: 0 duplicate edits, 35 windows with 0 gaps, 0 late, and every window's `n_events` equals the edits stored for it (2,330 = 2,330), including the windows spanning the restart
  - Replay at speed 20 (20 min of holdout in 60 s): windows stay 10 s apart in event time and hold about 340 to 420 edits each, against about 13 live. The idle gap before it produced an empty window
- Notes: Watermark changed with user approval to per partition (PLAN 3.6 updated). Measured problem: with a global max, a restart's backlog catch-up dropped 43 events as late and undercounted 4 windows by up to 19 events. Offsets are committed per partition only up to the first event of a still-open window, so restarts rebuild open windows. A partition silent for 10 s of wall clock (counted only after the first message arrives, because a restart waits out the dead member's Kafka session) stops holding the watermark back. When every partition is idle, the last window stays open until traffic resumes (tiles freeze, as PLAN expects). `current_state.late_dropped` is cumulative across restarts; `total_events` = sum of windowed events. Rulings: a restart does not emit empty windows for downtime it never observed (it starts at the first new event); `db.reset` inlined into `reset_db.py`. Deferred minors: `top_title` Counter computed in the processor rather than features.py; repeated 5-minute FILTER in the current_state SQL; processor offset-commit logic has no unit test (covered by the restart exit check).
- Commit: d347cd7

## Gate 3: Model training and live scoring, DONE 2026-10-06
- Checks:
  - `uv run scripts/train.py`: 8,640 windows (24 h of `train_24h.jsonl`, 226,343 edits), 1.01% flagged on training data, score p1/p50/p99/max = -0.184 / -0.138 / 0.000 / 0.092
  - Live, `inject.py --attack vandal_wave --intensity 3`: `window_metrics` row with `is_anomaly=true`, `has_synthetic=true` 16 s after the inject command started (reason `anon_ratio high (z=+8.6)`)
  - Live, all 5 attacks at intensity 4, each flagged on the first injection: vandal_wave 15 s (`anon_ratio high`), edit_war 12 s (`top_title_share high`), bot_flood 16 s (`edit_count high`), page_blank 12 s (`mean_abs_delta high`), spam_pages 18 s (`new_page_count high`)
  - `uv run pytest`: 66 passed (adds test_attacks: counts, synthetic flag, timestamps inside the window, shapes, seeding, window fitting; test_model: reason, scoring)
- Notes: Offline calibration on 50 holdout windows (flag rate at intensity 1 to 5): edit_war 50/98/100/100/100%, vandal_wave 26/54/74/92/100%, spam_pages 18/32/46/56/80%, page_blank 12/36/42/58/58%, bot_flood 2/14/24/32/42% (measured before attacks were fitted into one window; Gate 6 re-measures). bot_flood is weakest because enwiki is already about 36% bot edits. Not tuned here: PLAN 8.5 says report detection under 70% at intensity 3+ after the Gate 6 sweep, before tuning. IsolationForest is weak on anomalies that are extreme in only one feature (random feature splits); attacks move several features at once. Real anomalies seen live include `anon_ratio high (z=+4.9)` and empty windows during a 30 s EventStreams gap (`edit_count low`). Rulings: added `wikiradar/model.py` (fit, load, score, reason shared by train, processor and evaluate, to avoid scoring skew) and `tests/test_model.py`, neither in PLAN 3.2; `reason` ignores `hour_sin`/`hour_cos` (they say when, not what); `reason` is stored for every window, not only anomalies; attack generators return field dicts and `generate()` builds the `Edit`s; `inject.py` spreads an attack over up to 5 s but shortens it to end inside the current window, so it is never split across two windows and detection stays under 20 s; page_blank uses temporary (anon) accounts. Open issue for the user: `--replay --speed S` with S > 1 packs S times the edits into each window, so most replayed windows score as anomalies (edit_count z of about +7 at speed 20). The PLAN demo uses `--speed 5`. Deferred minors: artifact and score result are plain dict/tuple; model card hard-codes the model string and window seconds; `train.py` data default is relative to the working directory.
- Commit: (pending)
