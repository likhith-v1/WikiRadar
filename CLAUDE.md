# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Read first

`PLAN.md` is the single source of truth: the fixed design decisions are in section 3 and the gates are in section 4. `PROGRESS.md` records which gates are done. Before starting work, check `PROGRESS.md` to find the current gate. Course deadline: 2026-10-16.

Rules from PLAN §0 that apply to every session:
- **Gate discipline.** Don't start a gate until the previous gate's exit checks have passed. Don't mark a gate done until you have run its checks. Then append a block to `PROGRESS.md` in the format from PLAN §10, including the commit sha. Use the commit message that PLAN gives for that gate (Conventional Commits).
- **Scope lock.** Build only what PLAN lists. The items in PLAN §9 (Spark, Flink, Faust, NLP on diffs, multi-wiki, containerizing the Python services, and so on) are off-limits until Gate 7 is done and the user approves.
- **Stop and ask** when any condition in PLAN §8 occurs: EventStreams returns 403 or 429, `?since=` backfill gives less than 12 h, a gate runs more than 1 day late, a section 3 decision looks wrong, or detection at intensity 3 or above is under 70%. Never work around these silently.
- **Wikipedia is read-only.** Never call any edit or write API.
- Pin versions, and verify that an image tag exists before you pin it.

## Skills

Invoke these with the Skill tool at the points below. They are required, not optional.

**Before a gate**
- `superpowers:executing-plans`: work the current gate from PLAN.md. PLAN.md is the plan, so don't write a new one.
- `mattpocock-skills:tdd`: for any new code in `wikiradar/` (schema, features, attacks, db). Use only this TDD skill, not `superpowers:test-driven-development`, because the two conflict.

**During a gate**
- `mattpocock-skills:diagnosing-bugs`: as soon as a test fails unexpectedly or the pipeline misbehaves. Do this before you propose a fix.

**After a gate, before reporting it done**
- `superpowers:verification-before-completion`: run every exit check fresh, and paste the results into PROGRESS.md.
- `ponytail:ponytail-review`: check the gate's diff for over-engineering and leftover filler comments.
- `mattpocock-skills:code-review`: review against the previous gate's commit, checking both the coding standards and the PLAN spec.
- Leave `Commit:` as `(pending)` in PROGRESS.md. Never commit until the user explicitly says so. Then use `commit-commands:commit` with the gate's commit message from PLAN.

## Commands

```bash
uv sync                                   # install deps (Python pinned to 3.12 via .python-version)
uv run pytest                             # full suite
uv run pytest tests/test_schema.py -k anon   # single file / test
docker compose up -d                      # Kafka, Postgres, Grafana (+ one-shot kafka-init creates the topic)
docker compose down -v && docker compose up -d   # required after editing sql/schema.sql (initdb runs only on an empty volume)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic wiki-edits
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

These entry points are planned in PLAN and may not exist yet: `uv run scripts/{producer,processor,inject,train,evaluate,reset_db}.py`. The runbook is in PLAN §5.

## Architecture

The data path is: Wikimedia SSE (or a recorded JSONL file) → `producer.py` → Kafka topic `wiki-edits` (3 partitions, key = title) → `processor.py` → Postgres (`edits`, `window_metrics`, `current_state`) → Grafana. `inject.py` publishes synthetic attack edits into the same topic.

Design points that span several files:
- **No train/serve skew.** Windowing and features live only in `wikiradar/features.py`, in a pure, I/O-free `WindowAggregator` plus `featurize` and `FEATURE_NAMES`. `train.py`, `processor.py` and `evaluate.py` all import it. `train.py` saves `FEATURE_NAMES` into the model artifact, and the processor asserts that they match. This is a graded point, so never duplicate the feature logic.
- **Event time, not wall-clock time.** The processor uses 10 s tumbling windows aligned to epoch, with watermark = the slowest Kafka partition's max `dt` minus 5 s (one source for files). Late events are counted in `late_dropped`, not added. Empty windows are still emitted.
- **Replay re-stamps timestamps** (PLAN §3.12), so replayed and injected (`dt = now`) events share one timeline. The original timestamp is kept only as `orig_dt`.
- **At-least-once delivery.** Edits are inserted with `ON CONFLICT DO NOTHING`, and windows are upserted on `window_start`. Commit Kafka offsets only after the DB write, and per partition only up to the first event of a still-open window (restart rebuilds open windows).
- `wikiradar/attacks.py` is shared by `inject.py` (live) and `evaluate.py` (offline). Evaluation runs offline in batch, never on the live stack.

## Infrastructure gotchas

- Kafka has two listeners: Python on the Mac uses `localhost:9092`, and other containers use `kafka:19092`.
- Grafana expands `${POSTGRES_*}` in the provisioning YAML from env vars that compose passes in from `.env`. The datasource uid is `wikiradar-pg` and the dashboard uid is `wiki-radar`. The dashboard JSON in `grafana/dashboards/` is provisioned from files, so edit the file, not just the UI.
- The `WIKI_USER_AGENT` value in `.env` contains parentheses, so `source .env` fails in a shell. Compose and python-dotenv parse it fine.
- The Docker runtime is OrbStack on Apple Silicon. Python services run natively on the host.

## Writing style (report and docs)

Plain, direct prose with no em dashes (PLAN Gate 7).
