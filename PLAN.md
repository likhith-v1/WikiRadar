# PLAN.md: WikiRadar

Real-time anomaly detection on live English Wikipedia edits.
Course: 22CSE544 Data Streaming and Analysis, mini-project. Due: **2026-10-16**.

This file is the single source of truth for the build. Work gate by gate. Do not start a gate until the previous gate's exit criteria pass. After each gate: run the checks, commit, and append a line to `PROGRESS.md`.

---

## 0. Agent operating rules

1. **Gate discipline.** Each gate has entry criteria, tasks, exit checks and a commit message. Only start a gate after the previous gate's checks pass. Never mark a gate done without running its checks and pasting the output summary into `PROGRESS.md`.
2. **Scope lock.** Build only what this file lists. Anything in section 9 ("Out of scope") is forbidden until Gate 7 is done and the user approves.
3. **Stop and ask the user** when any condition in section 8 occurs. Do not work around it silently.
4. **No secrets in git.** Credentials live in `.env` (gitignored). Commit `.env.example`.
5. **Shared code, no duplication.** Feature extraction and windowing live only in `wikiradar/features.py`. Training, live processing and evaluation all import it. This is a graded design point (no train/serve skew).
6. **Small commits**, one per gate minimum, in Conventional Commits style (`feat:`, `fix:`, `docs:`).
7. **Host:** the user's Apple Silicon MacBook. Infrastructure (Kafka, Postgres, Grafana) runs in Docker. Python services run natively on the host during development.
8. **Pin versions.** Pin Docker image tags and Python deps. Before pinning, verify that the image tag exists.

---

## 1. Goal and success criteria

**One sentence:** stream live English Wikipedia edits through Kafka, aggregate them into 10-second event-time windows, score each window with an IsolationForest, store the results in Postgres, and show them on a Grafana dashboard that refreshes itself every 5 seconds.

**Course requirements (the guide's submission checklist):**

- [ ] Streaming tool: Kafka, working
- [ ] ML algorithm: IsolationForest, trained offline and scoring live
- [ ] Visualization: Grafana, refreshing itself from a live data source
- [ ] Database serving layer between the processor and the dashboard: PostgreSQL
- [ ] Pipeline runs end to end, dashboard updates on its own
- [ ] Written report and a live demo or screen recording

**Project-specific success criteria:**

- [ ] `docker compose up -d` plus three commands (`processor`, `producer`, open Grafana) brings up the whole system
- [ ] Works offline using `--replay` (the fallback if campus Wi-Fi fails)
- [ ] `inject.py` triggers a visible anomaly marker within 20 seconds
- [ ] Evaluation reports detection rate, false alarms per hour and a contamination sweep

---

## 2. Architecture

```
Wikimedia EventStreams (SSE)          data/*.jsonl (recorded)
        |  producer.py --live                 |  producer.py --replay
        +-------------------+-----------------+
                            v
                 Kafka topic: wiki-edits  (3 partitions, key = title)
                            ^
                            |  inject.py (synthetic attacks, synthetic=true)
                            v
                 processor.py
                   - parse + dedupe
                   - event-time 10 s tumbling windows (watermark = max_dt - 5 s)
                   - features.py -> feature vector
                   - IsolationForest (models/iforest.joblib) -> score, flag, reason
                            v
                 PostgreSQL: edits, window_metrics, current_state
                            v
                 Grafana (provisioned dashboard, 5 s refresh)
```

**Layer mapping for the report:**

| # | Layer | Choice |
|---|---|---|
| 1 | Data source | Wikimedia EventStreams `recentchange`, filtered to enwiki |
| 2 | Streaming tool | Apache Kafka in KRaft mode (no ZooKeeper) |
| 3 | Stream processor | Python, event-time tumbling windows with a watermark |
| 4 | ML | scikit-learn IsolationForest (unsupervised) |
| 5 | Database | PostgreSQL 16 |
| 6 | Visualization | Grafana, provisioned from files |

---

## 3. Fixed technical decisions

Do not change these without stopping to ask.

### 3.1 Stack

- Python 3.11+, managed with `uv` (fall back to `python -m venv` if `uv` is missing)
- Python deps: `confluent-kafka`, `requests`, `psycopg[binary]` (v3), `scikit-learn`, `pandas`, `numpy`, `joblib`, `python-dotenv`, `pytest`
- Docker images: `apache/kafka` (3.9.x or newer, pinned), `postgres:16`, `grafana/grafana-oss` (pinned 11.x or newer)

### 3.2 Repo layout

```
wiki-radar/
  PLAN.md  PROGRESS.md  README.md
  docker-compose.yml  .env.example  .gitignore  pyproject.toml
  wikiradar/
    __init__.py
    config.py        # env loading, constants
    sse.py           # minimal SSE client with reconnect + Last-Event-ID
    schema.py        # event normalization: raw recentchange -> Edit dataclass
    features.py      # WindowAggregator + feature vector (SHARED)
    attacks.py       # synthetic attack generators (SHARED by inject + eval)
    db.py            # psycopg helpers, upserts
  scripts/
    producer.py  processor.py  inject.py  train.py  evaluate.py  reset_db.py
  sql/schema.sql
  grafana/provisioning/datasources/postgres.yml
  grafana/provisioning/dashboards/dashboards.yml
  grafana/dashboards/wiki-radar.json
  models/            # iforest.joblib (gitignored), model_card.json (committed)
  data/              # recorded jsonl (gitignored except a small sample)
  results/           # eval CSVs + PNG plots (committed)
  report/report.md
  tests/test_schema.py  tests/test_features.py  tests/test_attacks.py
```

### 3.3 Data source specifics

- URL: `https://stream.wikimedia.org/v2/stream/recentchange`
- **A User-Agent header is required**, for example `WikiRadar-DSA-MiniProject/1.0 (likhithvenkatarama1@gmail.com)`. Put it in `.env` as `WIKI_USER_AGENT`. Without it, requests get 403 or 429.
- Backfill: `?since=<ISO8601 UTC>` replays historical events (retention is roughly 7 days). Use this to collect training data quickly.
- Reconnects: store the last SSE `id:` line verbatim and send it back as the `Last-Event-ID` header. Back off exponentially (1, 2, 4 ... up to 30 s).
- Filter: keep only `wiki == "enwiki"` and `type in {"edit", "new"}`. Drop everything else in the producer.

### 3.4 Normalized event (`schema.py`)

```python
@dataclass
class Edit:
    event_id: str        # meta.id (UUID), dedupe key
    dt: datetime         # meta.dt, UTC, event time
    title: str
    title_url: str
    user: str
    is_bot: bool         # event["bot"]
    is_anon: bool        # IPv4/IPv6 regex OR username starts with "~" (temporary accounts)
    is_new: bool         # type == "new"
    byte_delta: int      # length.new - (length.old or 0)
    is_revert: bool      # comment regex, see below
    rev_id: int | None   # revision.new
    synthetic: bool = False
```

Revert regex (case-insensitive) applied to `comment`:
`r"\b(undid revision|reverted \d+ edits?|reverted edits by|rv[ v]|rollback|revert(ed|ing)?)\b"`

Handle missing fields without crashing: count the event as dropped and increment a counter.

### 3.5 Kafka

- Single-node KRaft broker. Advertised listener `PLAINTEXT://localhost:9092` (Python runs on the host).
- Topic `wiki-edits`: 3 partitions, replication factor 1, key = `title` (UTF-8), value = JSON of the `Edit` (ISO datetime).
- Producer: `acks=all`, `linger.ms=20`, `enable.idempotence=true`.
- Consumer group `wikiradar-processor`, `auto.offset.reset=latest` for live runs and `earliest` behind a flag.

### 3.6 Windowing (`features.py`, `WindowAggregator`)

- Tumbling windows of **10 seconds** aligned to epoch, keyed by event time `dt`.
- Watermark = (max `dt` seen) minus **5 seconds**. A window closes and is emitted once `window_end <= watermark`.
- Events older than the watermark for an already-closed window are counted in `late_dropped`. They are not added.
- Empty windows inside the observed range are emitted with zero counts (a gap is information too).
- `WindowAggregator` is a pure class with no I/O: `add(edit) -> list[ClosedWindow]` and `flush() -> list[ClosedWindow]`. Training, live processing and evaluation all use it.

### 3.7 Features (per closed window)

| Feature | Definition |
|---|---|
| `edit_count` | log1p(number of events) |
| `bot_ratio` | bot events / events (0 if empty) |
| `anon_ratio` | anon events / events |
| `new_page_count` | log1p(new pages) |
| `mean_abs_delta` | log1p(mean of abs(byte_delta)) |
| `min_delta` | sign-preserving log: `-log1p(-min)` if min < 0 else `log1p(min)` |
| `revert_ratio` | revert events / events |
| `top_title_share` | max edits to a single title / events |
| `unique_users_ratio` | distinct users / events |
| `hour_sin`, `hour_cos` | UTC hour of window_start on the unit circle |

Feature order is fixed in `FEATURE_NAMES`, a constant that `train.py` saves into the model artifact. The processor asserts that it matches.

### 3.8 Model

- `IsolationForest(n_estimators=200, contamination=0.01, random_state=42)` as the default. Contamination is swept in Gate 6.
- Artifact `models/iforest.joblib` holds `{model, feature_names, train_mean, train_std, contamination, trained_on, n_windows}`.
- Live outputs per window:
  - `score = -model.decision_function(x)`, so higher means more anomalous and the threshold is 0
  - `is_anomaly = score > 0`
  - `reason` = the feature with the largest absolute z-score against the training mean and std, written as e.g. `"anon_ratio high (z=+6.2)"`
- `models/model_card.json` (committed) records the training data range, window count, contamination and feature list.

### 3.9 Database (`sql/schema.sql`)

```sql
CREATE TABLE IF NOT EXISTS edits (
  event_id    TEXT PRIMARY KEY,
  dt          TIMESTAMPTZ NOT NULL,
  title       TEXT NOT NULL,
  title_url   TEXT,
  user_name   TEXT,
  is_bot      BOOLEAN, is_anon BOOLEAN, is_new BOOLEAN, is_revert BOOLEAN,
  byte_delta  INTEGER,
  rev_id      BIGINT,
  synthetic   BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS edits_dt_idx ON edits (dt);

CREATE TABLE IF NOT EXISTS window_metrics (
  window_start      TIMESTAMPTZ PRIMARY KEY,
  window_end        TIMESTAMPTZ NOT NULL,
  n_events          INTEGER NOT NULL,
  edit_count        REAL, bot_ratio REAL, anon_ratio REAL, new_page_count REAL,
  mean_abs_delta    REAL, min_delta REAL, revert_ratio REAL,
  top_title_share   REAL, unique_users_ratio REAL,
  top_title         TEXT,
  score             REAL NOT NULL,
  is_anomaly        BOOLEAN NOT NULL,
  reason            TEXT,
  has_synthetic     BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS current_state (
  id                  INTEGER PRIMARY KEY CHECK (id = 1),
  updated_at          TIMESTAMPTZ,
  edits_last_min      INTEGER,
  bot_pct_last_5m     REAL,
  anon_pct_last_5m    REAL,
  anomalies_last_hour INTEGER,
  total_events        BIGINT,
  late_dropped        BIGINT,
  last_window_score   REAL
);
```

- Insert edits with `ON CONFLICT (event_id) DO NOTHING`. Kafka delivers at least once, so duplicates are expected and harmless.
- Upsert `window_metrics` on `window_start`.
- Every 10 minutes the processor deletes `edits` older than 24 hours. `window_metrics` is kept.

### 3.10 Grafana

- Provisioned Postgres datasource (uid `wikiradar-pg`) and dashboard (uid `wiki-radar`). Anonymous viewer access is enabled so the demo needs no login. Default refresh `5s`, default time range `now-30m`.
- Panels:
  1. Stat tiles: edits/min, bot %, anon %, anomalies in the last hour (from `current_state`)
  2. Time series: `n_events` per window, with annotations from `window_metrics WHERE is_anomaly`
  3. Time series: `score` with a threshold line at 0 (red above)
  4. Stacked time series: bot vs. human vs. anon edits per window
  5. Table: top 10 titles in the last 5 minutes, linked via `title_url`
  6. Table: recent anomalies (time, score, reason, top_title, synthetic flag)
- Every query uses `$__timeFilter(...)`.

### 3.11 Synthetic attacks (`attacks.py`)

Each generator returns `list[Edit]` with `synthetic=True`, timestamps spread across a given start time and duration, and an `intensity` parameter (1 to 5).

| Attack | Shape |
|---|---|
| `vandal_wave` | Many anonymous users, many titles, large negative deltas, few reverts |
| `edit_war` | One title, 2 to 3 users alternating, revert comments |
| `bot_flood` | One bot account, very high edit_count |
| `page_blank` | Few edits, deltas around -20,000 to -100,000 bytes |
| `spam_pages` | Burst of new pages from new or anon accounts |

`inject.py --attack vandal_wave --intensity 3` publishes to `wiki-edits` with `dt = now`.

### 3.12 Replay timing (important)

`producer.py --replay FILE --speed S` **re-stamps** event times: `new_dt = now_at_start + (orig_dt - first_orig_dt) / S`. Without this, the replayed historical timestamps and the injector's `now` timestamps would mix and the watermark would jump. The original `dt` is kept in the `orig_dt` field for reference only.

---

## 4. Gates

Dates are targets. If a gate runs more than 1 day past its date, stop and tell the user (section 8).

### Gate 0: Repo and environment (Oct 5)

**Entry:** empty directory, Docker running on the Mac.

**Tasks**

1. Create the repo layout from 3.2, `pyproject.toml`, `.gitignore` (data/*.jsonl, models/*.joblib, .env, __pycache__, .venv), `.env.example`, `README.md` stub, `PROGRESS.md`.
2. Write `docker-compose.yml` for Kafka (KRaft single-node), Postgres 16 (mounts `sql/schema.sql` into `/docker-entrypoint-initdb.d/`) and Grafana (mounts the provisioning and dashboards folders). Add healthchecks.
3. Create the `wiki-edits` topic. Add a one-shot `kafka-init` service in compose, or a `make topic` target.
4. `uv sync`, then `pytest` (an empty suite passes).

**Exit checks**

- `docker compose ps` shows all services healthy
- `kafka-topics.sh --describe --topic wiki-edits` shows 3 partitions
- `psql ... -c '\dt'` lists `edits`, `window_metrics`, `current_state`
- Grafana loads at `http://localhost:3000` without a login

**Commit:** `chore: scaffold repo and docker stack`

### Gate 1: Producer, live and recording (Oct 5 to 6)

**Entry:** Gate 0 passed.

**Tasks**

1. `sse.py`: streaming GET with `requests` (`stream=True`, timeout), parse `id:`, `event:` and `data:` lines, yield `(id, json)`, reconnect with `Last-Event-ID` and backoff.
2. `schema.py`: `Edit` plus `normalize(raw) -> Edit | None`. Unit tests for anon detection (IPv4, IPv6, `~2025-...`, normal name), revert regex, new page byte_delta, missing fields.
3. `producer.py` with modes:
   - `--live`: SSE to Kafka
   - `--record PATH [--since ISO] [--hours H]`: SSE to JSONL. Writes normalized edits. Stops after H hours of event time
   - `--replay PATH --speed S`: JSONL to Kafka, with re-stamping (3.12)
   - Prints throughput every 10 s (events/s, dropped, reconnects)
4. **Start the training backfill now:** `--record data/train_24h.jsonl --since <48h ago> --hours 24`, plus a separate held-out day, `data/holdout_24h.jsonl` (the following 24 h). Let them run in the background.

**Exit checks**

- `pytest tests/test_schema.py` passes
- `--live` for 60 s: the console shows a steady rate, and `kafka-console-consumer` prints enwiki events
- `--record` produces a non-empty JSONL. Spot-check 5 lines by hand
- `--replay` of a 5-minute sample at speed 10 publishes to Kafka with timestamps near now

**Stop if:** the stream returns 403 or 429 twice with the User-Agent set, or `--since` does not backfill (see section 8).

**Commit:** `feat: SSE producer with live, record and replay modes`

### Gate 2: Processor, windowing and storage (Oct 7 to 8)

**Entry:** Gate 1 passed, and at least a few hours of recorded data exist.

**Tasks**

1. `features.py`: `WindowAggregator` (3.6), `featurize(window) -> np.ndarray` (3.7), `FEATURE_NAMES`.
2. Tests in `tests/test_features.py`:
   - window alignment
   - watermark closing
   - late event dropped and counted
   - empty window emitted with zeros
   - feature values for a hand-built window
   - out-of-order events within the watermark land in the correct window
3. `db.py`: connection from env, `insert_edits(batch)`, `upsert_window(row)`, `update_current_state()`, `purge_old_edits()`.
4. `processor.py`: consume, normalize from JSON, batch-insert edits (every 500 events or every 1 s), feed the aggregator, and on each closed window upsert `window_metrics`. Model scoring is a stub this gate: `score=0`, `is_anomaly=false`. Update `current_state` every window. Commit Kafka offsets after the DB write, never before.
5. `reset_db.py`: truncate all tables.

**Exit checks**

- `pytest` passes in full
- Live run for 5 minutes: `SELECT count(*) FROM edits` climbs, and `window_metrics` gains about 6 rows per minute
- Kill and restart the processor: no crash, no duplicate edits, and windows continue
- Replay at speed 20: windows are 10 s apart in event time, not in wall-clock time

**Commit:** `feat: event-time windowed processor writing to postgres`

### Gate 3: Model training and live scoring (Oct 9)

**Entry:** Gate 2 passed, and `data/train_24h.jsonl` is complete (stop if not, section 8).

**Tasks**

1. `train.py`: read the JSONL, sort by `dt`, run it through `WindowAggregator`, build the feature matrix, fit the IsolationForest, save the artifact and `model_card.json`. Print the window count, the score distribution and the share flagged.
2. Processor: load the model once at startup, assert `feature_names` match, score each closed window, compute `reason` (3.8), and write `score`, `is_anomaly`, `reason`, `has_synthetic`.
3. `attacks.py` with all 5 generators. `tests/test_attacks.py` checks counts, the synthetic flag and that timestamps fall inside the window.
4. `inject.py --attack NAME --intensity N`.

**Exit checks**

- Training uses at least 6,000 windows (about 17 h or more), and around 1% are flagged on the training data
- Live: `inject.py --attack vandal_wave --intensity 3` gives a `window_metrics` row with `is_anomaly=true` and `has_synthetic=true` within 20 s
- All 5 attack types at intensity 4 are flagged at least once, live
- `pytest` passes

**Commit:** `feat: isolation forest scoring and synthetic attack injector`

### Gate 4: Grafana dashboard (Oct 10)

**Entry:** Gate 3 passed.

**Tasks**

1. Datasource and dashboard provisioning files (3.10). Build the dashboard in the UI, export the JSON to `grafana/dashboards/wiki-radar.json` with a fixed uid and datasource uid, then confirm it loads from a fresh `docker compose down -v && up`.
2. All 6 panels. Anomaly annotations appear in red. Score panel threshold at 0.

**Exit checks**

- After a fresh stack, replay and processor: the dashboard fills on its own, with no clicks
- An injected attack shows a red annotation and an entry in the anomalies table within 20 s
- Pause the producer: tiles stop changing while the page keeps refreshing. Resume: it continues
- Screenshot saved to `results/dashboard.png`

**Commit:** `feat: provisioned grafana dashboard`

### Gate 5: End-to-end hardening (Oct 11)

**Entry:** Gate 4 passed.

**Tasks**

1. `README.md`: prerequisites, a startup sequence of at most 6 commands, the demo script (section 6), and troubleshooting (User-Agent 403, Kafka not reachable, empty dashboard).
2. Run the pipeline in `--live` mode for at least 2 h unattended. Record any crash, memory growth or lag.
3. Make the processor log its lag (now minus the latest closed window end) every 30 s.
4. Optional, only if under 2 hours of work: a `Makefile` with `up`, `down`, `reset`, `live`, `replay`, `inject`, `train`.

**Exit checks**

- The 2 h run finishes with no crash, lag under 30 s, and process memory roughly flat
- A fresh clone plus the README steps works, verified by following the README literally

**Commit:** `chore: hardening, readme, makefile`

### Gate 6: Evaluation (Oct 12 to 13)

**Entry:** Gate 5 passed, and `data/holdout_24h.jsonl` is complete.

All evaluation runs **offline in batch** through the same `WindowAggregator` and features. Do not use the live stack for this: offline runs are deterministic and fast.

**Tasks** (`evaluate.py`, results to `results/`)

1. **False alarms:** score every holdout window. Report anomalies per hour, overall and by hour of day. Save `results/false_alarms.csv`.
2. **Detection rate:** merge 30 synthetic attacks (5 types x intensities 1 to 5, spread across the holdout day with a fixed seed) into the holdout events, re-window and score. An attack counts as detected if any window overlapping it is flagged. Report detection rate by type and by intensity. Save `results/detection.csv`.
3. **Contamination sweep:** retrain with contamination in {0.005, 0.01, 0.02, 0.05}. For each, compute false alarms per hour and detection rate. Plot detection rate (y) against false alarms per hour (x), saved as `results/contamination_sweep.png`, plus a CSV.
4. **Real-world sanity check (optional, about 2 h cap):** for the top 20 real (non-synthetic) holdout anomalies, take their `rev_id`s and query the MediaWiki Action API (`action=query&prop=revisions&revids=...&rvprop=tags`, same User-Agent, at most 1 request per second). Report the share tagged `mw-reverted`, against a random baseline of 20 normal windows.
5. Pick the final contamination value. Retrain and update `model_card.json`.

**Exit checks**

- All CSVs and PNGs exist and are committed
- `results/SUMMARY.md` lists the headline numbers in 5 to 8 lines

**Commit:** `feat: offline evaluation and contamination sweep`

### Gate 7: Report and recording (Oct 14)

**Entry:** Gate 6 passed.

**Tasks**

1. `report/report.md`, following the guide's structure:
   - Abstract
   - Architecture (include the diagram from section 2 as an image or Mermaid)
   - Dataset (what one event looks like, with a real redacted example)
   - One section per layer, with a short code snippet each
   - ML model
   - Dashboard (screenshot)
   - Results (from `results/SUMMARY.md` plus plots)
   - Conclusion
2. Required discussion points in the report:
   - event time vs. processing time, and the watermark
   - at-least-once delivery and idempotent inserts
   - train/serve split and the shared `features.py`
   - daily traffic cycle and the hour features
   - the contamination trade-off
   - limitations: unlabelled data, synthetic attacks are a proxy, single-node Kafka
3. Writing style: plain, direct prose. No em dashes. Explain why each layer exists.
4. Screen recording, 3 to 5 minutes, following the demo script (section 6). The user records it; the agent prepares the script and checks the stack is ready.

**Exit checks**

- The report covers every row of the guide's report table
- The recording exists (the user confirms)

**Commit:** `docs: final report`

### Gate 8: Buffer and rehearsal (Oct 15)

Two full dry runs of the demo from a cold start: once with `--live` and once with `--replay` and no network. Fix only bugs that would break the demo. No new features.

---

## 5. Daily runbook (cheat sheet)

```bash
docker compose up -d                                   # infra
uv run scripts/processor.py                            # terminal 1
uv run scripts/producer.py --live                      # terminal 2  (or --replay data/demo.jsonl --speed 5)
open http://localhost:3000/d/wiki-radar                # dashboard
uv run scripts/inject.py --attack vandal_wave --intensity 3   # trigger anomaly
uv run scripts/reset_db.py                             # clean slate before demo
```

---

## 6. Demo script (5 minutes)

1. Start with tables reset and the dashboard open and empty. Explain the six layers using the architecture slide (30 s).
2. Start the processor, then the producer (`--live`, or `--replay` if the network is down). Point out tiles counting up and the curve drawing itself (60 s).
3. Explain one panel: bot vs. human edits, the most-edited pages, and that table links are real Wikipedia pages (45 s).
4. Run `inject.py --attack vandal_wave --intensity 3`. Within one window a red annotation appears and the anomalies table shows `anon_ratio high`. Explain the train/serve split (60 s).
5. Run `inject.py --attack edit_war --intensity 4`. It is flagged with a different reason (30 s).
6. Pause the producer: the page keeps refreshing but the numbers freeze. Resume (30 s).
7. Close with the contamination sweep plot and detection numbers (45 s).

---

## 7. Definition of done

- [ ] Gates 0 to 7 are marked done in `PROGRESS.md`, with check output
- [ ] Every item in section 1 is checked
- [ ] `pytest` passes
- [ ] A fresh clone plus the README steps works
- [ ] `report/report.md`, `results/`, the dashboard screenshot and the recording all exist

---

## 8. Stop conditions (pause and ask the user)

1. EventStreams returns 403 or 429 repeatedly with a valid User-Agent, or is unreachable from the user's network. Proposed fallback: work only from recorded data the user captures on another network.
2. `?since=` backfill does not work or gives less than 12 h of data. The user must then decide: record live for longer, or train on fewer hours.
3. Any gate runs more than 1 day past its target date.
4. A fixed decision in section 3 looks wrong (for example, 10 s windows are too sparse because enwiki volume is lower than expected). Report the measured rate and propose a change; do not change it unilaterally.
5. Detection rate at intensity 3 or above is under 70% after the contamination sweep. Report this before tuning features.
6. Any need for a paid service, a cloud account, or a tool from the out-of-scope list.
7. Anything that would write to Wikipedia. This project is **read-only**: never call edit or write APIs.

---

## 9. Out of scope (scope lock)

Do not build any of these unless Gate 7 is done and the user approves:

- Spark, Flink, Storm, Faust or Kafka Streams processors
- Forecasting (ARIMA, Prophet)
- Per-edit classifiers, text or NLP on diffs, LLM features
- Multi-wiki support beyond enwiki
- Containerizing the Python services (nice to have later; native is fine for the demo)
- Authentication, TLS, multi-node Kafka, Kubernetes
- A custom web frontend (Grafana is the visualization layer)
- Alerting integrations (email, Slack, Telegram)

---

## 10. PROGRESS.md format

Append one block per gate:

```
## Gate N: <name>, DONE <YYYY-MM-DD>
- Checks: <one line per exit check with the result>
- Notes: <surprises, deviations approved by user>
- Commit: <short sha>
```
