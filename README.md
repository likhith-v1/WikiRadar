# WikiRadar

Real-time anomaly detection on live English Wikipedia edits.

Edits from the Wikimedia EventStreams `recentchange` feed go through Kafka, are aggregated into 10-second event-time windows, and each window is scored by an IsolationForest. Results are stored in Postgres and shown on a self-refreshing Grafana dashboard.

Course mini-project for 22CSE544 Data Streaming and Analysis.

> Status: scaffolding. See [PLAN.md](PLAN.md) for the build plan and [PROGRESS.md](PROGRESS.md) for gate status.

## Architecture

```
Wikimedia EventStreams (SSE)          data/*.jsonl (recorded)
        |  producer.py --live                 |  producer.py --replay
        +-------------------+-----------------+
                            v
                 Kafka topic: wiki-edits
                            ^
                            |  inject.py (synthetic attacks)
                            v
                 processor.py  (windows -> features -> IsolationForest)
                            v
                       PostgreSQL
                            v
                 Grafana (5 s refresh)
```

| Layer | Choice |
|---|---|
| Data source | Wikimedia EventStreams, enwiki only |
| Streaming | Apache Kafka (KRaft, single node) |
| Processor | Python, event-time tumbling windows with a watermark |
| ML | scikit-learn IsolationForest |
| Database | PostgreSQL 16 |
| Visualization | Grafana |

## Prerequisites

- Docker
- Python 3.11+ and [uv](https://docs.astral.sh/uv/)

## Setup

```bash
cp .env.example .env     # then set WIKI_USER_AGENT and POSTGRES_PASSWORD
uv sync
```

## Run

Once the stack is built (see the plan), the startup sequence will be:

```bash
docker compose up -d
uv run scripts/processor.py
uv run scripts/producer.py --live      # or: --replay data/demo.jsonl --speed 5
open http://localhost:3000/d/wiki-radar
```

Trigger a synthetic anomaly:

```bash
uv run scripts/inject.py --attack vandal_wave --intensity 3
```

## Repo layout

| Path | Purpose |
|---|---|
| `wikiradar/` | Shared library: config, SSE client, schema, features, attacks, db |
| `scripts/` | Entry points: producer, processor, inject, train, evaluate, reset_db |
| `sql/` | Postgres schema |
| `grafana/` | Provisioned datasource and dashboard |
| `models/` | Trained model (gitignored) and model card |
| `data/` | Recorded edits (gitignored) |
| `results/` | Evaluation outputs |
| `report/` | Final report |
| `tests/` | pytest suite |

## Tests

```bash
uv run pytest
```

## License

MIT, see [LICENSE](LICENSE).
