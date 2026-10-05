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
- Commit: (pending user review)
