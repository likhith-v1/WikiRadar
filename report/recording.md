# Screen recording runbook (3 to 5 minutes)

## Before you press record

1. `docker compose up -d`, then `docker compose ps`: kafka, postgres and grafana show `healthy`.
2. Stop any running processor or producer (Ctrl-C), then clear both the tables and the processor's Kafka position, so it does not replay old messages:
   ```bash
   uv run scripts/reset_db.py
   docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --group wikiradar-processor --topic wiki-edits --reset-offsets --to-latest --execute
   ```
3. Open three terminals side by side with the browser:
   - terminal 1 (processor), terminal 2 (producer), terminal 3 (inject)
   - browser: `http://localhost:3000/d/wiki-radar` (empty is expected)
4. Have the architecture diagram ready (section 1 of `report/report.md`, or the one in `README.md`).
5. If the network is unreliable, use the replay command in step 2 below instead of `--live`.

## Script

| Time | Do | Say |
|---|---|---|
| 0:00 | Show the architecture diagram | "WikiRadar watches every English Wikipedia edit live. Edits come from Wikimedia's public stream, go through Kafka, a Python processor groups them into 10-second windows by when the edit was made, an IsolationForest scores each window, and the results go to Postgres and this Grafana dashboard." |
| 0:30 | Terminal 1: `uv run scripts/processor.py`; terminal 2: `uv run scripts/producer.py --live` (offline: `--replay data/sample_demo.jsonl --speed 1`) | "The processor reads from the newest messages in Kafka. The producer prints its rate every 10 seconds, about 2 edits a second." |
| 0:50 | Watch the dashboard fill | "Each window closes 5 seconds after it ends, once every partition has moved past it. That is the watermark, and it is why the first points appear after about 20 seconds. The tiles count up and the curves draw themselves; nobody is clicking." |
| 1:30 | Point at the stacked chart and the most edited pages; click one title (keep it short) | "This splits each window into bot, human and anonymous edits; around a third of Wikipedia edits are made by bots. These are the most edited pages right now, and the links are real Wikipedia pages." |
| 2:00 | Terminal 3: `uv run scripts/inject.py --attack vandal_wave --intensity 3` | "This injects a synthetic vandalism wave: 18 anonymous users removing text from 18 pages within a few seconds. It goes into the same Kafka topic, marked as synthetic." |
| 2:20 | Wait for the red marker and the anomalies table row (up to about 20 s) | "The window is flagged. The reason column says `anon_ratio high`: the share of anonymous edits is far above normal. Training and live scoring use the same feature code, so the model sees exactly what it was trained on." |
| 3:00 | `uv run scripts/inject.py --attack edit_war --intensity 4` | "An edit war: two or three users reverting each other on one page. It is flagged for a different reason, `top_title_share high`: one page is taking a large share of all edits." |
| 3:30 | Terminal 2: Ctrl-Z | "If I pause the producer, the page keeps refreshing every 5 seconds but the numbers freeze, because no new edits arrive to close the next window." |
| 3:50 | Terminal 2: `fg` | "Resuming. It catches up the edits it missed in a quick burst, and they still land in the right windows because windows use the time each edit was made." |
| 4:05 | Open `results/contamination_sweep.png` | "Offline, on a separate day with 100 synthetic attacks mixed in, the model catches 87% of attacks at intensity 3 or more for about 17 false alarms an hour; most of those come from one morning when the normal mix of bot traffic shifted." |
| 4:35 | End | |

## Notes

- Red markers can also appear on their own. At the chosen threshold the model flagged 2% of training windows and about 5% of the holdout day's normal windows, and more when traffic shifts. If one appears, say so: "that one is real traffic, not injected; the synthetic column says false".
- If `bot_flood` or `page_blank` is used instead of `edit_war`, it may not be flagged at intensity 4 every time; `vandal_wave` and `edit_war` are the reliable ones.
- After recording: Ctrl-C both terminals.
