# Screenshot guide for WikiRadar_Report.docx

The report has 13 figures. Figures 1, 2, 3, 6, 11, 12 and 13 are already in the document. The other 6 (figures 4, 5, 7, 8, 9 and 10) are dashed placeholder boxes. Take the screenshots in the order below; the order keeps the live run going only once.

## Before you start

- **Terminal look:** use a light profile so the screenshots print cleanly. In Terminal, go to Settings > Profiles, choose "Basic", and set the font size to 14. Make the window about 110 columns wide.
- **Grafana in light theme, without menus:** open
  `http://localhost:3000/d/wiki-radar?theme=light&kiosk&refresh=5s`
- **Capturing one window on macOS:** press Cmd+Shift+4, then Space, then hold Option and click the window. Holding Option removes the shadow. The file goes to the Desktop.
- **Pasting into Word:** click inside a dashed box, delete the two placeholder lines, then use Insert > Pictures > Picture from File. Right-click the picture, choose Size and Position, and set the width to 6.2 in. To hide the dashed border, click in the box, then go to Table Design > Borders > No Border.
- **Fill in the title page:** USN, semester, section and faculty name.

All commands run from the project folder:

```bash
cd ~/Documents/projects/WikiRadar
```

---

## Part A: Static screenshots (no live run needed)

### Figure 2: Docker services healthy

```bash
docker compose up -d
docker compose ps -a
```

Wait until kafka, postgres and grafana show `healthy` and kafka-init shows `Exited (0)`. Rerun `docker compose ps -a` if needed. Capture the terminal.

### Figure 3: Kafka topic

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --describe --topic wiki-edits
```

The output should show `PartitionCount: 3` and three partition lines. Capture it.

### Figure 13: Tests passing

```bash
uv run pytest
```

Capture the final line showing all tests passed.

### Figure 6: Training output

`train.py` rewrites the model. It uses a fixed random seed, so the result is identical, but back it up first anyway:

```bash
cp models/iforest.joblib /tmp/iforest.backup.joblib
cp models/model_card.json /tmp/model_card.backup.json
uv run scripts/train.py data/train_24h.jsonl
```

The output should show 8640 windows, score percentiles and `flagged on training data: 2.00%`. Capture it.

### Figure 11: Offline evaluation

This takes about 20 seconds. It rewrites the files in `results/` with the same numbers.

```bash
uv run scripts/evaluate.py
```

Capture the sweep rows and the "detection by" lines. If the output is taller than the window, take two captures, or make the font smaller just for this one.

---

## Part B: Live run (about 20 minutes)

### Reset to a clean slate

Stop any processor or producer that is still running (Ctrl-C), then:

```bash
uv run scripts/reset_db.py
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --group wikiradar-processor --topic wiki-edits --reset-offsets --to-latest --execute
```

### Start the pipeline

Terminal 1, start this first:

```bash
uv run scripts/processor.py
```

Terminal 2:

```bash
uv run scripts/producer.py --live
```

If the network is down, use `uv run scripts/producer.py --replay data/sample_demo.jsonl --speed 1` instead.

Open the Grafana link from "Before you start" in the browser.

### Figure 4: Producer output

After about 40 seconds, terminal 2 shows several lines like `... events (2.4/s), dropped 0, reconnects 0`. Capture terminal 2.

### Figure 8: Full dashboard

Let it run for **15 minutes** so the charts fill. Then capture the whole browser window.

If a red marker appears on its own, keep it. It is real traffic (the `synthetic` column says `false`), and you can mention it.

### Figure 9: Vandal wave detected

Terminal 3:

```bash
uv run scripts/inject.py --attack vandal_wave --intensity 3
```

Wait about 20 seconds, until a red marker appears and **Recent anomalies** has a new top row reading `anon_ratio high ...` with `synthetic = true`. Capture the whole dashboard.

### Figure 10: Edit war detected

Wait about 30 seconds after Figure 9, then:

```bash
uv run scripts/inject.py --attack edit_war --intensity 4
```

Wait about 20 seconds for a row reading `top_title_share high`. To capture only that panel, hover over **Recent anomalies** and press `v` to enlarge it. Capture it, then press Esc.

### Figure 5: Processor output

Capture terminal 1. It should now show `windows up to ...` lines, lag lines and the `ANOMALY ...` lines from the two injections. If the ANOMALY lines have scrolled off, scroll up a little so they are visible.

### Figure 7: Database contents

```bash
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT (SELECT count(*) FROM edits) AS edits, (SELECT count(*) FROM window_metrics) AS windows, (SELECT count(*) FROM window_metrics WHERE is_anomaly) AS anomalies;" -c "SELECT window_start, n_events, round(score::numeric, 3) AS score, is_anomaly, reason FROM window_metrics ORDER BY window_start DESC LIMIT 10;"'
```

Capture both result tables.

### Finish

Press Ctrl-C in terminals 1 and 2. Run `docker compose down` only if you want to stop the containers.

---

## Checklist

| Fig | What | Done |
|---|---|---|
| 2 | `docker compose ps -a` healthy | [x] |
| 3 | Kafka topic describe | [x] |
| 4 | Producer live output | [ ] |
| 5 | Processor output with ANOMALY lines | [ ] |
| 6 | `train.py` output | [x] |
| 7 | psql counts and latest windows | [ ] |
| 8 | Full dashboard after 15 min | [ ] |
| 9 | Dashboard after vandal_wave | [ ] |
| 10 | Recent anomalies after edit_war | [ ] |
| 11 | `evaluate.py` output | [x] |
| 13 | `pytest` passing | [x] |
| - | Title page: USN, semester, section, faculty | [ ] |
