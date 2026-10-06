"""Offline evaluation on the holdout day: false alarms, attack detection, contamination sweep."""

import argparse
import csv
import itertools
import json
import random
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple

import matplotlib
import numpy as np
import requests

from wikiradar import config
from wikiradar.attacks import ATTACKS, fit_in_window, generate
from wikiradar.features import WINDOW, featurize, windows_from_edits
from wikiradar.model import fit, scores
from wikiradar.schema import from_json

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402  (backend must be set first)

RESULTS = Path("results")
CONTAMINATIONS = (0.005, 0.01, 0.02, 0.05)
SEED = 2026
REPEATS = 4  # each (attack, intensity) pair is injected this many times
ATTACK_DURATION = timedelta(seconds=5)
WINDOWS_PER_HOUR = timedelta(hours=1) / WINDOW


class Hit(NamedTuple):
    flagged: bool
    score: float
    flagged_without_attack: bool


def read(path: Path) -> list:
    return [from_json(line) for line in path.open()]


def matrix(windows: list) -> np.ndarray:
    return np.array([featurize(w) for w in windows])


def schedule_attacks(windows: list) -> list[dict]:
    """REPEATS x 5 types x 5 intensities, one per evenly spaced holdout window, in seeded random order."""
    rng = random.Random(SEED)
    combos = [(name, i) for name, i in itertools.product(sorted(ATTACKS), range(1, 6))] * REPEATS
    rng.shuffle(combos)
    stride = len(windows) // len(combos)
    attacks = []
    for k, (name, intensity) in enumerate(combos):
        window = windows[k * stride + rng.randrange(stride // 2)]
        start = window.start + timedelta(seconds=rng.uniform(0, 9.9))
        attacks.append({"attack": name, "intensity": intensity, "window_start": window.start,
                        "edits": generate(name, start, fit_in_window(start, ATTACK_DURATION), intensity, seed=k)})
    return attacks


def evaluate(artifact: dict, clean_windows: list, clean_X: np.ndarray, attacked_windows: list,
             attacked_X: np.ndarray, attacks: list[dict]) -> tuple[np.ndarray, dict]:
    """Clean holdout flags, and a Hit per attack window start."""
    clean_flags = scores(artifact, clean_X) > 0
    clean_by_start = {w.start: bool(f) for w, f in zip(clean_windows, clean_flags)}
    s = scores(artifact, attacked_X)
    by_start = {w.start: (bool(v > 0), float(v)) for w, v in zip(attacked_windows, s)}
    return clean_flags, {a["window_start"]: Hit(*by_start[a["window_start"]], clean_by_start[a["window_start"]])
                         for a in attacks}


def detection_rate(attacks: list[dict], hits: dict, min_intensity: int = 1, beyond_baseline: bool = False) -> float:
    """Share of attacks whose window is flagged; beyond_baseline also requires it was not flagged without the attack."""
    chosen = [hits[a["window_start"]] for a in attacks if a["intensity"] >= min_intensity]
    return sum(h.flagged and not (beyond_baseline and h.flagged_without_attack) for h in chosen) / len(chosen)


def write_csv(path: Path, rows: list[dict]):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def plot_sweep(rows: list[dict], path: Path):
    """Detection rate against false alarms per hour, one point per contamination value."""
    ink, muted, grid = "#0b0b0b", "#52514e", "#e4e3df"
    fig, ax = plt.subplots(figsize=(7, 4.5), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    x = [r["false_alarms_per_hour"] for r in rows]
    for key, label, color in (("detection_rate_intensity_3plus", "intensity 3 to 5", "#2a78d6"),
                              ("detection_rate_all", "all intensities", "#eb6834")):
        y = [100 * r[key] for r in rows]
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=7,
                markeredgecolor="#fcfcfb", markeredgewidth=2, label=label, zorder=3)
        ax.annotate(label, (x[-1], y[-1]), xytext=(8, 0), textcoords="offset points", va="center", color=muted, fontsize=9)
    for r, xi in zip(rows, x):
        top = 100 * r["detection_rate_intensity_3plus"]
        ax.annotate(f"c={r['contamination']}", (xi, top), xytext=(0, 9), textcoords="offset points",
                    ha="center", color=muted, fontsize=8)
    ax.axhline(70, color=muted, linewidth=1, linestyle=(0, (4, 3)), zorder=1)
    ax.annotate("70% target (intensity 3+)", (max(x) * 1.23, 70), xytext=(0, -11), textcoords="offset points",
                ha="right", color=muted, fontsize=8)
    ax.set_xlabel(f"False alarms per hour on the clean holdout day ({WINDOWS_PER_HOUR:.0f} windows per hour)", color=ink)
    ax.set_ylabel("Attacks detected (%)", color=ink)
    ax.set_title("IsolationForest contamination sweep", color=ink, loc="left", fontsize=12)
    ax.set_ylim(0, 105)
    ax.set_xlim(0, max(x) * 1.25)
    ax.grid(color=grid, linewidth=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(grid)
    ax.tick_params(colors=muted)
    ax.legend(frameon=False, loc="lower right", labelcolor=ink)
    fig.tight_layout()
    fig.savefig(path, facecolor=fig.get_facecolor())


def revert_check(windows: list, window_scores: np.ndarray) -> dict:
    """Share of edits later tagged mw-reverted: top 20 real anomalous windows vs 20 random normal windows."""
    order = np.argsort(-window_scores)
    anomalous = [windows[i] for i in order[:20] if window_scores[i] > 0]
    normal = random.Random(SEED).sample([w for w, s in zip(windows, window_scores) if s <= 0], 20)
    session = requests.Session()
    session.headers["User-Agent"] = config.USER_AGENT

    def reverted_share(group: list) -> tuple[int, int]:
        rev_ids = [e.rev_id for w in group for e in w.edits if e.rev_id]
        tagged = seen = 0
        for k in range(0, len(rev_ids), 50):
            resp = session.get("https://en.wikipedia.org/w/api.php", params={
                "action": "query", "prop": "revisions", "revids": "|".join(map(str, rev_ids[k:k + 50])),
                "rvprop": "ids|tags", "format": "json", "formatversion": 2}, timeout=30)
            resp.raise_for_status()
            for page in resp.json()["query"].get("pages", []):
                for rev in page.get("revisions", []):
                    seen += 1
                    tagged += "mw-reverted" in rev.get("tags", [])
            time.sleep(1)  # at most one request per second
        return tagged, seen

    a_tagged, a_seen = reverted_share(anomalous)
    n_tagged, n_seen = reverted_share(normal)
    return {"anomalous_windows": len(anomalous), "anomalous_reverted": a_tagged, "anomalous_revisions": a_seen,
            "normal_windows": len(normal), "normal_reverted": n_tagged, "normal_revisions": n_seen}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--train", type=Path, default=Path("data/train_24h.jsonl"))
    parser.add_argument("--holdout", type=Path, default=Path("data/holdout_24h.jsonl"))
    parser.add_argument("--contamination", type=float, default=0.02, help="value for the per-hour and per-attack tables")
    parser.add_argument("--revert-check", action="store_true", help="also query the MediaWiki API (about 1 min)")
    args = parser.parse_args()
    RESULTS.mkdir(exist_ok=True)

    train_X = matrix(windows_from_edits(read(args.train)))
    holdout_edits = read(args.holdout)
    clean_windows = windows_from_edits(holdout_edits)
    clean_X = matrix(clean_windows)
    attacks = schedule_attacks(clean_windows)
    attacked_windows = windows_from_edits(holdout_edits + [e for a in attacks for e in a["edits"]])
    attacked_X = matrix(attacked_windows)
    hours = len(clean_windows) / WINDOWS_PER_HOUR
    print(f"train {len(train_X)} windows, holdout {len(clean_windows)} windows ({hours:.1f} h), {len(attacks)} attacks")

    sweep, chosen = [], None
    for c in CONTAMINATIONS:
        artifact = fit(train_X, c)
        clean_flags, hits = evaluate(artifact, clean_windows, clean_X, attacked_windows, attacked_X, attacks)
        sweep.append({
            "contamination": c,
            "false_alarms_per_hour": round(clean_flags.sum() / hours, 2),
            "false_alarm_rate": round(clean_flags.mean(), 4),
            "detection_rate_all": round(detection_rate(attacks, hits), 3),
            "detection_rate_intensity_3plus": round(detection_rate(attacks, hits, 3), 3),
            "detection_rate_intensity_3plus_beyond_baseline": round(detection_rate(attacks, hits, 3, True), 3),
            "attack_windows_flagged_without_attack": sum(h.flagged_without_attack for h in hits.values()),
        })
        print(sweep[-1])
        if c == args.contamination:
            chosen = (artifact, clean_flags, hits)
    write_csv(RESULTS / "contamination_sweep.csv", sweep)
    plot_sweep(sweep, RESULTS / "contamination_sweep.png")

    artifact, clean_flags, hits = chosen
    by_hour = Counter(w.start.hour for w in clean_windows)
    flagged_by_hour = Counter(w.start.hour for w, f in zip(clean_windows, clean_flags) if f)
    edits_by_hour = Counter(e.dt.hour for e in holdout_edits)
    bots_by_hour = Counter(e.dt.hour for e in holdout_edits if e.is_bot)
    write_csv(RESULTS / "false_alarms.csv", [
        {"hour_utc": h, "edits": edits_by_hour[h], "bot_share": round(bots_by_hour[h] / edits_by_hour[h], 3),
         "windows": by_hour[h], "flagged": flagged_by_hour[h],
         "false_alarms_per_hour": round(flagged_by_hour[h] / (by_hour[h] / WINDOWS_PER_HOUR), 2)}
        for h in sorted(by_hour)])
    write_csv(RESULTS / "detection.csv", [
        {"attack": a["attack"], "intensity": a["intensity"], "window_start": a["window_start"].isoformat(),
         "edits": len(a["edits"]), "detected": hits[a["window_start"]].flagged,
         "score": round(hits[a["window_start"]].score, 4),
         "flagged_without_attack": hits[a["window_start"]].flagged_without_attack}
        for a in attacks])
    print(f"\ncontamination {args.contamination}: {clean_flags.sum()} of {len(clean_flags)} clean windows flagged "
          f"({clean_flags.sum() / hours:.2f} per hour)")
    for key, label in (("attack", "type"), ("intensity", "intensity")):
        groups = sorted({a[key] for a in attacks})
        print(f"detection by {label}: " + ", ".join(
            f"{g} {detection_rate([a for a in attacks if a[key] == g], hits):.0%}"
            f" ({detection_rate([a for a in attacks if a[key] == g], hits, beyond_baseline=True):.0%})"
            for g in groups) + "   (beyond baseline in brackets)")

    if args.revert_check:
        result = revert_check(clean_windows, scores(artifact, clean_X))
        (RESULTS / "revert_check.json").write_text(json.dumps(result, indent=2) + "\n")
        print("revert check:", result)


if __name__ == "__main__":
    main()
