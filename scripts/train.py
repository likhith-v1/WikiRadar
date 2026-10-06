"""Window a recorded JSONL, fit the IsolationForest, and save the artifact and model card."""

import argparse
import json
from pathlib import Path

import joblib
import numpy as np

from wikiradar.features import FEATURE_NAMES, WindowAggregator, featurize
from wikiradar.model import MODEL_PATH, fit
from wikiradar.schema import from_json

MODEL_CARD = MODEL_PATH.with_name("model_card.json")


def window_matrix(path: Path) -> tuple[np.ndarray, list]:
    edits = sorted((from_json(line) for line in path.open()), key=lambda e: e.dt)
    agg = WindowAggregator()
    windows = [w for e in edits for w in agg.add(e)] + agg.flush()
    return np.array([featurize(w) for w in windows]), windows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path, nargs="?", default=Path("data/train_24h.jsonl"))
    parser.add_argument("--contamination", type=float, default=0.01)
    args = parser.parse_args()

    X, windows = window_matrix(args.data)
    artifact = fit(X, args.contamination)
    artifact["trained_on"] = f"{args.data.name}: {windows[0].start.isoformat()} to {windows[-1].end.isoformat()}"
    joblib.dump(artifact, MODEL_PATH)
    MODEL_CARD.write_text(json.dumps({
        "model": "IsolationForest(n_estimators=200, random_state=42)",
        "contamination": args.contamination,
        "trained_on": artifact["trained_on"],
        "n_windows": len(X),
        "n_edits": sum(len(w.edits) for w in windows),
        "window_seconds": 10,
        "features": list(FEATURE_NAMES),
    }, indent=2) + "\n")

    scores = -artifact["model"].decision_function(X)
    print(f"{len(X)} windows from {artifact['trained_on']}")
    print("score percentiles 1/50/99/max:", np.round(np.percentile(scores, [1, 50, 99, 100]), 3))
    print(f"flagged on training data: {np.mean(scores > 0):.2%}")
    print(f"saved {MODEL_PATH} and {MODEL_CARD}")


if __name__ == "__main__":
    main()
