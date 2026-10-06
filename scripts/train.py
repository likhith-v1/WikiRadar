"""Window a recorded JSONL, fit the IsolationForest, and save the artifact and model card."""

import argparse
import json
from pathlib import Path

import joblib
import numpy as np

from wikiradar.features import FEATURE_NAMES, featurize, windows_from_edits
from wikiradar.model import MODEL_PATH, fit, scores
from wikiradar.schema import from_json

MODEL_CARD = MODEL_PATH.with_name("model_card.json")


def window_matrix(path: Path) -> tuple[np.ndarray, list]:
    windows = windows_from_edits([from_json(line) for line in path.open()])
    return np.array([featurize(w) for w in windows]), windows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", type=Path, nargs="?", default=Path("data/train_24h.jsonl"))
    parser.add_argument("--contamination", type=float, default=0.02)
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

    s = scores(artifact, X)
    print(f"{len(X)} windows from {artifact['trained_on']}")
    print("score percentiles 1/50/99/max:", np.round(np.percentile(s, [1, 50, 99, 100]), 3))
    print(f"flagged on training data: {np.mean(s > 0):.2%}")
    print(f"saved {MODEL_PATH} and {MODEL_CARD}")


if __name__ == "__main__":
    main()
