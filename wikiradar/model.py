"""IsolationForest fit, load and per-window scoring, shared by train, processor and evaluate."""

from pathlib import Path

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from wikiradar.features import FEATURE_NAMES

MODEL_PATH = Path(__file__).resolve().parent.parent / "models" / "iforest.joblib"
# Hour features describe when, not what; they never explain an anomaly.
REASON_FEATURES = [i for i, f in enumerate(FEATURE_NAMES) if not f.startswith("hour_")]


def fit(X: np.ndarray, contamination: float = 0.01) -> dict:
    model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42).fit(X)
    return {
        "model": model,
        "feature_names": list(FEATURE_NAMES),
        "train_mean": X.mean(axis=0),
        "train_std": X.std(axis=0),
        "contamination": contamination,
        "n_windows": len(X),
    }


def load(path: Path = MODEL_PATH) -> dict:
    artifact = joblib.load(path)  # pickle: only ever our own train.py output, never downloaded
    if artifact["feature_names"] != list(FEATURE_NAMES):
        raise ValueError(f"{path} was trained on {artifact['feature_names']}, expected {list(FEATURE_NAMES)}")
    return artifact


def reason(x: np.ndarray, mean: np.ndarray, std: np.ndarray) -> str:
    z = (x - mean) / np.where(std > 0, std, 1)
    i = max(REASON_FEATURES, key=lambda k: abs(z[k]))
    return f"{FEATURE_NAMES[i]} {'high' if z[i] > 0 else 'low'} (z={z[i]:+.1f})"


def score(artifact: dict, x: np.ndarray) -> tuple[float, bool, str]:
    """Higher is more anomalous; above 0 is flagged."""
    s = float(-artifact["model"].decision_function(x.reshape(1, -1))[0])
    return s, s > 0, reason(x, artifact["train_mean"], artifact["train_std"])
