import numpy as np
import pytest

from wikiradar.features import FEATURE_NAMES
from wikiradar.model import fit, reason, score, scores

MEAN = np.array([3.2, 0.36, 0.06, 0.4, 5.3, -5.9, 0.02, 0.05, 0.65, 0.0, 0.0])
STD = np.array([0.4, 0.16, 0.05, 0.5, 0.9, 1.8, 0.04, 0.02, 0.15, 0.7, 0.7])


def test_reason_names_the_most_extreme_feature():
    x = MEAN.copy()
    x[2] = 0.06 + 6.2 * 0.05  # anon_ratio
    x[0] = 3.2 - 2 * 0.4
    assert reason(x, MEAN, STD) == "anon_ratio high (z=+6.2)"


def test_reason_reports_low_values_and_ignores_hour_features():
    x = MEAN.copy()
    x[5] = -5.9 - 3 * 1.8  # min_delta far below normal
    x[9] = 0.0 + 9 * 0.7  # hour_sin: never a reason
    assert reason(x, MEAN, STD) == "min_delta low (z=-3.0)"


def test_fitted_model_flags_an_outlier_and_not_a_typical_window():
    rng = np.random.default_rng(0)
    X = MEAN + STD * rng.standard_normal((2000, len(FEATURE_NAMES)))
    artifact = fit(X, contamination=0.01)
    assert artifact["feature_names"] == list(FEATURE_NAMES)
    assert artifact["n_windows"] == 2000
    typical, outlier = MEAN.copy(), MEAN.copy()
    outlier[:9] = MEAN[:9] + 5 * STD[:9]
    outlier[2] = MEAN[2] + 8 * STD[2]  # anon_ratio is the most extreme
    assert score(artifact, typical)[1] is False
    s, flagged, why = score(artifact, outlier)
    assert flagged and s > 0 and why.startswith("anon_ratio high")
    assert np.mean(scores(artifact, X) > 0) == pytest.approx(0.01, abs=0.005)
