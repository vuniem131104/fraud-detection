"""Drift detector for the ``amount_usd`` column using the log-scale Wasserstein distance.

PSI on quantile bins is blind to right-tail *magnitude* shifts: because the top bin is
open-ended, a distribution whose tail values explode (e.g. mean 59 → 19,375) can still
score PSI ≈ 0 as long as the per-bin row proportions are roughly unchanged. The
1-Wasserstein ("earth-mover") distance on ``log1p(amount)`` measures how far probability
mass actually moves, so it reacts to scale/tail shifts while staying scale-free and
comparable across columns.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

COLUMN = "amount_usd"
DEFAULT_THRESHOLD = 0.1

# Wasserstein(log1p) thresholds — earth-mover distance in log-dollars.
# < 0.1   → no significant change
# 0.1–0.25 → moderate change, monitor
# ≥ 0.25  → significant drift, investigate
WD_THRESHOLD_LOW = 0.1
WD_THRESHOLD_HIGH = 0.25


def _compute_wasserstein(baseline: np.ndarray, current: np.ndarray) -> float:
    """Compute the 1-Wasserstein distance between ``log1p`` baseline and current amounts.

    For two 1-D empirical samples the 1-Wasserstein distance equals the integral of the
    absolute difference between their CDFs. Working in ``log1p`` space keeps the metric
    scale-free and interpretable (distance in log-dollars) and prevents a few huge outliers
    from dominating the raw-scale value. Implemented with numpy only (no scipy dependency).

    Args:
        baseline: Reference distribution (1-D float array, raw amounts).
        current:  Current distribution to compare against baseline (raw amounts).

    Returns:
        Wasserstein distance (float ≥ 0). Higher means more drift.
    """
    a = np.sort(np.log1p(baseline))
    b = np.sort(np.log1p(current))

    # Merge the support of both samples; integrate |CDF_a - CDF_b| over the gaps.
    all_values = np.concatenate([a, b])
    all_values.sort()
    deltas = np.diff(all_values)

    cdf_a = np.searchsorted(a, all_values[:-1], side="right") / len(a)
    cdf_b = np.searchsorted(b, all_values[:-1], side="right") / len(b)

    wd = float(np.sum(np.abs(cdf_a - cdf_b) * deltas))
    return round(wd, 6)


def _wd_label(wd: float) -> str:
    if wd < WD_THRESHOLD_LOW:
        return "no_drift"
    if wd < WD_THRESHOLD_HIGH:
        return "moderate_drift"
    return "significant_drift"


class DriftDetector:
    """Loads the baseline once and runs Wasserstein drift detection on the ``amount_usd`` column on demand."""

    def __init__(self, baseline_df: pd.DataFrame, threshold: float = DEFAULT_THRESHOLD) -> None:
        self._baseline = baseline_df[COLUMN].dropna().to_numpy(dtype=float)
        self._threshold = threshold
        self._baseline_mean = float(np.mean(self._baseline))
        self._baseline_std = float(np.std(self._baseline, ddof=1))

    @property
    def baseline_mean(self) -> float:
        return self._baseline_mean

    @property
    def baseline_std(self) -> float:
        return self._baseline_std

    @property
    def baseline_rows(self) -> int:
        return len(self._baseline)

    def detect(self, amounts: list[float], threshold: float | None = None) -> dict:
        """Run the log-scale Wasserstein test between the baseline and the current ``amount_usd`` window.

        Args:
            amounts:   Current-window ``amount_usd`` values to compare against the baseline.
            threshold: Wasserstein cut-off above which drift is flagged. Falls back to the
                       detector's configured threshold when ``None``.

        Returns:
            A plain dict ready for JSON serialisation.
        """
        current = np.array(amounts, dtype=float)
        cutoff = self._threshold if threshold is None else threshold

        wasserstein = _compute_wasserstein(self._baseline, current)
        drift_detected = wasserstein >= cutoff

        return {
            "column": COLUMN,
            "drift_detected": bool(drift_detected),
            "wasserstein": wasserstein,
            "wasserstein_label": _wd_label(wasserstein),
            "threshold": cutoff,
            "n_current": len(amounts),
            "current_mean": round(float(np.mean(current)), 4),
            "current_std": round(float(np.std(current, ddof=1)), 4),
            "baseline_mean": round(self._baseline_mean, 4),
            "baseline_std": round(self._baseline_std, 4),
        }
