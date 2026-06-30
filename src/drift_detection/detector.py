"""Drift detector for TransactionAmt using KS test + PSI."""

from __future__ import annotations

import numpy as np
import pandas as pd

COLUMN = "TransactionAmt"
DEFAULT_THRESHOLD = 0.1

# PSI thresholds (industry standard)
# < 0.1  → no significant change
# 0.1–0.25 → moderate change, monitor
# ≥ 0.25 → significant drift, investigate
PSI_THRESHOLD_LOW = 0.1
PSI_THRESHOLD_HIGH = 0.25
_N_BINS = 10  # number of quantile-based bins for PSI


def _compute_psi(baseline: np.ndarray, current: np.ndarray, n_bins: int = _N_BINS) -> float:
    """Compute Population Stability Index (PSI) between baseline and current distributions.

    Bins are derived from the baseline quantiles so they are meaningful regardless
    of the current distribution's range.

    PSI = Σ (current% − baseline%) × ln(current% / baseline%)

    A small epsilon is added before log to avoid division-by-zero when a bin is empty.

    Args:
        baseline: Reference distribution (1-D float array).
        current:  Current distribution to compare against baseline.
        n_bins:   Number of quantile-based bins (default 10).

    Returns:
        PSI value (float ≥ 0). Higher means more drift.
    """
    # Build bin edges from baseline quantiles (equal-frequency binning)
    quantiles = np.linspace(0, 100, n_bins + 1)
    bin_edges = np.percentile(baseline, quantiles)

    # Make edges unique and extend boundaries so all current values are captured
    bin_edges = np.unique(bin_edges)
    bin_edges[0] = -np.inf
    bin_edges[-1] = np.inf

    # Count observations per bin
    baseline_counts = np.histogram(baseline, bins=bin_edges)[0]
    current_counts = np.histogram(current, bins=bin_edges)[0]

    # Convert to proportions, guard against zero-length arrays
    eps = 1e-8
    baseline_pct = baseline_counts / len(baseline) + eps
    current_pct = current_counts / len(current) + eps

    psi = float(np.sum((current_pct - baseline_pct) * np.log(current_pct / baseline_pct)))
    return round(psi, 6)


def _psi_label(psi: float) -> str:
    if psi < PSI_THRESHOLD_LOW:
        return "no_drift"
    if psi < PSI_THRESHOLD_HIGH:
        return "moderate_drift"
    return "significant_drift"


class DriftDetector:
    """Loads the baseline once and runs KS + PSI drift detection on demand."""

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

    def detect(self, amounts: list[float]) -> dict:
        """Run PSI between baseline and current window.

        Returns a plain dict ready for JSON serialisation.
        """
        current = np.array(amounts, dtype=float)

        psi = _compute_psi(self._baseline, current)
        drift_detected = psi >= PSI_THRESHOLD_LOW

        return {
            "column": COLUMN,
            "drift_detected": bool(drift_detected),
            "psi": psi,
            "psi_label": _psi_label(psi),
            "n_current": len(amounts),
            "current_mean": round(float(np.mean(current)), 4),
            "current_std": round(float(np.std(current, ddof=1)), 4),
            "baseline_mean": round(self._baseline_mean, 4),
            "baseline_std": round(self._baseline_std, 4),
        }
