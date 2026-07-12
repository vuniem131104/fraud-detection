"""Pydantic schemas for the drift detection API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class DriftResult(BaseModel):
    """Drift detection result for the ``amount_usd`` column."""

    column: str = Field(description="Column name that was tested.")
    drift_detected: bool = Field(description="True if PSI >= the configured threshold.")
    psi: float = Field(description="Population Stability Index. <0.1 no drift, 0.1–0.25 moderate, ≥0.25 significant.")
    psi_label: str = Field(description="no_drift / moderate_drift / significant_drift.")
    threshold: float = Field(description="PSI cut-off used to flag drift.")
    n_current: int = Field(description="Number of samples in the current window.")
    current_mean: float = Field(description="Mean of amount_usd in the current window.")
    current_std: float = Field(description="Std-dev of amount_usd in the current window.")
    baseline_mean: float = Field(description="Mean of amount_usd in the baseline dataset.")
    baseline_std: float = Field(description="Std-dev of amount_usd in the baseline dataset.")
