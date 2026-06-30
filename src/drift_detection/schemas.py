"""Pydantic schemas for the drift detection API."""

from __future__ import annotations

from pydantic import BaseModel, Field



class DriftRequest(BaseModel):
    """Request body: a list of TransactionAmt values to check against the baseline."""

    amounts: list[float] = Field(
        ...,
        min_length=30,
        description="List of TransactionAmt values (minimum 30 samples for reliable stats).",
        examples=[[100.5, 200.0, 75.3, 312.0]],
    )


class DriftResult(BaseModel):
    """Drift detection result for the TransactionAmt column."""

    column: str = Field(description="Column name that was tested.")
    drift_detected: bool = Field(description="True if PSI >= 0.1.")
    psi: float = Field(description="Population Stability Index. <0.1 no drift, 0.1–0.25 moderate, ≥0.25 significant.")
    psi_label: str = Field(description="no_drift / moderate_drift / significant_drift.")
    n_current: int = Field(description="Number of samples in the current window.")
    current_mean: float = Field(description="Mean of TransactionAmt in the current window.")
    current_std: float = Field(description="Std-dev of TransactionAmt in the current window.")
    baseline_mean: float = Field(description="Mean of TransactionAmt in the baseline dataset.")
    baseline_std: float = Field(description="Std-dev of TransactionAmt in the baseline dataset.")



class HealthResponse(BaseModel):
    status: str
    baseline_rows: int
    baseline_mean: float
