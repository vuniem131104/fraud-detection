"""Drift detection service orchestrating baseline loading and current-window scoring.

Defines :class:`DriftDetectionService`, which owns the connection to Postgres and
the in-memory PSI :class:`DriftDetector` built from the training baseline. On each
request it queries the last 30 days of ``amount_usd`` from Postgres and runs a PSI
drift test against the baseline. Lifecycle is managed via :meth:`open` / :meth:`close`,
mirroring the ``fraud_detection`` service.
"""

from __future__ import annotations

import pandas as pd
from structlog import get_logger

from database import PostgresDatabase
from drift_detection.detector import COLUMN, DEFAULT_THRESHOLD, DriftDetector
from drift_detection.repository import fetch_amounts_last_30_days

logger = get_logger(__name__)

# Minimum current-window rows required for a statistically meaningful PSI.
MIN_CURRENT_ROWS = 30


class InsufficientDataError(Exception):
    """Raised when the current window has too few rows for a reliable PSI."""

    def __init__(self, found: int, required: int = MIN_CURRENT_ROWS) -> None:
        self.found = found
        self.required = required
        super().__init__(
            f"Only {found} rows found in the last 30 days. Need at least {required}."
        )


class DriftDetectionService:
    """Coordinates baseline loading and current-window drift scoring.

    Holds the long-lived Postgres pool and the PSI detector built from the training
    baseline (``amount_usd``). Lifecycle is managed via :meth:`open` and :meth:`close`.
    """

    def __init__(
        self,
        baseline_path: str,
        database: PostgresDatabase,
        threshold: float = DEFAULT_THRESHOLD,
    ) -> None:
        """Store configuration without loading the baseline or opening the pool.

        Args:
            baseline_path: Path to the training dataset (parquet) used as the drift baseline.
            database: Postgres database providing the current ``amount_usd`` window.
            threshold: Default PSI cut-off above which drift is flagged.
        """
        self.baseline_path = baseline_path
        self.database = database
        self.threshold = threshold
        self._detector: DriftDetector | None = None

    async def open(self) -> None:
        """Open the database pool and build the PSI detector from the training baseline."""
        await self.database.open()
        baseline_df = pd.read_parquet(self.baseline_path, columns=[COLUMN])
        self._detector = DriftDetector(baseline_df, threshold=self.threshold)
        logger.info(
            "Baseline loaded",
            column=COLUMN,
            rows=self._detector.baseline_rows,
            mean=round(self._detector.baseline_mean, 2),
        )
        logger.info("DriftDetectionService is ready")

    async def close(self) -> None:
        """Close the database pool and release the in-memory detector."""
        await self.database.close()
        self._detector = None
        logger.info("DriftDetectionService has been closed")

    @property
    def baseline_rows(self) -> int:
        """Number of rows in the loaded baseline (0 before :meth:`open`)."""
        return self._detector.baseline_rows if self._detector is not None else 0

    async def detect(self, threshold: float | None = None) -> dict:
        """Query the last 30 days of ``amount_usd`` from Postgres and run the PSI drift test.

        Args:
            threshold: Optional PSI cut-off overriding the service default for this call.

        Returns:
            The PSI drift result dict produced by :class:`DriftDetector`.

        Raises:
            RuntimeError: If the service has not been opened.
            InsufficientDataError: If fewer than :data:`MIN_CURRENT_ROWS` rows are available.
        """
        if self._detector is None:
            raise RuntimeError("DriftDetectionService has not been opened")

        async with self.database.connection() as conn:
            amounts = await fetch_amounts_last_30_days(conn)

        if len(amounts) < MIN_CURRENT_ROWS:
            raise InsufficientDataError(len(amounts))

        return self._detector.detect(amounts, threshold=threshold)
