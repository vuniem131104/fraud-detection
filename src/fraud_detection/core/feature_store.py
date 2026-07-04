"""Feast-backed online feature store for fraud-detection serving.

Wraps a Feast ``FeatureStore`` and reads the model's precomputed features for a
``(user_id, card_id)`` pair from the online store (Redis) via the async API.
The store is opened once at application startup and warmed up so the first real
request does not pay the one-off registry-load cost (~seconds for a SQL
registry).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from feast import FeatureStore
from structlog import get_logger

logger = get_logger(__name__)

# feature_store.py lives at src/fraud_detection/core/; the Feast repo
# (feature_store.yaml) lives at src/feature_store/ from the project root.
_DEFAULT_REPO_PATH = Path(__file__).resolve().parents[2] / "feature_store"


class FeastFeatureStore:
    """Read the model's online features for a user/card pair from Feast.

    Holds a long-lived ``FeatureStore`` and the fully-qualified feature
    references (``<feature_view>:<column>``) for every model feature. Reads use
    ``get_online_features_async`` so they do not block the event loop.
    """

    def __init__(
        self,
        feature_columns: list[str],
        feature_view: str = "transaction_features",
        repo_path: str | None = None,
    ) -> None:
        """Configure the store with the columns and Feast repo to read from.

        Args:
            feature_columns: Model feature column names (from the schema); each
                is resolved to ``<feature_view>:<column>`` for the online read.
            feature_view: Name of the Feast feature view holding the columns.
            repo_path: Path to the Feast repo (dir containing
                ``feature_store.yaml``). Defaults to ``$FEAST_REPO_PATH`` or the
                project's ``src/feature_store``.
        """
        self.repo_path = repo_path or os.getenv("FEAST_REPO_PATH") or str(_DEFAULT_REPO_PATH)
        self.feature_view = feature_view
        self.feature_refs = [f"{feature_view}:{column}" for column in feature_columns]
        self.store: FeatureStore | None = None

    async def open(self) -> None:
        """Construct the Feast store and warm up the online read path.

        The warm-up issues one throwaway online read so the SQL registry load,
        provider construction and Redis connection all happen at startup instead
        of on the first user request. Warm-up failures are logged, not fatal.
        """
        self.store = FeatureStore(repo_path=self.repo_path)
        try:
            await self.get_online_features("warmup", "warmup")
            logger.info("Feast online store warmed up", extra={"repo_path": self.repo_path})
        except Exception as exc:
            logger.warning(
                "Feast online store warm-up failed",
                extra={"repo_path": self.repo_path, "error": str(exc)},
            )

    async def get_online_features(self, user_id: str, card_id: str) -> dict[str, Any]:
        """Fetch the model features for a user/card pair from the online store.

        Args:
            user_id: Identifier of the user (Feast entity join key).
            card_id: Identifier of the card (Feast entity join key).

        Returns:
            A flat dict mapping each feature column (and the entity join keys) to
            its single online value, with ``None`` for features that have not
            been materialised for this pair.
        """
        if self.store is None:
            raise RuntimeError("FeastFeatureStore.open() must be called before reading features")

        entity_rows = [{"user_id": user_id, "card_id": card_id}]
        response = await self.store.get_online_features_async(
            features=self.feature_refs,
            entity_rows=entity_rows,
        )
        columns = response.to_dict()
        return {name: (values[0] if values else None) for name, values in columns.items()}

    async def close(self) -> None:
        """Release the Feast store. No persistent resources need explicit teardown."""
        self.store = None
