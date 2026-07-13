"""Unit tests for the Feast-backed online feature store wrapper."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from fraud_detection.core import feature_store as feature_store_module
from fraud_detection.core.feature_store import _ONLINE_COLUMNS, FeastFeatureStore


@pytest.fixture
def fake_feast(monkeypatch) -> MagicMock:
    """Replace ``feast.FeatureStore`` with a mock returning one online row."""
    response = MagicMock()
    response.to_dict.return_value = {
        "user_id": ["user-1"],
        "card_id": ["card-1"],
        "card_brand": ["visa"],
        "card_type": [None],
        "is_virtual": [],  # empty column -> None
    }
    store = MagicMock()
    store.get_online_features_async = AsyncMock(return_value=response)
    monkeypatch.setattr(
        feature_store_module, "FeatureStore", MagicMock(return_value=store)
    )
    return store


def test_feature_refs_are_fully_qualified():
    store = FeastFeatureStore(feature_view="tx_features", repo_path="/tmp/repo")
    assert store.feature_refs == [f"tx_features:{column}" for column in _ONLINE_COLUMNS]
    assert store.store is None


async def test_open_warms_up_the_online_path(fake_feast):
    store = FeastFeatureStore(repo_path="/tmp/repo")
    await store.open()

    assert store.store is fake_feast
    fake_feast.get_online_features_async.assert_awaited_once()
    kwargs = fake_feast.get_online_features_async.await_args.kwargs
    assert kwargs["entity_rows"] == [{"user_id": "warmup", "card_id": "warmup"}]


async def test_open_survives_warmup_failure(fake_feast):
    fake_feast.get_online_features_async = AsyncMock(side_effect=Exception("no redis"))
    store = FeastFeatureStore(repo_path="/tmp/repo")
    await store.open()  # must not raise
    assert store.store is fake_feast


async def test_get_online_features_flattens_single_row(fake_feast):
    store = FeastFeatureStore(repo_path="/tmp/repo")
    await store.open()

    features = await store.get_online_features("user-1", "card-1")

    assert features["card_brand"] == "visa"
    assert features["card_type"] is None
    assert features["is_virtual"] is None  # empty column collapses to None
    assert features["user_id"] == "user-1"


async def test_get_online_features_requires_open():
    store = FeastFeatureStore(repo_path="/tmp/repo")
    with pytest.raises(RuntimeError, match="open"):
        await store.get_online_features("user-1", "card-1")


async def test_close_releases_the_store(fake_feast):
    store = FeastFeatureStore(repo_path="/tmp/repo")
    await store.open()
    await store.close()
    assert store.store is None
