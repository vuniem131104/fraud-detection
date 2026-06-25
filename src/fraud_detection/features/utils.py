"""Helpers for building the Redis key names used by the feature store.

Centralises the key naming scheme so that producers (workers, scripts) and
consumers (the prediction service) always agree on where per-user/per-card
transaction history and computed features are stored.
"""


def build_transactions_key(user_id: str, card_id: str) -> str:
    """Return the Redis key holding the transaction history for a user/card pair."""
    return f"user:card:transactions:{user_id}_{card_id}"


def build_features_key(user_id: str, card_id: str) -> str:
    """Return the Redis key holding the computed features for a user/card pair."""
    return f"user:card:features:{user_id}_{card_id}"
