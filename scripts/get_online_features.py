"""Đọc thử online feature (đã materialize vào Redis) qua Feast cho 1 cặp user_id/card_id.

Chạy:
    python scripts/get_online_features.py [user_id] [card_id]

Mặc định dùng user_id/card_id truyền vào lúc viết script này nếu không có argv.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from feast import FeatureStore

REPO_ROOT = Path(__file__).resolve().parent.parent
FEATURE_STORE_DIR = REPO_ROOT / "src" / "feature_store"

def main() -> None:

    user_id = sys.argv[1] if len(sys.argv) > 1 else "5719da0a5ef544c88ae8218ba58a2943"
    card_id = sys.argv[2] if len(sys.argv) > 2 else "32a0523848384228aa55b338ad19a9c2"

    fs = FeatureStore(repo_path=str(FEATURE_STORE_DIR))

    result = fs.get_online_features(
        features=fs.get_feature_service("fraud_detection_service"),
        entity_rows=[{"user_id": user_id, "card_id": card_id}],
    ).to_dict()

    print(f"user_id={user_id}")
    print(f"card_id={card_id}")
    print("-" * 60)
    for key, values in result.items():
        if key in ("user_id", "card_id"):
            continue
        print(f"{key:<28} = {values[0]}")


if __name__ == "__main__":
    main()
