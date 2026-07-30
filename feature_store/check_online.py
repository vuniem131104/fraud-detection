"""Smoke test: đọc feature từ ONLINE store (Redis) qua feature service.

Lấy một bộ entity có thật, gọi ``get_online_features`` đúng như lúc ``/score`` sẽ
làm, rồi in cả bốn tầng feature để kiểm tra đường Feast đã thông.

Cái này kiểm được những thứ mà test đơn vị không kiểm được:

* ``feast materialize`` đã ghi đúng 4 view batch chưa
* bridge đã push được giá trị Flink chưa (và phép kiểm tra độ tươi có ăn không)
* ODFV có tính ra đủ 22 feature dẫn xuất không

**Không** kiểm velocity 5 phút: nhóm đó do code API tính bằng Redis sorted set, ở
đây ta truyền giá trị giả qua RequestSource để ODFV chạy được.
Xem ``src/fraud_detection/features/velocity.py`` và
``tests/test_velocity_parity.py``.

Chạy::

    docker compose exec -w /opt/airflow/feature_store airflow-scheduler \\
      python check_online.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import psycopg
from feast import FeatureStore

SERVICE = "fraud_detection_service"


def sample_entity_row() -> dict:
    """Lấy một bộ (card, user, merchant, device) có hoạt động thật.

    Chọn thẻ hoạt động nhiều nhất để feature có giá trị khác 0 — thẻ ngẫu nhiên
    trong 35k thẻ gần như luôn im lặng và mọi số sẽ là 0, không kiểm được gì.
    """
    dsn = (f"host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']} "
           f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} "
           f"password={os.environ['POSTGRES_PASSWORD']}")
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT card_id, user_id, merchant_id, device_id
            FROM application.feat_training
            ORDER BY card_tx_count_90d DESC, merch_tx_count_10min DESC
            LIMIT 1
        """)
        card_id, user_id, merchant_id, device_id = cur.fetchone()
    return {"card_id": card_id, "user_id": user_id,
            "merchant_id": merchant_id, "device_id": device_id}


def main() -> None:
    store = FeatureStore(repo_path=".")
    ent = sample_entity_row()
    now = datetime.now(timezone.utc)

    # entity_rows = khoá entity + mọi trường của RequestSource ``txn_request``
    row = dict(ent)
    row.update({
        "amount_usd": 1999.0,
        "billing_country_code": "US",
        "ip_country_code": "RU",                  # cố ý lệch -> geo_mismatch = 1
        "email_purchaser": "a@x.com",
        "email_recipient": "cashout@proton.me",   # khác -> recipient_differs = 1
        "event_ts_epoch": int(now.timestamp()),
        # velocity do API tính; ở đây truyền giá trị giả để ODFV chạy được
        "card_tx_count_5min_req": 3,
        "card_amount_sum_5min_req": 10.2,
        "card_amount_avg_5min_req": 3.4,
    })

    resp = store.get_online_features(
        features=store.get_feature_service(SERVICE), entity_rows=[row]).to_dict()

    def show(title: str, keys: tuple[str, ...]) -> None:
        print(f"\n--- {title} ---")
        for k in keys:
            if k in resp:
                print(f"  {k:<30}: {resp[k][0]}")
            else:
                print(f"  {k:<30}: (KHÔNG có trong service!)")

    print("Entity:", {k: v[:12] + "..." for k, v in ent.items()})

    show("BATCH — materialize từ Postgres", (
        "card_brand", "card_type", "is_virtual",
        "card_tx_count_90d", "card_tx_count_7d", "card_amount_avg_90d",
        "card_amount_max_90d", "card_amount_std_90d", "card_distinct_merchant_90d",
        "customer_segment", "kyc_level", "user_country",
        "user_tx_count_30d", "user_device_count_30d", "user_distinct_country_30d",
        "merchant_category", "merchant_risk_level",
        "merchant_tx_count_30d", "merchant_amount_avg_30d", "merchant_amount_std_30d",
        "device_tx_count_30d", "device_distinct_users_30d", "device_distinct_cards_30d",
    ))

    # Nếu cả 6 số này bằng 0 thì có hai khả năng: Flink/bridge chưa chạy, HOẶC
    # entity này im lặng nên giá trị đã quá hạn và bị ODFV gate về 0 (đúng hành vi).
    # Đối chiếu với raw_* + *_ts_epoch trong Redis để phân biệt.
    show("FLINK — bridge push (đã gate độ tươi)", (
        "merch_tx_count_10min", "merch_distinct_cards_10min", "merch_amount_avg_10min",
        "device_tx_count_1h", "device_distinct_users_1h", "device_distinct_cards_1h",
    ))

    show("ON-DEMAND — tính tại request", (
        "log_amount", "hour", "weekday", "is_night",
        "geo_mismatch", "foreign_ip", "recipient_differs",
        "account_age_days", "card_age_days", "device_age_hours",
        "amount_vs_card_avg", "amount_vs_card_max", "amount_z_vs_card",
        "amount_vs_user_avg", "amount_z_vs_merchant",
        "card_acceleration", "hours_since_last_card_tx", "hours_since_last_user_tx",
        "merchant_spread", "merchant_burst", "device_burst", "device_cards_per_user",
    ))

    show("ĐỒNG BỘ — velocity do API tính (ở đây là giá trị giả)", (
        "card_tx_count_5min", "card_amount_sum_5min", "card_amount_avg_5min",
    ))


if __name__ == "__main__":
    main()
