"""Bridge: topic kết quả của Flink -> Feast online store (Redis).

Flink chỉ ghi được ra Kafka; Feast không có Kafka sink sẵn. Cầu nối này đọc hai
topic kết quả rồi gọi ``store.push()`` để giá trị vào **online store** ngay khi
Flink tính xong — không chờ tới nhịp batch. Chạy như service (``--max-seconds 0``).

Vì sao đi qua Feast ``push()`` chứ không ghi thẳng Redis
-------------------------------------------------------
Định dạng khoá Redis của Feast không phải tuỳ ý: khoá là
``hash(project, entity_key)`` và tên field bên trong là
``hash(f"{feature_view_name}:{feature_name}")``. Tự dựng khoá là đi bảo trì một
bản sao của chi tiết nội bộ Feast — sai một chữ là serving đọc null mà không báo.
Để Feast là **người ghi duy nhất** vào Redis thì khoá luôn đúng.

Vì sao KHÔNG đè lên giá trị batch
---------------------------------
Mỗi ``FeatureView`` có không gian tên riêng trong Redis (tên view nằm trong hash
của field, và mốc thời gian ở field riêng ``_ts:{view}``). ``merchant_features``
(batch, 30 ngày) và ``merchant_realtime`` (Flink, 10 phút) sống chung một hash mà
không đụng nhau. Luật rút ra: **một FeatureView = một người ghi.** Bug cũ xảy ra
vì một view có CẢ batch_source lẫn push_source -> ``feast materialize`` đè lên
giá trị Flink vừa đẩy.

Chạy::

    python -m include.feature_bridge --max-seconds 0        # service
    python -m include.feature_bridge --max-seconds 60       # chạy thử 1 phút
    python -m include.feature_bridge --from-beginning       # đọc lại từ đầu topic
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer
from feast import FeatureStore

try:                        # chạy như package
    from include import kafka_conf
except ImportError:        # chạy trực tiếp trong include/
    import kafka_conf

REPO = os.environ.get("FEAST_REPO_PATH", "/opt/airflow/feature_store")
LOCAL_TZ = "Asia/Ho_Chi_Minh"

# topic Flink -> (tên push source, khoá entity, cột số cần chuyển kiểu)
ROUTES = {
    "merchant_rt_10min": {
        "push_source": "merchant_rt_push_source",
        "key": "merchant_id",
        "ts_field": "merchant_rt_ts_epoch",
        "int_cols": ["merch_tx_count_10min", "merch_distinct_cards_10min"],
        "float_cols": ["merch_amount_avg_10min"],
    },
    "device_rt_1h": {
        "push_source": "device_rt_push_source",
        "key": "device_id",
        "ts_field": "device_rt_ts_epoch",
        "int_cols": ["device_tx_count_1h", "device_distinct_users_1h",
                     "device_distinct_cards_1h"],
        "float_cols": [],
    },
}


def make_consumer(bootstrap: str, group: str, from_beginning: bool) -> Consumer:
    """Kafka consumer đọc mọi topic kết quả của Flink."""
    return Consumer(kafka_conf.client_config(
        **{
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
        }
    ))


def to_frame(route: dict, records: list[dict]) -> pd.DataFrame:
    """Message của Flink -> DataFrame đúng schema push source."""
    df = pd.DataFrame(records)
    key = route["key"]
    # upsert-kafka có thể gửi nhiều bản của cùng key trong một batch -> giữ bản mới nhất
    df = df.sort_values("window_end").drop_duplicates(key, keep="last")

    # ``window_end`` của Flink là giờ ĐỊA PHƯƠNG: producer gửi created_at dạng
    # ISO-8601 naive theo giờ HCM, cột trong Flink khai TIMESTAMP(3) (không mang
    # timezone) nên Flink giữ nguyên. Parse thẳng utc=True sẽ lệch 7 tiếng ->
    # *_ts_epoch sai -> phép kiểm tra độ tươi trong ODFV luôn cho "stale".
    win_end = (pd.to_datetime(df["window_end"])
               .dt.tz_localize(LOCAL_TZ)
               .dt.tz_convert("UTC"))

    # Tên field trong online store mang tiền tố ``raw_``: Feast KHÔNG filter online
    # read theo ttl, nên giá trị đọc ra có thể đã quá hạn. ODFV ``txn_on_demand`` so
    # ``*_ts_epoch`` với thời điểm giao dịch rồi mới xuất ra tên chuẩn (không tiền
    # tố). Nhờ vậy model không bao giờ thấy giá trị chưa kiểm tra độ tươi.
    out = pd.DataFrame({key: df[key].astype(str)})
    for c in route["int_cols"]:
        out[f"raw_{c}"] = df[c].astype("int64")
    for c in route["float_cols"]:
        out[f"raw_{c}"] = df[c].astype("float64")
    # mốc của giá trị -> ODFV so với thời điểm giao dịch để biết còn tươi hay không
    out[route["ts_field"]] = (win_end.astype("int64") // 10**9).astype("int64")
    out["event_timestamp"] = win_end
    out["created"] = datetime.now(timezone.utc)
    return out


def run(args: argparse.Namespace) -> int:
    """Đọc hai topic, gom theo batch rồi push vào Feast online store."""
    store = FeatureStore(repo_path=REPO)
    consumer = make_consumer(args.bootstrap, args.group, args.from_beginning)
    topics = list(ROUTES)
    consumer.subscribe(topics)
    print(f"Bridge: {topics} -> Feast push | kafka={kafka_conf.describe()} "
          f"(batch={args.batch_size}, "
          f"{'service' if args.max_seconds == 0 else str(args.max_seconds) + 's'})")

    buf: dict[str, list[dict]] = {t: [] for t in topics}
    pushed = {t: 0 for t in topics}

    def flush(topic: str) -> None:
        if not buf[topic]:
            return
        route = ROUTES[topic]
        store.push(route["push_source"], to_frame(route, buf[topic]))
        pushed[topic] += len(buf[topic])
        buf[topic] = []

    t0 = time.time()
    try:
        while args.max_seconds == 0 or time.time() - t0 < args.max_seconds:
            msg = consumer.poll(1.0)
            if msg is None:                       # hết hàng -> đẩy phần còn lại
                for t in topics:
                    flush(t)
                continue
            if msg.error():
                print("  kafka error:", msg.error())
                continue
            val = msg.value()
            if not val:                           # tombstone của upsert-kafka
                continue
            topic = msg.topic()
            if topic not in ROUTES:
                continue
            buf[topic].append(json.loads(val))
            if len(buf[topic]) >= args.batch_size:
                flush(topic)
                print("  pushed: " + "  ".join(f"{k}={v:,}" for k, v in pushed.items()))
        for t in topics:
            flush(t)
    finally:
        consumer.close()

    print("Done. " + "  ".join(f"{k}={v:,}" for k, v in pushed.items()))
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI cho bridge."""
    p = argparse.ArgumentParser(description="Kafka (Flink) -> Feast online store.")
    p.add_argument("--bootstrap", default=kafka_conf.bootstrap())
    p.add_argument("--group", default="feast-feature-bridge")
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--max-seconds", type=int, default=60,
                   help="0 = chạy mãi (dùng khi làm service).")
    p.add_argument("--from-beginning", action="store_true")
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
