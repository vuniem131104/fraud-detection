"""Kafka ``transactions`` -> Postgres ``ops.transactions`` (bảng của TEAM DATA).

Đây là *ingestion service của phòng ban vận hành*: nhận event thanh toán từ Kafka
rồi ghi xuống bảng landing của họ. Bảng này mới là **system of record** cho luồng
batch — file ngày được dump ra từ đây, KHÔNG phải replay Kafka (Kafka chỉ là
transport, retention ngắn).

Giao hàng at-least-once nên bảng cố ý **không có primary key**: duplicate ~1.5%
mà producer tiêm vào sẽ nằm nguyên trong bảng, rồi chảy xuống file → Bronze, để
DP2 (Spark) khử. Đúng vòng đời của một lỗi dữ liệu thật.

Chạy::

    python -m include.kafka_to_ops --max-seconds 120
    python -m include.kafka_to_ops --from-beginning --max-seconds 60
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg
from confluent_kafka import Consumer

try:                        # chạy như package
    from include import kafka_conf
except ImportError:        # chạy trực tiếp trong include/
    import kafka_conf

COLS = ["id", "user_id", "card_id", "merchant_id", "device_id", "amount_usd",
        "currency", "channel", "billing_country_code", "ip_country_code",
        "email_purchaser", "email_recipient", "created_at", "auth_3ds_flag"]

# Producer gửi ``created_at`` dạng ISO-8601 **naive theo giờ HCM**: cột trong Flink
# khai TIMESTAMP(3) nên gửi kèm offset sẽ bị parse lệch (xem
# ``generator/generate_stream.py``). Còn ops.transactions.created_at là TIMESTAMPTZ,
# nên COPY một datetime naive vào đó là để Postgres diễn giải theo session
# ``TimeZone`` — Cloud SQL mặc định UTC và repo không set PGTZ ở đâu cả, nên giờ HCM
# bị đóng dấu nhãn UTC và mọi giao dịch live lệch +7h. Phải localize lại đúng như
# ``include/feature_bridge.py`` làm với ``window_end`` của Flink.
LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def ops_dsn() -> str:
    """DSN tới DB vận hành (opsdb)."""
    return (f"host={os.environ.get('PG_HOST', 'postgres')} "
            f"port={os.environ.get('POSTGRES_PORT', '5432')} "
            f"dbname={os.environ.get('OPS_POSTGRES_DB', 'opsdb')} "
            f"user={os.environ.get('POSTGRES_USER') or os.environ['AIRFLOW_USER']} "
            f"password={os.environ.get('POSTGRES_PASSWORD') or os.environ['AIRFLOW_PASSWORD']}")


def make_consumer(bootstrap: str, group: str, from_beginning: bool) -> Consumer:
    """Consumer đọc topic transactions.

    Phần bảo mật (SASL_SSL/OAUTHBEARER cho Managed Kafka) lấy từ
    ``include.kafka_conf`` — local để mặc định PLAINTEXT nên không đổi hành vi.
    """
    return Consumer(kafka_conf.client_config(
        **{
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
        }
    ))


def to_row(msg: dict) -> tuple:
    """Đổi 1 message JSON thành tuple theo thứ tự COLS.

    ``created_at`` được localize về HCM trước khi ghi — xem ghi chú ở ``LOCAL_TZ``.
    Có kiểm ``tzinfo`` thay vì ``replace`` thẳng: nếu sau này producer đổi sang gửi
    kèm offset thì phải tôn trọng offset đó, chứ ``replace`` sẽ ghi đè trong im lặng
    và lỗi lệch giờ quay lại y như cũ mà không ai thấy.
    """
    created_at = datetime.fromisoformat(msg["created_at"])
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=LOCAL_TZ)
    return (
        msg["id"], msg["user_id"], msg["card_id"], msg["merchant_id"], msg["device_id"],
        float(msg["amount_usd"]), msg.get("currency"), msg.get("channel"),
        msg.get("billing_country_code"), msg.get("ip_country_code"),
        msg.get("email_purchaser"), msg.get("email_recipient"),
        created_at, msg.get("auth_3ds_flag"),
    )


def flush(conn: psycopg.Connection, rows: list[tuple]) -> int:
    """COPY một batch row vào ops.transactions."""
    if not rows:
        return 0
    with conn.cursor() as cur, cur.copy(
            f"COPY ops.transactions ({','.join(COLS)}) FROM STDIN") as cp:
        for r in rows:
            cp.write_row(r)
    conn.commit()
    return len(rows)


def run(args: argparse.Namespace) -> int:
    """Đọc Kafka trong ``max_seconds`` giây, ghi batch xuống ops.transactions."""
    consumer = make_consumer(args.bootstrap, args.group, args.from_beginning)
    consumer.subscribe([args.topic])
    print(f"Ingest: {args.topic} -> ops.transactions | "
          f"kafka={kafka_conf.describe()} "
          f"(batch={args.batch_size}, {args.max_seconds}s)")

    t0, buf, total = time.time(), [], 0
    with psycopg.connect(ops_dsn()) as conn:
        try:
            while args.max_seconds == 0 or time.time() - t0 < args.max_seconds:
                msg = consumer.poll(1.0)
                if msg is None:
                    total += flush(conn, buf)
                    buf = []
                    continue
                if msg.error():
                    print("  kafka error:", msg.error())
                    continue
                buf.append(to_row(json.loads(msg.value())))
                if len(buf) >= args.batch_size:
                    total += flush(conn, buf)
                    buf = []
                    print(f"  ingested={total:,}")
            total += flush(conn, buf)
        finally:
            consumer.close()

    print(f"Done. {total:,} transaction vào ops.transactions.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI cho ingestion service."""
    p = argparse.ArgumentParser(description="Kafka transactions -> ops.transactions")
    p.add_argument("--bootstrap", default=kafka_conf.bootstrap())
    p.add_argument("--topic", default="transactions")
    p.add_argument("--group", default="ops-ingest")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--max-seconds", type=int, default=120,
                   help="0 = chạy mãi (dùng khi làm service).")
    p.add_argument("--from-beginning", action="store_true")
    return p


if __name__ == "__main__":
    raise SystemExit(run(build_parser().parse_args()))
