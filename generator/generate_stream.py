"""Luồng LIVE: bắn transactions vào Managed Kafka theo thời gian thực.

Chạy liên tục từ 29/07/2026 trở đi. Mỗi ngày, script lập **kế hoạch** cho trọn
ngày đó (00:00 -> 24:00 giờ HCM) rồi phát từng message **đúng lúc** — nên nhìn từ
ngoài vào nó giống hệ thống thật đang hoạt động.

Vì sao phải LẬP KẾ HOẠCH rồi phát, thay vì "lấy một giao dịch bất kỳ và đóng dấu
thời gian = now"
-----------------------------------------------------------------------------
Cách sau phá vỡ cấu trúc thời gian của fraud. Archetype ``card_testing`` là 12-45
giao dịch cách nhau 4-90 GIÂY trên cùng một thẻ; nếu phát mỗi lần một giao dịch
rồi ngủ ``1/rate`` giây (ở nhịp ~817/ngày là ~105 giây/giao dịch) thì burst đó bị
kéo dãn thành hơn một giờ. Velocity 5 phút sẽ không bao giờ thấy gì, và Flink
cũng vậy. Giữ nguyên timestamp đã lập kế hoạch thì burst còn là burst.

Ba lỗi streaming được tiêm
--------------------------
1. **burst** — KHÔNG tiêm nhân tạo. Ở nhịp ~817 giao dịch/ngày (0,0095 msg/s),
   một "spike x10 trong 5 giây" chỉ thêm 0,5 message: không đo được. Burst thật
   trong hệ này là chính các episode fraud (card_testing dồn 12-45 giao dịch vào
   vài phút trên cùng thẻ + cùng merchant). Đó là burst có ý nghĩa duy nhất, và
   đúng là thứ velocity/Flink phải chịu.
2. **late arrival** — event-time giữ nguyên (thời điểm giao dịch thật), chỉ thời
   điểm GỬI bị đẩy về sau 5-60 giây. Đúng nghĩa "hàng về muộn" mà watermark của
   Flink (90s) phải chờ.
3. **duplicate** — gửi lại y hệt (trùng ``id``) sau 0,5-5 giây, mô phỏng
   at-least-once của Kafka.

Nhịp streaming khớp nhịp lịch sử (``transactions.count / số ngày``). Nếu để
streaming đông hơn lịch sử hàng chục lần thì ``merch_*_10min`` lúc serve sẽ lớn
gấp hàng nghìn lần lúc train — model vô dụng.

Chạy::

    uv run python data_pipelines/generator/generate_stream.py               # live, chạy mãi
    uv run python data_pipelines/generator/generate_stream.py --date 2026-07-28 --drain
    uv run python data_pipelines/generator/generate_stream.py --speedup 60  # demo nhanh
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from confluent_kafka import Producer

import ops_store
from generate_offline import (
    build_transactions_df,
    load_config,
    patch_generator,
    window_days,
)
from ops_store import REPO_ROOT, gen

# Lập kế hoạch với chân trời 45 ngày rồi chỉ giữ đúng ngày mục tiêu.
#
# Vì sao không dùng days=1: các archetype cần lịch sử dài hơn một ngày để dựng
# bối cảnh. bust_out có warm-up 21-45 ngày, account_takeover cần changepoint
# trong 30 ngày, fraud_ring lấy mốc trong [now-days, now-2 ngày] — với days=1
# khoảng này rỗng và archetype không sinh được gì. Sinh với chân trời 45 ngày rồi
# lọc lấy ngày mục tiêu cho ra ĐÚNG hỗn hợp 4 archetype.
PLAN_HORIZON_DAYS = 45


def make_producer(bootstrap: str) -> Producer:
    """Kafka producer tới Managed Kafka (SASL_SSL).

    Phần bảo mật lấy từ ``ops_store.kafka_client_config``.
    """
    return Producer(ops_store.kafka_client_config(
        **{"bootstrap.servers": bootstrap, "linger.ms": 20,
           "compression.type": "snappy"}))


def daily_count(cfg: dict) -> int:
    """Số giao dịch mô phỏng cho một ngày (khớp nhịp lịch sử)."""
    scfg = cfg.get("streaming") or {}
    if scfg.get("daily_count"):
        return int(scfg["daily_count"])
    return max(cfg["transactions"]["count"] // max(window_days(cfg), 1), 1)


# --------------------------------------------------------------------------- #
# Lập kế hoạch một ngày                                                        #
# --------------------------------------------------------------------------- #

def plan_day(cfg: dict, entities: tuple, date_str: str, seed: int):
    """Sinh trọn giao dịch của ngày ``date_str``, sắp theo event-time.

    Trả DataFrame đã tiêm schema-evolution (ngày live luôn sau cutover nên cột
    ``auth_3ds_flag`` luôn có mặt).
    """
    rng = random.Random(seed)
    nprng = np.random.default_rng(seed)
    users, cards, per_user, merchants, device_ids, device_types = entities
    tcfg = cfg["transactions"]

    # now = 00:00 ngày HÔM SAU -> cửa sổ [now-45d, now] chứa trọn ngày mục tiêu
    day_end = (datetime.fromisoformat(date_str).replace(tzinfo=gen.HCM_TZ)
               + timedelta(days=1))
    want = daily_count(cfg)
    # sinh dày hơn theo tỉ lệ chân trời, rồi lọc -> kỳ vọng ~want dòng trong ngày
    gross = want * PLAN_HORIZON_DAYS

    tx_rows, _labels, _stats = gen.generate_transactions(
        gross, users, cards, per_user, merchants, device_ids, device_types,
        rng, nprng, day_end, PLAN_HORIZON_DAYS, tcfg["fraud_rate"],
        label_cutoff_days=0, patterns=tcfg["patterns"], difficulty=tcfg["difficulty"])

    df = build_transactions_df(tx_rows)
    df = df[df["event_date"] == date_str].sort_values("created_at").reset_index(drop=True)
    # ngày live > cutover nên hệ nguồn LUÔN gửi cột này
    df["auth_3ds_flag"] = nprng.random(len(df)) < 0.6
    return df


def to_payload(row: dict) -> bytes:
    """Dòng kế hoạch -> JSON cho Kafka.

    ``created_at`` gửi dạng ISO-8601 **naive theo giờ HCM**: cột trong Flink khai
    ``TIMESTAMP(3)`` (không mang timezone) nên gửi kèm offset sẽ bị parse lệch.
    Bridge sau đó localize lại HCM trước khi đổi UTC — xem
    ``include/feature_bridge.py``.
    """
    msg = {
        "id": row["id"], "user_id": row["user_id"], "card_id": row["card_id"],
        "merchant_id": row["merchant_id"], "device_id": row["device_id"],
        "amount_usd": float(row["amount_usd"]), "currency": row["currency"],
        "channel": row["channel"],
        "billing_country_code": row["billing_country_code"],
        "ip_country_code": row["ip_country_code"],
        "email_purchaser": row["email_purchaser"],
        "email_recipient": row["email_recipient"],
        "auth_3ds_flag": bool(row["auth_3ds_flag"]),
        "created_at": (row["created_at"].tz_convert(gen.HCM_TZ)
                       .tz_localize(None).isoformat(timespec="milliseconds")),
    }
    return json.dumps(msg).encode()


def build_schedule(df, scfg: dict, rng: random.Random, counter) -> list[tuple]:
    """Đổi kế hoạch ngày thành lịch GỬI: [(send_at, seq, key, payload)].

    ``send_at`` = event-time, cộng thêm độ trễ nếu message thuộc nhóm "về muộn".
    Duplicate được xếp thêm một lần gửi 0,5-5 giây sau bản đầu.
    """
    late_rate = scfg.get("late_rate", 0.05)
    late_min = scfg.get("late_delay_min_sec", 5)
    late_max = scfg.get("late_delay_max_sec", 60)
    dup_rate = scfg.get("duplicate_rate", 0.015)

    out = []
    for row in df.to_dict("records"):
        payload = to_payload(row)
        key = row["card_id"].encode()
        send_at = row["created_at"]
        if rng.random() < late_rate:                       # (2) late arrival
            send_at = send_at + timedelta(seconds=rng.uniform(late_min, late_max))
        out.append((send_at, next(counter), key, payload))
        if rng.random() < dup_rate:                        # (3) duplicate
            out.append((send_at + timedelta(seconds=rng.uniform(0.5, 5.0)),
                        next(counter), key, payload))
    heapq.heapify(out)
    return out


# --------------------------------------------------------------------------- #
# Vòng chạy                                                                    #
# --------------------------------------------------------------------------- #

def run(cfg: dict, args: argparse.Namespace) -> int:
    """Lập kế hoạch từng ngày rồi phát đúng lịch, sang ngày mới thì lập lại."""
    scfg = cfg.get("streaming") or {}
    speedup = max(float(args.speedup or scfg.get("speedup", 1) or 1), 0.01)
    rng = random.Random(cfg["seed"] + 777)
    counter = itertools.count()          # phá thế hoà khi hai message cùng send_at

    patch_generator(cfg)
    print("Đọc reference data từ ops.* ...")
    frames = ops_store.load_dims(cfg)
    entities = ops_store.reconstruct_entities(frames, rng)
    print(f"  users={len(entities[0]):,} cards={len(entities[1]):,} "
          f"merchants={len(entities[3]):,} devices={len(entities[4]):,}")

    kcfg = cfg.get("kafka") or {}
    bootstrap = args.bootstrap or os.environ["KAFKA_BOOTSTRAP"]
    topic = args.topic or kcfg.get("topic", "transactions")
    producer = make_producer(bootstrap)

    # Đồng hồ mô phỏng: speedup=1 thì trùng thời gian thật.
    day = args.date or datetime.now(gen.HCM_TZ).strftime("%Y-%m-%d")
    t0_wall = time.time()
    if args.drain:
        # Bù một ngày đã qua: đặt đồng hồ ở CUỐI ngày đó nên mọi message đều quá
        # hạn -> phát ngay, không chờ. Event-time trong message vẫn là giờ thật.
        t0_sim = (datetime.fromisoformat(day).replace(tzinfo=gen.HCM_TZ)
                  + timedelta(days=1, minutes=1))
    else:
        t0_sim = datetime.now(gen.HCM_TZ)

    def sim_now() -> datetime:
        return t0_sim + timedelta(seconds=(time.time() - t0_wall) * speedup)

    print(f"Lập kế hoạch ngày {day} (~{daily_count(cfg):,} giao dịch) ...")
    plan = build_schedule(
        plan_day(cfg, entities, day, cfg["seed"] + int(day.replace("-", ""))),
        scfg, rng, counter)
    print(f"Bắn vào {bootstrap} ({ops_store.kafka_describe()}) topic='{topic}' | "
          f"speedup=x{speedup:g} | {len(plan):,} message trong lịch")

    sent = late_drain = 0
    t_report = time.time()
    try:
        while True:
            if not plan:
                if args.drain or args.once:
                    break
                # sang ngày mới: reference data đổi một ít (để SCD2 có việc),
                # rồi lập kế hoạch ngày kế tiếp
                nxt = (datetime.fromisoformat(day) + timedelta(days=1)).strftime("%Y-%m-%d")
                churn = ops_store.apply_dim_churn(cfg, rng)
                print(f"\n[{day} xong] dim churn: "
                      + "  ".join(f"{k}={v}" for k, v in churn.items()))
                frames = ops_store.load_dims(cfg)
                entities = ops_store.reconstruct_entities(frames, rng)
                day = nxt
                print(f"Lập kế hoạch ngày {day} ...")
                plan = build_schedule(
                    plan_day(cfg, entities, day, cfg["seed"] + int(day.replace("-", ""))),
                    scfg, rng, counter)
                continue

            send_at, _seq, key, payload = plan[0]
            wait = (send_at - sim_now()).total_seconds() / speedup
            if wait > 0:
                # Ngủ tối đa 1 giây một nhịp để Ctrl-C phản hồi nhanh và producer
                # được poll thường xuyên (delivery callback + tránh queue full).
                time.sleep(min(wait, 1.0))
                producer.poll(0)
                continue

            heapq.heappop(plan)
            producer.produce(topic, key=key, value=payload)
            producer.poll(0)
            sent += 1
            if wait < -5:            # đã quá hạn: đang drain phần quá khứ của ngày
                late_drain += 1

            if time.time() - t_report >= 10:
                t_report = time.time()
                print(f"  [{sim_now():%H:%M:%S}] sent={sent:,} "
                      f"còn lại={len(plan):,} (drain={late_drain:,})")
    except KeyboardInterrupt:
        print("\n(dừng bằng Ctrl-C)")
    finally:
        producer.flush(15)

    print(f"\nDone. sent={sent:,} message vào topic '{topic}'.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI cho producer."""
    p = argparse.ArgumentParser(description="Bắn transactions live vào Managed Kafka.")
    p.add_argument("--config", type=Path,
                   default=Path(__file__).with_name("generator_config.yaml"))
    p.add_argument("--bootstrap", default=None, help="ghi đè kafka.bootstrap.")
    p.add_argument("--topic", default=None, help="ghi đè kafka.topic.")
    p.add_argument("--date", default=None,
                   help="YYYY-MM-DD (mặc định: hôm nay). Dùng với --drain để bù ngày đã qua.")
    p.add_argument("--drain", action="store_true",
                   help="Phát HẾT ngày đó ngay lập tức rồi dừng (bù ngày bị thiếu).")
    p.add_argument("--once", action="store_true",
                   help="Chạy hết ngày hiện tại rồi dừng (không sang ngày mới).")
    p.add_argument("--speedup", type=float, default=None,
                   help="Nén thời gian: 60 = 1 ngày trong 24 phút (demo).")
    return p


def main() -> int:
    """Load .env + config rồi chạy producer."""
    gen.load_dotenv(REPO_ROOT / ".env")
    args = build_parser().parse_args()
    return run(load_config(args.config), args)


if __name__ == "__main__":
    raise SystemExit(main())
