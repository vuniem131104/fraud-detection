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

Report chất lượng
-----------------
Service chạy liên tục (``restart: unless-stopped``) nên report được in ở BA chỗ,
đọc trực tiếp bằng ``docker logs stream-generator``:

* mỗi ngày, ngay sau khi lập kế hoạch — chất lượng của ngày sắp phát (burst,
  late/duplicate đã tiêm, nhịp so với lịch sử);
* cuối mỗi ngày, trước khi sang ngày mới — tổng kết luỹ kế;
* khi dừng (Ctrl-C **hoặc SIGTERM của ``docker stop``**) — report cuối.

Số đo quan trọng nhất là **out-of-orderness**: với mỗi message đã gửi, khoảng
cách giữa event-time của nó và event-time lớn nhất đã gửi. Đó đúng là thứ
``WATERMARK FOR created_at AS created_at - INTERVAL '90' SECOND`` của Flink nhìn
thấy, nên bậc ``>90s`` trong histogram = số message Flink sẽ BỎ khỏi window.

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
import signal
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from confluent_kafka import Producer

import ops_store
from generate_offline import (
    build_transactions_df,
    load_config,
    patch_generator,
    rows_to_df,
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

    Trả ``(df, fraud_ids)``: DataFrame đã tiêm schema-evolution (ngày live luôn
    sau cutover nên cột ``auth_3ds_flag`` luôn có mặt) và tập id fraud của ngày.
    ``fraud_ids`` KHÔNG đi vào message — nó chỉ dùng cho report (ground truth
    nằm ở ``application.labels``, luồng live không được biết label).
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

    tx_rows, label_rows, _stats = gen.generate_transactions(
        gross, users, cards, per_user, merchants, device_ids, device_types,
        rng, nprng, day_end, PLAN_HORIZON_DAYS, tcfg["fraud_rate"],
        label_cutoff_days=0, patterns=tcfg["patterns"], difficulty=tcfg["difficulty"])

    df = build_transactions_df(tx_rows)
    df = df[df["event_date"] == date_str].sort_values("created_at").reset_index(drop=True)
    # ngày live > cutover nên hệ nguồn LUÔN gửi cột này
    df["auth_3ds_flag"] = nprng.random(len(df)) < 0.6

    labels = rows_to_df(label_rows, ops_store.LABEL_COLS)
    fraud_ids = set(labels.loc[labels["label"] == 1, "transaction_id"]) & set(df["id"])
    return df, fraud_ids


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
    """Đổi kế hoạch ngày thành lịch GỬI: [(send_at, seq, key, payload, meta)].

    ``send_at`` = event-time, cộng thêm độ trễ nếu message thuộc nhóm "về muộn".
    Duplicate được xếp thêm một lần gửi 0,5-5 giây sau bản đầu.

    ``meta = (event_time, kind)`` với ``kind`` ∈ {``ontime``, ``late``, ``dup``}.
    Meta đi kèm từng phần tử của lịch để report đo **cái đã gửi thật**, không
    phải đo lại tỉ lệ trong config — hai con số này khác nhau khi tiến trình bị
    dừng giữa ngày, và chỉ con số đo được mới là bằng chứng.
    """
    late_rate = scfg.get("late_rate", 0.05)
    late_min = scfg.get("late_delay_min_sec", 5)
    late_max = scfg.get("late_delay_max_sec", 60)
    dup_rate = scfg.get("duplicate_rate", 0.015)

    out = []
    for row in df.to_dict("records"):
        payload = to_payload(row)
        key = row["card_id"].encode()
        event_at = send_at = row["created_at"]
        kind = "ontime"
        if rng.random() < late_rate:                       # (2) late arrival
            send_at = send_at + timedelta(seconds=rng.uniform(late_min, late_max))
            kind = "late"
        out.append((send_at, next(counter), key, payload, (event_at, kind)))
        if rng.random() < dup_rate:                        # (3) duplicate
            out.append((send_at + timedelta(seconds=rng.uniform(0.5, 5.0)),
                        next(counter), key, payload, (event_at, "dup")))
    heapq.heapify(out)
    return out


# --------------------------------------------------------------------------- #
# Report chất lượng (= bằng chứng, đọc bằng `docker logs stream-generator`)     #
# --------------------------------------------------------------------------- #

DAYS_IN_REPORT = 14      # bảng sản lượng chỉ in đuôi (service chạy nhiều tháng)


def ooo_bands(watermark: float) -> list[tuple[float, str]]:
    """Các bậc phân loại out-of-orderness (giây), tăng dần.

    Bậc cuối là bậc DUY NHẤT thật sự mất dữ liệu: Flink khai
    ``WATERMARK FOR created_at AS created_at - INTERVAL '90' SECOND`` nên record
    có event-time thấp hơn ``max(event-time đã thấy) - watermark`` bị coi là late
    và bị bỏ khỏi window. Vì vậy report phải đo bậc này riêng, không gộp vào
    "đến muộn" chung — muộn mà còn kịp thì không mất gì.
    """
    return [(0.0, "đúng thứ tự"), (5.0, "<= 5s"), (30.0, "<= 30s"),
            (60.0, "<= 60s"), (watermark, f"<= {watermark:g}s (còn kịp)"),
            (float("inf"), f"> {watermark:g}s  ⚠ FLINK DROP")]


@dataclass
class StreamStats:
    """Bộ đếm chất lượng của luồng ĐÃ GỬI THẬT.

    Cố ý dùng bộ đếm O(1) bộ nhớ (histogram + max + sum) thay vì giữ list mẫu:
    service chạy ``restart: unless-stopped`` nên nó phải sống được nhiều tháng
    trong container mà không phình.
    """

    watermark_sec: float = 90.0
    speedup: float = 1.0                           # giá trị THỰC TẾ (CLI ghi đè cfg)
    t_start: float = field(default_factory=time.time)
    day: str = ""                                  # ngày đang phát
    planned: Counter = field(default_factory=Counter)     # theo ngày
    sent: Counter = field(default_factory=Counter)        # theo ngày
    dup: int = 0
    late: int = 0
    overdue: int = 0                               # phát khi đã quá hạn > 5s
    delivered: int = 0                             # Kafka ack (delivery callback)
    failed: int = 0
    first_error: str | None = None
    ooo_sum: float = 0.0
    ooo_max: float = 0.0
    ooo: Counter = field(default_factory=Counter)
    max_event: object | None = None                # event-time lớn nhất đã gửi
    burst_card_max: int = 0                        # max message/thẻ/5 phút
    burst_merch_max: int = 0                       # max message/merchant/10 phút
    fraud_planned: int = 0
    days: list[str] = field(default_factory=list)

    @property
    def total_sent(self) -> int:
        return sum(self.sent.values())

    @property
    def total_planned(self) -> int:
        return sum(self.planned.values())

    def begin_day(self, day: str, n_planned: int) -> None:
        self.day = day
        self.planned[day] += n_planned
        self.sent.setdefault(day, 0)
        if day not in self.days:
            self.days.append(day)

    def on_delivery(self, err, _msg) -> None:
        """Delivery callback của producer.

        Không có callback thì message bị broker từ chối sẽ mất **im lặng** —
        ``sent`` vẫn tăng mà topic không có gì. Report phải phân biệt "đã
        produce" với "Kafka đã ack".
        """
        if err is None:
            self.delivered += 1
            return
        self.failed += 1
        if self.first_error is None:
            self.first_error = str(err)

    def record(self, meta: tuple, overdue: bool) -> None:
        """Ghi nhận một message vừa produce."""
        event_at, kind = meta
        self.sent[self.day] += 1
        if kind == "dup":
            self.dup += 1
        elif kind == "late":
            self.late += 1
        if overdue:
            self.overdue += 1

        if self.max_event is None or event_at > self.max_event:
            self.max_event = event_at
        ooo = (self.max_event - event_at).total_seconds()
        self.ooo_sum += ooo
        self.ooo_max = max(self.ooo_max, ooo)
        for hi, label in ooo_bands(self.watermark_sec):
            if ooo <= hi:
                self.ooo[label] += 1
                break

    @property
    def dropped_by_watermark(self) -> int:
        return self.ooo[ooo_bands(self.watermark_sec)[-1][1]]


def plan_report(df, plan: list[tuple], fraud_ids: set, day: str,
                cfg: dict, stats: StreamStats) -> None:
    """In chất lượng KẾ HOẠCH của một ngày, ngay trước khi phát.

    Đây là chỗ duy nhất còn giữ DataFrame trọn ngày nên burst phải đo ở đây;
    vòng gửi chỉ giữ bộ đếm.
    """
    scfg = cfg.get("streaming") or {}
    kinds = Counter(k for *_, (_, k) in plan)
    want = daily_count(cfg)
    ts = df["created_at"]

    if df.empty:
        # Ngày rỗng là BẤT THƯỜNG (chân trời 45 ngày lẽ ra luôn có giao dịch rơi
        # vào ngày mục tiêu) -> phải báo, không được lặng lẽ đi tiếp.
        print(f"\n⚠ KẾ HOẠCH NGÀY {day}: 0 giao dịch (chờ ~{want:,}) — "
              f"kiểm tra reference data ở ops.* và transactions.patterns")
        return

    # Burst: đúng hai cửa sổ mà Flink/velocity dựa vào (merchant 10 phút, và
    # thẻ 5 phút = cửa sổ velocity). Nếu max ~1 thì feature real-time là hằng số.
    c5 = df.assign(b=ts.dt.floor("5min")).groupby(["card_id", "b"]).size()
    m10 = df.assign(b=ts.dt.floor("10min")).groupby(["merchant_id", "b"]).size()
    stats.burst_card_max = max(stats.burst_card_max, int(c5.max()))
    stats.burst_merch_max = max(stats.burst_merch_max, int(m10.max()))
    stats.fraud_planned += len(fraud_ids)

    print("\n" + "-" * 70)
    print(f"KẾ HOẠCH NGÀY {day} — {len(df):,} giao dịch, {len(plan):,} message")
    print("-" * 70)
    dev = abs(len(df) - want) / max(want, 1)
    flag = "  ⚠ lệch nhịp lịch sử > 20%" if dev > 0.2 else ""
    print(f"  nhịp        : {len(df):,} giao dịch/ngày (lịch sử ~{want:,}){flag}")
    print(f"  event-time  : {ts.min():%H:%M:%S} -> {ts.max():%H:%M:%S}  "
          f"(distinct card={df['card_id'].nunique():,} "
          f"merchant={df['merchant_id'].nunique():,} "
          f"device={df['device_id'].nunique():,})")
    print(f"  fraud (ground truth, KHÔNG gửi vào message): {len(fraud_ids):,} "
          f"({len(fraud_ids) / max(len(df), 1):.2%})")
    print(f"  tiêm lỗi    : late={kinds['late']:,} "
          f"({kinds['late'] / max(len(df), 1):.1%} vs cfg "
          f"{scfg.get('late_rate', 0.05):.1%})   "
          f"duplicate={kinds['dup']:,} "
          f"({kinds['dup'] / max(len(df), 1):.1%} vs cfg "
          f"{scfg.get('duplicate_rate', 0.015):.1%})")
    hot_card = c5.idxmax()
    print(f"  burst thật  : thẻ {hot_card[0]} có {c5.max():,} giao dịch trong "
          f"5 phút lúc {hot_card[1]:%H:%M}  |  merchant/10 phút max={m10.max():,} "
          f"(p99={m10.quantile(0.99):.0f})")


def stream_report(stats: StreamStats, cfg: dict, topic: str,
                  plan_left: int, reason: str) -> None:
    """In report cuối (hoặc tổng kết luỹ kế khi sang ngày mới)."""
    scfg = cfg.get("streaming") or {}
    elapsed = max(time.time() - stats.t_start, 1e-9)
    sent = stats.total_sent
    span = f"{stats.days[0]} -> {stats.days[-1]}" if stats.days else "(chưa có)"
    ran = (f"{elapsed:.0f}s" if elapsed < 600
           else f"{elapsed / 60:.0f} phút" if elapsed < 7200
           else f"{elapsed / 3600:.1f}h")

    print("\n" + "=" * 70)
    print(f"STREAMING — QUALITY REPORT (topic '{topic}')  [{reason}]")
    print("=" * 70)
    print(f"  chạy {ran}  |  ngày đã phát: {span} "
          f"({len(stats.days)} ngày)  |  speedup=x{stats.speedup:g}")
    print(f"  message trong lịch : {stats.total_planned:>10,}")
    print(f"  đã produce         : {sent:>10,}  "
          f"({sent / max(stats.total_planned, 1):.1%} của lịch; "
          f"{plan_left:,} chưa tới hạn)")
    print(f"  Kafka ack          : {stats.delivered:>10,}")
    print(f"  Kafka lỗi          : {stats.failed:>10,}"
          + (f"  ⚠ {stats.first_error}" if stats.failed else ""))
    if sent and stats.delivered + stats.failed < sent:
        print(f"    ⚠ {sent - stats.delivered - stats.failed:,} message chưa có "
              f"delivery callback (còn trong queue lúc in report)")
    print(f"  throughput         : {sent / elapsed:.3f} msg/s "
          f"(bù quá khứ: {stats.overdue:,} message phát khi đã quá hạn)")

    print("\n  --- SẢN LƯỢNG THEO NGÀY (phải khớp nhịp lịch sử "
          f"~{daily_count(cfg):,}/ngày) ---")
    # Service sống nhiều tháng -> chỉ in đuôi, nhưng PHẢI nói đã bỏ bao nhiêu:
    # bảng bị cắt lặng lẽ đọc y như "chỉ chạy có 14 ngày".
    shown = stats.days[-DAYS_IN_REPORT:]
    if len(stats.days) > len(shown):
        print(f"    (bỏ {len(stats.days) - len(shown)} ngày trước đó; "
              f"tổng luỹ kế vẫn tính đủ)")
    for d in shown:
        print(f"    {d} : sent={stats.sent[d]:>7,} / lịch {stats.planned[d]:>7,}")

    print("\n  --- (1) DUPLICATE (at-least-once của Kafka) ---")
    print(f"    gửi lại trùng id: {stats.dup:,}  rate={stats.dup / max(sent, 1):.2%} "
          f"(cfg {scfg.get('duplicate_rate', 0.015):.2%}) "
          f"-> Flink dedup + Spark dropDuplicates phải xử lý")

    print("\n  --- (2) LATE ARRIVAL / OUT-OF-ORDERNESS ---")
    print(f"    message bị đẩy trễ lúc gửi: {stats.late:,} "
          f"rate={stats.late / max(sent, 1):.2%} "
          f"(cfg {scfg.get('late_rate', 0.05):.2%}, "
          f"trễ {scfg.get('late_delay_min_sec', 5)}-"
          f"{scfg.get('late_delay_max_sec', 60)}s)")
    print(f"    lệch thứ tự so với event-time lớn nhất đã gửi: "
          f"max={stats.ooo_max:.1f}s  mean={stats.ooo_sum / max(sent, 1):.2f}s")
    for _hi, label in ooo_bands(stats.watermark_sec):
        n = stats.ooo[label]
        print(f"      {label:<24}: {n:>8,}  ({n / max(sent, 1):6.2%})")
    if stats.dropped_by_watermark:
        print(f"    ⚠ {stats.dropped_by_watermark:,} message vượt watermark "
              f"{stats.watermark_sec:g}s -> Flink BỎ khỏi window "
              f"(hạ streaming.late_delay_max_sec hoặc nâng watermark)")

    print("\n  --- (3) BURST (không tiêm; đến từ archetype card_testing) ---")
    print(f"    max message/thẻ/5 phút     : {stats.burst_card_max:,}"
          + ("   ⚠ ~1 => velocity 5 phút vô nghĩa"
             if stats.burst_card_max <= 1 else ""))
    print(f"    max message/merchant/10 phút: {stats.burst_merch_max:,}")

    print("\n  --- FRAUD TRONG KẾ HOẠCH (ground truth, chỉ để đối chiếu) ---")
    print(f"    {stats.fraud_planned:,} giao dịch fraud "
          f"({stats.fraud_planned / max(stats.total_planned, 1):.2%} của lịch; "
          f"cfg fraud_rate={cfg['transactions']['fraud_rate']:.2%})")
    print("=" * 70)


# --------------------------------------------------------------------------- #
# Vòng chạy                                                                    #
# --------------------------------------------------------------------------- #

def install_stop(flag: dict) -> None:
    """SIGINT (Ctrl-C) và SIGTERM (``docker stop``) dẫn về CÙNG một đường ra.

    Không có handler SIGTERM thì container bị ``docker stop`` sẽ chết thẳng ở
    trong ``time.sleep`` — producer không flush và report KHÔNG BAO GIỜ được in,
    đúng lúc người ta cần nó nhất.
    """
    def _stop(signum, _frame):
        flag["stop"] = signal.Signals(signum).name
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)


def start_day(cfg: dict, entities: tuple, day: str, scfg: dict,
              rng: random.Random, counter, stats: StreamStats) -> list[tuple]:
    """Lập kế hoạch một ngày + in report kế hoạch + mở bộ đếm cho ngày đó."""
    print(f"Lập kế hoạch ngày {day} (~{daily_count(cfg):,} giao dịch) ...")
    df, fraud_ids = plan_day(cfg, entities, day,
                             cfg["seed"] + int(day.replace("-", "")))
    plan = build_schedule(df, scfg, rng, counter)
    plan_report(df, plan, fraud_ids, day, cfg, stats)
    stats.begin_day(day, len(plan))
    return plan


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

    rcfg = scfg.get("report") or {}
    stats = StreamStats(watermark_sec=float(rcfg.get("watermark_sec", 90)),
                        speedup=speedup)
    every = float(rcfg.get("progress_every_sec", 10) or 10)
    stop = {"stop": None}
    install_stop(stop)

    plan = start_day(cfg, entities, day, scfg, rng, counter, stats)
    print(f"\nBắn vào {bootstrap} ({ops_store.kafka_describe()}) topic='{topic}' | "
          f"speedup=x{speedup:g} | watermark Flink={stats.watermark_sec:g}s")

    t_report = time.time()
    try:
        while not stop["stop"]:
            if not plan:
                if args.drain or args.once:
                    break
                # sang ngày mới: tổng kết ngày vừa xong, reference data đổi một ít
                # (để SCD2 có việc), rồi lập kế hoạch ngày kế tiếp
                nxt = (datetime.fromisoformat(day) + timedelta(days=1)).strftime("%Y-%m-%d")
                producer.flush(10)          # ack hết ngày cũ trước khi tổng kết
                stream_report(stats, cfg, topic, 0, f"hết ngày {day}")
                churn = ops_store.apply_dim_churn(cfg, rng)
                print(f"\n[{day} xong] dim churn: "
                      + "  ".join(f"{k}={v}" for k, v in churn.items()))
                frames = ops_store.load_dims(cfg)
                entities = ops_store.reconstruct_entities(frames, rng)
                day = nxt
                plan = start_day(cfg, entities, day, scfg, rng, counter, stats)
                continue

            send_at, _seq, key, payload, meta = plan[0]
            wait = (send_at - sim_now()).total_seconds() / speedup
            if wait > 0:
                # Ngủ tối đa 1 giây một nhịp để tín hiệu dừng phản hồi nhanh và
                # producer được poll thường xuyên (delivery callback + queue full).
                time.sleep(min(wait, 1.0))
                producer.poll(0)
                continue

            heapq.heappop(plan)
            producer.produce(topic, key=key, value=payload,
                             on_delivery=stats.on_delivery)
            producer.poll(0)
            # wait < -5: đã quá hạn > 5s -> đang bù phần quá khứ của ngày
            stats.record(meta, overdue=wait < -5)

            if time.time() - t_report >= every:
                t_report = time.time()
                print(f"  [{sim_now():%H:%M:%S}] sent={stats.total_sent:,} "
                      f"(dup={stats.dup:,} late={stats.late:,}) "
                      f"ack={stats.delivered:,} fail={stats.failed:,} | "
                      f"drop>{stats.watermark_sec:g}s={stats.dropped_by_watermark:,} | "
                      f"còn lại={len(plan):,} (bù={stats.overdue:,})")
    except KeyboardInterrupt:                # Ctrl-C trước khi handler kịp cài
        stop["stop"] = "SIGINT"
    finally:
        # Flush TRƯỚC khi in report: delivery callback còn trong queue mới được
        # tính vào ack/fail, nếu không report báo thiếu ack một cách vô cớ.
        left = producer.flush(10)
        producer.poll(0)
        reason = (f"dừng bởi {stop['stop']}" if stop["stop"]
                  else "drain xong" if args.drain else "hết ngày")
        if left:
            print(f"\n⚠ còn {left:,} message trong queue của producer khi flush "
                  f"hết thời gian -> KHÔNG chắc đã vào Kafka")
        stream_report(stats, cfg, topic, len(plan), reason)

    # Chỉ lỗi Kafka mới là thất bại (mất dữ liệu ở sink) -> exit != 0 để
    # `restart: unless-stopped` chạy lại. Message vượt watermark là cảnh báo về
    # THAM SỐ sinh dữ liệu, chạy lại không sửa được gì nên không đổi exit code.
    return 1 if stats.failed else 0


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
