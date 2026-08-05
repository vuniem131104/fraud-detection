"""Nạp DỮ LIỆU LỊCH SỬ vào cơ sở dữ liệu vận hành của team data.

Bước 1 của luồng. Sinh 1 năm giao dịch (27/07/2025 -> 29/07/2026) cùng reference
data và ground truth, rồi ghi vào **Postgres**:

    ops.users / ops.cards / ops.merchants / ops.devices    (reference data)
    ops.transactions                                      (fact, đã tiêm 4 lỗi)
    application.labels          (warehouse của team ML)   (ground truth)

**Không** ghi file lên MinIO. Đó là việc của ``include.ops_to_source`` — job
export của team data. Giữ đúng một system of record (``ops.*``) cho cả lịch sử
lẫn luồng live nên batch và streaming không thể phân kỳ.

Bốn lỗi data cố tình tiêm (chỉ trên transactions):
  1. duplicate  -- nhân bản y hệt (cùng id) ~1% dòng; sống được vì
                   ops.transactions cố ý KHÔNG có primary key
  2. skew       -- ~80% giao dịch ở US -> key lệch điển hình cho Spark
  3. schema evo -- cột auth_3ds_flag chỉ có từ cutover_date; job export sẽ BỎ HẲN
                   cột này ở partition trước mốc đó
  4. high card. -- device_id/card_id vốn cardinality cao (chỉ đo, không tiêm)

Chạy::

    uv run python data_pipelines/generator/generate_offline.py
    uv run python data_pipelines/generator/generate_offline.py --smoke
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

import ops_store
from ops_store import REPO_ROOT, gen

# generator gốc trả tuple 14 phần tử; 'status' bị bỏ ở MVP (không dùng làm feature)
RAW_TX_COLS = ["id", "user_id", "card_id", "merchant_id", "device_id", "amount_usd",
               "currency", "channel", "billing_country_code", "ip_country_code",
               "email_purchaser", "email_recipient", "status", "created_at"]


# --------------------------------------------------------------------------- #
# Config                                                                       #
# --------------------------------------------------------------------------- #

def load_config(path: Path, smoke: bool = False) -> dict:
    """Đọc YAML config; ``smoke`` thu nhỏ quy mô để test nhanh."""
    cfg = yaml.safe_load(path.read_text())
    if smoke:
        cfg["entities"] = {"users": 800, "merchants": 120, "devices": 700}
        cfg["transactions"]["count"] = 4000
    return cfg


def window_days(cfg: dict) -> int:
    """Số ngày của cửa sổ lịch sử [start_date, end_date)."""
    t = cfg["transactions"]
    return (datetime.fromisoformat(t["end_date"])
            - datetime.fromisoformat(t["start_date"])).days


# --------------------------------------------------------------------------- #
# Hai chỗ ghi đè hành vi của generator gốc                                     #
# --------------------------------------------------------------------------- #

def apply_geo_skew(us_share: float) -> None:
    """Dồn ~``us_share`` user về US -> ~cùng tỉ lệ giao dịch ở US.

    Skew tiêm ở TRỌNG SỐ QUỐC GIA, trước khi sinh, chứ không ghi đè cột sau khi
    sinh: billing/ip_country của giao dịch được suy từ country của user, nên cách
    này tạo skew chảy đúng qua logic fraud thay vì dán nhãn giả lên dữ liệu.
    """
    if not us_share or us_share <= 0:
        return
    others = [(c, cur, w) for (c, cur, w) in gen.HOME_COUNTRIES if c != "US"]
    others_sum = sum(w for _, _, w in others)
    new = [("US", "USD", us_share * 100.0)]
    for c, cur, w in others:                        # 20% còn lại chia theo tỉ lệ cũ
        new.append((c, cur, (1 - us_share) * 100.0 * w / others_sum))
    gen.HOME_COUNTRIES = new


def uniform_created_at(now: datetime, floor: datetime, rng, days: int) -> datetime:
    """Timestamp PHÂN BỐ ĐỀU trong [max(floor, now-days), now] + giờ diurnal.

    Thay ``gen._draw_created_at`` gốc (dồn 50% giao dịch vào 30 ngày cuối). Với
    cửa sổ 1 năm, recency-weighting làm số giao dịch/ngày dốc đứng ở đuôi: ngày
    cuối gấp ~20 lần ngày đầu. Phân bố đều cho ~count/số_ngày mỗi ngày.
    """
    start = max(floor + timedelta(minutes=1), now - timedelta(days=days))
    if start >= now:
        return now - timedelta(seconds=rng.uniform(1, 60))
    total = (now - start).total_seconds()
    created = (start + timedelta(seconds=rng.uniform(0, total))).replace(
        hour=min(gen._diurnal_hour(rng), 23), minute=rng.randrange(60),
        second=rng.randrange(60), microsecond=rng.randrange(1_000_000))
    if not (start <= created <= now):               # giờ diurnal đẩy lệch -> vẽ lại
        created = start + timedelta(seconds=rng.uniform(0, total))
    return created


def patch_generator(cfg: dict) -> None:
    """Áp hai chỉnh sửa trên vào module generator gốc."""
    apply_geo_skew(cfg["dirty"]["skew"]["us_share"])
    gen._draw_created_at = uniform_created_at


# --------------------------------------------------------------------------- #
# Frames                                                                       #
# --------------------------------------------------------------------------- #

def rows_to_df(rows: list[tuple], cols: list[str]) -> pd.DataFrame:
    """List-of-tuples (COPY-ready) -> DataFrame theo đúng thứ tự cột."""
    return pd.DataFrame(rows, columns=cols)


def build_transactions_df(tx_rows: list[tuple]) -> pd.DataFrame:
    """DataFrame transactions: bỏ 'status', thêm ``event_date`` (giờ HCM).

    ``event_date`` phải theo giờ ĐỊA PHƯƠNG để khớp partition mà job export tạo
    ra ở luồng live — nếu một bên dùng UTC thì giao dịch lúc 06:00 HCM sẽ rơi vào
    partition ngày hôm trước.
    """
    df = rows_to_df(tx_rows, RAW_TX_COLS).drop(columns=["status"])
    df["amount_usd"] = df["amount_usd"].apply(
        lambda d: float(d) if isinstance(d, Decimal) else float(d))
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["event_date"] = (df["created_at"].dt.tz_convert(gen.HCM_TZ)
                        .dt.strftime("%Y-%m-%d"))
    return df


def inject_duplicates(df: pd.DataFrame, rate: float,
                      rng: np.random.Generator) -> pd.DataFrame:
    """Nhân bản y hệt (trùng cả ``id``) ``rate`` fraction số dòng."""
    if rate <= 0:
        return df
    n_dup = int(len(df) * rate)
    dup_idx = rng.integers(0, len(df), size=n_dup)
    return pd.concat([df, df.iloc[dup_idx].copy()], ignore_index=True)


def inject_schema_evolution(df: pd.DataFrame, dcfg: dict,
                            rng: np.random.Generator) -> pd.DataFrame:
    """Cột mới chỉ có giá trị từ ``cutover_date`` trở đi, trước đó là NULL.

    Job export sẽ BỎ HẲN cột ở partition trước cutover -> hai schema khác nhau
    cùng tồn tại trên đĩa, buộc DP2 phải dùng ``mergeSchema``.
    """
    col = dcfg["schema_evolution"]["new_column"]
    cutover = dcfg["schema_evolution"]["cutover_date"]
    df[col] = pd.Series(rng.random(len(df)) < 0.6, index=df.index).astype("boolean")
    df.loc[df["event_date"] < cutover, col] = pd.NA
    return df


# --------------------------------------------------------------------------- #
# Report (= bằng chứng cho docs)                                               #
# --------------------------------------------------------------------------- #

def quality_report(tx: pd.DataFrame, labels: pd.DataFrame, cfg: dict) -> None:
    """In báo cáo đo đúng 4 lỗi đã tiêm + tín hiệu fraud tập trung."""
    dcfg = cfg["dirty"]
    new_col = dcfg["schema_evolution"]["new_column"]
    cutover = dcfg["schema_evolution"]["cutover_date"]
    days = tx["event_date"].nunique()

    print("\n" + "=" * 70)
    print("HISTORICAL LOAD — QUALITY REPORT (ops.transactions)")
    print("=" * 70)
    print(f"  transactions (kể cả duplicate): {len(tx):>10,}")
    print(f"  labels                        : {len(labels):>10,}")
    if len(labels):
        print(f"  labeled fraud rate            : {(labels['label'] == 1).mean():.3%}")
    per_day = tx.groupby("event_date").size()
    print(f"  số ngày={days}  giao dịch/ngày: mean={per_day.mean():.0f} "
          f"median={per_day.median():.0f} min={per_day.min():,} max={per_day.max():,}")

    print("\n  --- (1) DUPLICATE ---")
    n_uni = tx["id"].nunique()
    print(f"    tổng={len(tx):,}  unique id={n_uni:,}  "
          f"duplicate={len(tx) - n_uni:,}  rate={1 - n_uni / len(tx):.2%}")

    print("\n  --- (2) SKEW (billing_country_code) ---")
    vc = tx["billing_country_code"].value_counts(normalize=True)
    print(f"    US chiếm {vc.get('US', 0):.1%}")
    print("    top-5: " + ", ".join(f"{c}={p:.1%}" for c, p in vc.head(5).items()))

    print(f"\n  --- (3) SCHEMA EVOLUTION (cột '{new_col}', cutover {cutover}) ---")
    old, new = tx[tx["event_date"] < cutover], tx[tx["event_date"] >= cutover]
    print(f"    trước cutover: {len(old):,} dòng, null {old[new_col].isna().mean():.0%} "
          f"(job export sẽ BỎ HẲN cột)")
    print(f"    sau  cutover: {len(new):,} dòng, null {new[new_col].isna().mean():.0%}")

    print("\n  --- (4) HIGH CARDINALITY (chỉ đo) ---")
    for col in ("device_id", "card_id", "user_id", "merchant_id"):
        print(f"    {col:<12}: {tx[col].nunique():>9,} distinct")

    # Tín hiệu mà feature real-time dựa vào: hoạt động phải TẬP TRUNG, không rải.
    # Nếu hai số này ~1 thì merch_*_10min / device_*_1h sẽ là hằng số vô dụng.
    print("\n  --- TẬP TRUNG HOẠT ĐỘNG (điều kiện để feature real-time có tín hiệu) ---")
    ts = tx["created_at"]
    bucket10 = ts.dt.floor("10min")
    m10 = tx.assign(b=bucket10).groupby(["merchant_id", "b"]).size()
    d60 = (tx.assign(b=ts.dt.floor("1h"))
             .groupby(["device_id", "b"])["user_id"].nunique())
    print(f"    merchant/10 phút : max={m10.max():,}  p99={m10.quantile(0.99):.0f}  "
          f"mean={m10.mean():.2f}")
    print(f"    device 1h distinct users: max={d60.max():,}  "
          f"p99={d60.quantile(0.99):.0f}  mean={d60.mean():.2f}")
    print("=" * 70)


# --------------------------------------------------------------------------- #
# Orchestration                                                               #
# --------------------------------------------------------------------------- #

def run(cfg: dict) -> int:
    """Sinh entity + transactions + labels, tiêm lỗi, nạp Postgres, in report."""
    seed = cfg["seed"]
    rng, nprng = random.Random(seed), np.random.default_rng(seed)
    tcfg, ecfg, dcfg = cfg["transactions"], cfg["entities"], cfg["dirty"]

    days = window_days(cfg)
    start = tcfg["start_date"]
    now = datetime.fromisoformat(tcfg["end_date"]).replace(tzinfo=gen.HCM_TZ)
    # Sinh entity ở mốc start_date: mọi user/card/device tồn tại TRƯỚC cửa sổ nên
    # phân bố đều mới cho ~count/days giao dịch mỗi ngày. Nếu sinh entity ở mốc
    # end_date thì nửa số thẻ "chưa ra đời" trong nửa đầu năm.
    entity_now = datetime.fromisoformat(start).replace(tzinfo=gen.HCM_TZ)

    patch_generator(cfg)

    print(f"Sinh reference data (mốc {entity_now.date()}, cửa sổ {days} ngày, "
          f"US≈{dcfg['skew']['us_share']:.0%}) ...")
    users, user_rows = gen.generate_users(ecfg["users"], rng, entity_now)
    device_rows = gen.generate_devices(ecfg["devices"], rng, entity_now)
    merchants, merchant_rows = gen.generate_merchants(ecfg["merchants"], rng, entity_now)
    cards, per_user_cards, card_rows = gen.generate_cards(users, rng, entity_now)
    print(f"  users={len(user_rows):,} cards={len(card_rows):,} "
          f"merchants={len(merchant_rows):,} devices={len(device_rows):,}")

    print(f"Sinh ~{tcfg['count']:,} transactions ({tcfg['difficulty']}) ...")
    # label_cutoff_days=0 -> label 1:1 tức thời (chỉ mất do label noise cố ý).
    # Label delay thật (chargeback 30-120 ngày) là hạng mục CHƯA làm, xem docs.
    tx_rows, label_rows, stats = gen.generate_transactions(
        tcfg["count"], users, cards, per_user_cards, merchants,
        [r[0] for r in device_rows], [r[2] for r in device_rows],
        rng, nprng, now, days, tcfg["fraud_rate"], label_cutoff_days=0,
        patterns=tcfg["patterns"], difficulty=tcfg["difficulty"])
    print(f"  transactions={len(tx_rows):,} labels={len(label_rows):,} "
          f"true_fraud={stats['true_fraud']:,}")

    tx = build_transactions_df(tx_rows)
    labels = rows_to_df(label_rows, ops_store.LABEL_COLS)

    # Clamp về đúng cửa sổ. Archetype account_takeover vẽ "clean history" từ ngày
    # phát hành thẻ (tới ~720 ngày trước) nên rò rỉ vài giao dịch sớm hơn start_date.
    n_before = int((tx["event_date"] < start).sum())
    if n_before:
        tx = tx[tx["event_date"] >= start].reset_index(drop=True)
        labels = labels[labels["transaction_id"].isin(set(tx["id"]))].reset_index(drop=True)
        print(f"  clamp cửa sổ: bỏ {n_before:,} giao dịch trước {start} "
              f"(clean-history của ATO) -> còn {len(tx):,}")

    print("Tiêm lỗi data ...")
    tx = inject_schema_evolution(tx, dcfg, nprng)          # (3)
    tx = inject_duplicates(tx, dcfg["duplicate_rate"], nprng)   # (1)

    print("Nạp Postgres ...")
    counts = ops_store.write_dims(cfg, {
        "users": rows_to_df(user_rows, ops_store.USER_COLS),
        "cards": rows_to_df(card_rows, ops_store.CARD_COLS),
        "merchants": rows_to_df(merchant_rows, ops_store.MERCHANT_COLS),
        "devices": rows_to_df(device_rows, ops_store.DEVICE_COLS)})
    print("  ops: " + "  ".join(f"{k}={v:,}" for k, v in counts.items()))
    print(f"  ops.transactions={ops_store.write_transactions(cfg, tx):,}")
    print(f"  application.labels={ops_store.write_labels(cfg, labels):,}")

    quality_report(tx, labels, cfg)
    print("\nBước tiếp: python -m include.ops_to_source "
          f"--from {start} --to {tx['event_date'].max()}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI: --config, --smoke."""
    p = argparse.ArgumentParser(description="Nạp dữ liệu lịch sử vào ops.* + labels.")
    p.add_argument("--config", type=Path,
                   default=Path(__file__).with_name("generator_config.yaml"))
    p.add_argument("--smoke", action="store_true", help="Quy mô nhỏ để test nhanh.")
    return p


def main() -> int:
    """Load .env (cred Postgres) rồi chạy."""
    gen.load_dotenv(REPO_ROOT / "data_pipelines" / ".env")
    args = build_parser().parse_args()
    return run(load_config(args.config, args.smoke))


if __name__ == "__main__":
    raise SystemExit(main())
