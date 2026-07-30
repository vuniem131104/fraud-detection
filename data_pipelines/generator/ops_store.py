"""Đọc/ghi các bảng của TEAM DATA trong ``opsdb`` (schema ``ops``).

Một chỗ duy nhất chạm vào cơ sở dữ liệu vận hành, cho cả hai generator:

* ``generate_offline.py`` — nạp lịch sử: reference data + transactions + labels
* ``generate_stream.py``  — luồng live: đọc reference data để dựng lại entity,
  và mỗi khi sang ngày mới thì sửa một ít thuộc tính (dim churn) để SCD2 ở Gold
  có thay đổi mà bắt

Vì sao reference data nằm ở Postgres chứ không phải ghi thẳng parquet lên MinIO:
**một system of record duy nhất**. Team data sở hữu ``ops.*``; mọi file trên
MinIO đều là bản export từ đó (job ``ops_to_source``). Nếu generator ghi thẳng
MinIO thì có hai nguồn sự thật và sớm muộn chúng phân kỳ.
"""

from __future__ import annotations

import os
import random
import sys
from pathlib import Path

import pandas as pd
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "initial"))
import generate_fake_data as gen  # noqa: E402  (thêm path xong mới import được)

# Thứ tự cột COPY-ready — khớp DDL sql/ops/02_dimensions.sql
USER_COLS = ["id", "email", "country_code", "customer_segment",
             "kyc_level", "email_verified", "created_at"]
CARD_COLS = ["id", "user_id", "issuer_code", "country_code", "brand",
             "type", "bin_code", "is_virtual", "created_at"]
MERCHANT_COLS = ["id", "name", "category", "country_code", "risk_level", "created_at"]
DEVICE_COLS = ["id", "fingerprint", "device_type", "os", "browser",
               "screen_resolution", "created_at"]

DIM_COLS = {"users": USER_COLS, "cards": CARD_COLS,
            "merchants": MERCHANT_COLS, "devices": DEVICE_COLS}

# transactions: 14 cột (bỏ 'status' của generator gốc, thêm auth_3ds_flag)
TX_COLS = ["id", "user_id", "card_id", "merchant_id", "device_id", "amount_usd",
           "currency", "channel", "billing_country_code", "ip_country_code",
           "email_purchaser", "email_recipient", "created_at", "auth_3ds_flag"]
LABEL_COLS = ["transaction_id", "label", "label_source", "created_at"]


# --------------------------------------------------------------------------- #
# Kết nối                                                                      #
# --------------------------------------------------------------------------- #

def pg_dsn(cfg: dict, db_key: str) -> str:
    """DSN Postgres cho ``ops_db`` hoặc ``warehouse_db`` (env ghi đè host được).

    Cùng một config chạy được cả từ host (``localhost``) và trong container
    (``PG_HOST=postgres``) mà không phải sửa file.
    """
    p = cfg["postgres"]
    host = os.environ.get("PG_HOST", p["host"])
    return (f"host={host} port={p.get('port', 5432)} dbname={p[db_key]} "
            f"user={os.environ['AIRFLOW_USER']} password={os.environ['AIRFLOW_PASSWORD']}")


def connect(cfg: dict, db_key: str = "ops_db"):
    """Kết nối autocommit tới opsdb (mặc định) hoặc warehouse."""
    return psycopg.connect(pg_dsn(cfg, db_key), autocommit=True)


# --------------------------------------------------------------------------- #
# Ghi (nạp lịch sử)                                                            #
# --------------------------------------------------------------------------- #

def _copy(conn, table: str, cols: list[str], rows) -> int:
    """TRUNCATE rồi COPY — nạp lại từ đầu, chạy nhiều lần không nhân đôi."""
    n = 0
    conn.execute(f"TRUNCATE {table}")
    with conn.cursor() as cur, cur.copy(
            f"COPY {table} ({','.join(cols)}) FROM STDIN") as cp:
        for row in rows:
            cp.write_row(row)
            n += 1
    return n


def write_dims(cfg: dict, frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Nạp 4 bảng reference data vào ``ops.*``."""
    out = {}
    with connect(cfg) as conn:
        # cards tham chiếu users -> nạp users trước (không có FK nhưng giữ thứ tự
        # cho đúng nghĩa, và để lỗi nếu có thì lộ ra ở bảng nhỏ trước)
        for name in ("users", "cards", "merchants", "devices"):
            df = frames[name][DIM_COLS[name]]
            out[name] = _copy(conn, f"ops.{name}", DIM_COLS[name],
                              df.itertuples(index=False, name=None))
    return out


def write_transactions(cfg: dict, df: pd.DataFrame) -> int:
    """Nạp transactions vào ``ops.transactions`` (bảng landing của team data).

    Lịch sử đi đúng con đường của luồng live: giao dịch vào bảng vận hành trước,
    rồi mới được dump ra file theo ngày. Nhờ vậy batch và streaming dùng CÙNG
    một system of record.
    """
    out = df[TX_COLS].copy()
    # psycopg không adapt được pd.NA (nullable boolean) -> đổi sang None
    out["auth_3ds_flag"] = [None if pd.isna(v) else bool(v)
                            for v in out["auth_3ds_flag"]]
    with connect(cfg) as conn:
        return _copy(conn, "ops.transactions", TX_COLS,
                     out.itertuples(index=False, name=None))


def write_labels(cfg: dict, labels: pd.DataFrame) -> int:
    """Nạp ground truth vào ``warehouse.application.labels``.

    Label không phải feature và không đi qua medallion — nó là nhãn để train nên
    nạp thẳng vào warehouse của team ML.
    """
    with connect(cfg, "warehouse_db") as conn:
        return _copy(conn, "application.labels", LABEL_COLS,
                     labels[LABEL_COLS].itertuples(index=False, name=None))


# --------------------------------------------------------------------------- #
# Đọc (luồng live)                                                             #
# --------------------------------------------------------------------------- #

def load_dims(cfg: dict) -> dict[str, pd.DataFrame]:
    """Đọc 4 bảng reference data từ ``ops.*`` về DataFrame."""
    frames = {}
    with connect(cfg) as conn:
        for name, cols in DIM_COLS.items():
            frames[name] = pd.read_sql(
                f"SELECT {','.join(cols)} FROM ops.{name}", conn)
    return frames


def reconstruct_entities(frames: dict[str, pd.DataFrame], rng: random.Random):
    """Dựng lại object entity mà ``gen.generate_transactions`` cần.

    Hai trường không lưu trong bảng được suy lại: ``currency`` (từ country) và
    ``spend_mu`` (từ segment). Chúng chỉ ảnh hưởng biên độ số tiền của giao dịch
    hợp lệ, không ảnh hưởng cấu trúc fraud -> chấp nhận cho MVP.

    Trả ``(users, cards, per_user_cards, merchants, device_ids, device_types)``.
    """
    ccy = {c: cur for c, cur, _ in gen.HOME_COUNTRIES}
    seg_bonus = {"normal": 0.0, "premium": 0.5, "vip": 1.1}

    users_df, cards_df = frames["users"], frames["cards"]
    # chỉ giữ user sở hữu >= 1 thẻ (tránh per_user_cards rỗng -> rng.choice lỗi)
    users_df = users_df[users_df["id"].isin(set(cards_df["user_id"]))].reset_index(drop=True)

    users, uidx = [], {}
    for i, r in enumerate(users_df.itertuples(index=False)):
        uidx[r.id] = i
        users.append(gen.User(
            r.id, r.email, r.country_code, ccy.get(r.country_code, "USD"),
            r.customer_segment, r.created_at.to_pydatetime(),
            rng.uniform(2.9, 3.9) + seg_bonus.get(r.customer_segment, 0.0)))

    cards, per_user = [], [[] for _ in users]
    for r in cards_df.itertuples(index=False):
        if r.user_id not in uidx:
            continue
        ui = uidx[r.user_id]
        cards.append(gen.Card(r.id, ui, r.created_at.to_pydatetime()))
        per_user[ui].append(len(cards) - 1)

    merchants = [gen.Merchant(r.id, r.country_code, int(r.risk_level))
                 for r in frames["merchants"].itertuples(index=False)]
    devices_df = frames["devices"]
    return (users, cards, per_user, merchants,
            list(devices_df["id"]), list(devices_df["device_type"]))


# --------------------------------------------------------------------------- #
# Dim churn (để SCD Type 2 có thay đổi mà bắt)                                 #
# --------------------------------------------------------------------------- #

# Thuộc tính đổi được + cách đổi. Chỉ chọn những thứ ĐỜI THẬT có đổi:
#   country_code   : khách chuyển chỗ ở / du học
#   kyc_level      : nâng cấp định danh
#   email_verified : xác thực email muộn
#   is_virtual     : phát hành lại thẻ dưới dạng virtual
#   risk_level     : team risk đánh giá lại merchant
_CHURN_SQL = {
    "users": [
        "UPDATE ops.users SET country_code = %s, updated_at = NOW() WHERE id = %s",
        "UPDATE ops.users SET kyc_level = LEAST(kyc_level + 1, 3), updated_at = NOW() WHERE id = %s",
        "UPDATE ops.users SET email_verified = NOT email_verified, updated_at = NOW() WHERE id = %s",
    ],
    "cards": [
        "UPDATE ops.cards SET is_virtual = NOT is_virtual, updated_at = NOW() WHERE id = %s",
    ],
    "merchants": [
        "UPDATE ops.merchants SET risk_level = %s, updated_at = NOW() WHERE id = %s",
    ],
}


def apply_dim_churn(cfg: dict, rng: random.Random) -> dict[str, int]:
    """Sửa một ít thuộc tính reference data — gọi mỗi khi sang ngày mới.

    Đây là thứ làm SCD Type 2 sống động: hôm sau DP0 export snapshot mới, DP2 so
    với bản ``is_current`` ở Gold, thấy hash thuộc tính đổi -> đóng version cũ
    (``valid_to_ts``, ``is_current=false``) và mở version mới. Không có bước này
    thì mỗi thực thể chỉ có đúng một version.
    """
    ccfg = cfg.get("dim_churn") or {}
    countries = [c for c, _, _ in gen.HOME_COUNTRIES]
    done = {"users": 0, "cards": 0, "merchants": 0}

    with connect(cfg) as conn:
        for table, key in (("users", "users_per_day"), ("cards", "cards_per_day"),
                           ("merchants", "merchants_per_day")):
            n = int(ccfg.get(key) or 0)
            if n <= 0:
                continue
            ids = [r[0] for r in conn.execute(
                f"SELECT id FROM ops.{table} ORDER BY random() LIMIT %s", (n,)).fetchall()]
            for entity_id in ids:
                sql = rng.choice(_CHURN_SQL[table])
                if table == "users" and "country_code" in sql:
                    conn.execute(sql, (rng.choice(countries), entity_id))
                elif table == "merchants":
                    conn.execute(sql, (rng.randint(1, 5), entity_id))
                else:
                    conn.execute(sql, (entity_id,))
                done[table] += 1
    return done
