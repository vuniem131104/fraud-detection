"""Đọc/ghi các bảng của TEAM DATA trong ``opsdb`` (schema ``ops``).

Một chỗ duy nhất chạm vào cơ sở dữ liệu vận hành, cho cả hai generator:

* ``generate_offline.py`` — nạp lịch sử: reference data + transactions + labels
* ``generate_stream.py``  — luồng live: đọc reference data để dựng lại entity,
  và mỗi khi sang ngày mới thì sửa một ít thuộc tính (dim churn) để SCD2 ở Gold
  có thay đổi mà bắt

Vì sao reference data nằm ở Postgres chứ không phải ghi thẳng parquet lên GCS:
**một system of record duy nhất**. Team data sở hữu ``ops.*``; mọi file trên
GCS đều là bản export từ đó (job ``ops_to_source``). Nếu generator ghi thẳng
GCS thì có hai nguồn sự thật và sớm muộn chúng phân kỳ.
"""

from __future__ import annotations

import base64
import json
import time
import os
import random
from datetime import timezone
import sys
from pathlib import Path

import pandas as pd
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))
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
    """DSN Cloud SQL cho ``ops_db`` hoặc ``warehouse_db``.

    Host luôn lấy từ env ``PG_HOST`` (Private IP của instance) — không đặt trong
    YAML để chỉ có MỘT nguồn sự thật.
    """
    p = cfg["postgres"]
    host = os.environ.get("PG_HOST")
    if not host:
        raise RuntimeError("Thiếu PG_HOST (Private IP của Cloud SQL)")
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


# --------------------------------------------------------------------------- #
# Kafka — bảo mật                                                             #
# --------------------------------------------------------------------------- #
# BẢN SAO CÓ CHỦ ĐÍCH của airflow/include/kafka_conf.py: container
# stream-generator chỉ mount ./generator nên không import được include/.
# Sửa một bên thì phải sửa bên kia.
_GCP_KAFKA_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _metadata(path: str) -> str:
    """Đọc một trường từ metadata server của GCE."""
    import urllib.request

    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/" + path,
        headers={"Metadata-Flavor": "Google"})
    return urllib.request.urlopen(req, timeout=5).read().decode().strip()


def _principal(creds) -> str:
    """Email của service account — SASL principal mà Managed Kafka đòi.

    librdkafka bắt buộc principal khác rỗng. Trên GCE, ``google.auth.default()``
    trả về ComputeEngineCredentials với ``service_account_email`` là "default"
    cho tới khi refresh, nên phải hỏi metadata server.
    """
    if p := os.environ.get("GOOGLE_MANAGED_KAFKA_AUTH_PRINCIPAL"):
        return p
    email = getattr(creds, "service_account_email", None)
    if email and email != "default":
        return email
    return _metadata("instance/service-accounts/default/email")


# Managed Kafka KHÔNG nhận access token thô. Nó đòi một JWT gói quanh token, đúng
# như GcpLoginCallbackHandler của Google dựng (đã dịch ngược từ
# managed-kafka-auth-login-handler-1.0.6):
#
#   b64url({"typ":"JWT","alg":"GOOG_OAUTH2_TOKEN"})
#   + "." + b64url({"exp":<hết hạn>,"iat":<bây giờ>,"scope":"kafka","sub":<email SA>})
#   + "." + b64url(<access token>)
#
# base64url KHÔNG padding, ba phần nối bằng dấu chấm. Gửi access token thô sẽ bị
# broker trả "invalid credentials with SASL mechanism OAUTHBEARER" — thông báo
# không hề gợi ý rằng vấn đề là ĐỊNH DẠNG chứ không phải quyền.
_JWT_HEADER = {"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"}


def _b64(raw: str) -> str:
    """base64url không padding — giống Base64.getUrlEncoder().withoutPadding()."""
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _kafka_access_token(token: str, exp_epoch: int, subject: str) -> str:
    """Gói access token thành JWT mà Managed Kafka chấp nhận.

    ``scope`` là chuỗi cố định ``"kafka"`` (không phải scope OAuth dùng để LẤY
    token — cái đó là cloud-platform).
    """
    claims = {"exp": int(exp_epoch), "iat": int(time.time()),
              "scope": "kafka", "sub": subject}
    return ".".join([
        _b64(json.dumps(_JWT_HEADER, separators=(",", ":"))),
        _b64(json.dumps(claims, separators=(",", ":"))),
        _b64(token),
    ])


def _oauth_token_cb(_config: str):
    """Trả 4-tuple ``(token, expiry_epoch_giây, principal, extensions)``.

    Hai chỗ dễ sai, cả hai đều báo cùng một lỗi "invalid credentials":
      1. Số phần tử: hợp đồng ``oauth_cb`` là 4-tuple, không phải 2.
      2. Định dạng token: phải là JWT bọc quanh access token (xem
         ``_kafka_access_token``), không phải access token thô.

    Dùng ADC: trên VM GCP đó là service account gắn kèm, không cần key file.
    SA cần role ``roles/managedkafka.client``.
    """
    import google.auth
    import google.auth.transport.requests

    creds, _ = google.auth.default(scopes=[_GCP_KAFKA_SCOPE])
    creds.refresh(google.auth.transport.requests.Request())

    # creds.expiry là datetime NAIVE biểu diễn UTC. Gọi .timestamp() trực tiếp sẽ
    # được hiểu là giờ ĐỊA PHƯƠNG — container chạy TZ=Asia/Ho_Chi_Minh nên token
    # trông như đã hết hạn 7 tiếng trước và librdkafka loại nó.
    expiry = creds.expiry
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    exp = expiry.timestamp()

    principal = _principal(creds)
    return _kafka_access_token(creds.token, exp, principal), exp, principal, {}


def kafka_client_config(**extra) -> dict:
    """Config Producer/Consumer cho Managed Kafka (SASL_SSL)."""
    mechanism = os.environ.get("KAFKA_SASL_MECHANISM", "OAUTHBEARER").upper()
    cfg: dict = {"security.protocol": "SASL_SSL", "sasl.mechanisms": mechanism}
    if ca := os.environ.get("KAFKA_SSL_CAFILE"):
        cfg["ssl.ca.location"] = ca
    if mechanism == "OAUTHBEARER":
        cfg["oauth_cb"] = _oauth_token_cb
    else:
        cfg["sasl.username"] = os.environ.get("KAFKA_SASL_USERNAME", "")
        cfg["sasl.password"] = os.environ.get("KAFKA_SASL_PASSWORD", "")
    cfg.update(extra)
    return cfg


def kafka_describe() -> str:
    """Mô tả một dòng để log (không lộ secret)."""
    return f"SASL_SSL/{os.environ.get('KAFKA_SASL_MECHANISM', 'OAUTHBEARER').upper()}"
