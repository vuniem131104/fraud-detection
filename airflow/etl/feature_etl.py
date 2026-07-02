"""Batch feature ETL: transactions -> application.transaction_features.

Reads the raw six-table dataset, engineers a point-in-time-correct feature set
(calendar, account/card static, card & user velocity, amount z-score, entity
graph, prior declines) plus the fraud label, and loads it into
``application.transaction_features`` via a fast ``COPY``.

Leakage discipline
------------------
Every derived feature uses **only data available at or before the transaction's
own timestamp** (trailing time windows, expanding stats shifted by one, and
as-of cumulative counts). The table is rebuilt in full each run because the
point-in-time windows require the complete history. ``label`` is the target, not
a feature, and is NULL for transactions too recent to have ground truth.
"""

import io
import os
import warnings

import numpy as np
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

# psycopg 3 (the project's declared driver) is used directly rather than a
# SQLAlchemy engine: Airflow pins SQLAlchemy < 2.0, but pandas 2.3 only accepts
# a SQLAlchemy >= 2.0 connectable, so it can't use a 1.4 engine. A raw psycopg
# connection sidesteps that for read_sql, and COPY is used for the write.
CONNINFO = (
    f"host={os.getenv('POSTGRES_HOST')} port={os.getenv('POSTGRES_PORT')} "
    f"user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} "
    f"dbname={os.getenv('POSTGRES_DB')}"
)

SOURCE_SQL = """
SELECT
    t.id            AS transaction_id,
    t.user_id,
    t.card_id,
    t.merchant_id,
    t.device_id,
    t.created_at,
    t.amount_usd,
    t.channel,
    t.status,
    t.billing_country_code,
    t.ip_country_code,
    t.email_purchaser,
    t.email_recipient,
    u.created_at        AS user_created_at,
    u.country_code      AS user_country,
    u.customer_segment,
    u.kyc_level,
    u.email_verified,
    c.created_at        AS card_created_at,
    c.brand            AS card_brand,
    c.type             AS card_type,
    c.is_virtual,
    m.category         AS merchant_category,
    m.risk_level       AS merchant_risk_level,
    l.label,
    l.label_source
FROM application.transactions t
JOIN application.users u      ON u.id = t.user_id
JOIN application.cards c      ON c.id = t.card_id
JOIN application.merchants m  ON m.id = t.merchant_id
LEFT JOIN application.labels l ON l.transaction_id = t.id
ORDER BY t.created_at
"""

FEATURES_DDL = """
CREATE TABLE IF NOT EXISTS application.transaction_features (
    transaction_id                 TEXT PRIMARY KEY REFERENCES application.transactions(id) ON DELETE CASCADE,
    created_at                     TIMESTAMPTZ NOT NULL,
    user_id                        TEXT NOT NULL,
    card_id                        TEXT NOT NULL,
    merchant_id                    TEXT NOT NULL,
    device_id                      TEXT NOT NULL,
    amount_usd                     DOUBLE PRECISION,
    log_amount                     DOUBLE PRECISION,
    hour                           SMALLINT,
    weekday                        SMALLINT,
    is_night                       SMALLINT,
    channel                        TEXT,
    card_brand                     TEXT,
    card_type                      TEXT,
    is_virtual                     BOOLEAN,
    customer_segment               TEXT,
    kyc_level                      SMALLINT,
    email_verified                 BOOLEAN,
    merchant_category              TEXT,
    merchant_risk_level            SMALLINT,
    account_age_days               INTEGER,
    card_age_days                  INTEGER,
    geo_mismatch                   SMALLINT,
    foreign_ip                     SMALLINT,
    recipient_differs              SMALLINT,
    is_declined                    SMALLINT,
    card_tx_count_1h               INTEGER,
    card_tx_count_24h              INTEGER,
    card_amount_sum_24h            DOUBLE PRECISION,
    card_seconds_since_last_tx     DOUBLE PRECISION,
    card_amount_zscore             DOUBLE PRECISION,
    card_tx_seq                    INTEGER,
    card_distinct_merchants_sofar  INTEGER,
    card_declines_24h              INTEGER,
    user_tx_count_24h              INTEGER,
    user_amount_sum_24h            DOUBLE PRECISION,
    user_seconds_since_last_tx     DOUBLE PRECISION,
    device_distinct_users_sofar    INTEGER,
    device_tx_count_sofar          INTEGER,
    recipient_distinct_users_sofar INTEGER,
    label                          SMALLINT,
    label_source                   TEXT,
    computed_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS transaction_features_label_idx   ON application.transaction_features (label);
CREATE INDEX IF NOT EXISTS transaction_features_created_idx ON application.transaction_features (created_at);
"""

# Column order for the COPY write (must match FEATURES_DDL, excluding computed_at).
FEATURE_COLUMNS = [
    "transaction_id", "created_at", "user_id", "card_id", "merchant_id", "device_id",
    "amount_usd", "log_amount", "hour", "weekday", "is_night",
    "channel", "card_brand", "card_type", "is_virtual",
    "customer_segment", "kyc_level", "email_verified",
    "merchant_category", "merchant_risk_level",
    "account_age_days", "card_age_days",
    "geo_mismatch", "foreign_ip", "recipient_differs", "is_declined",
    "card_tx_count_1h", "card_tx_count_24h", "card_amount_sum_24h",
    "card_seconds_since_last_tx", "card_amount_zscore", "card_tx_seq",
    "card_distinct_merchants_sofar", "card_declines_24h",
    "user_tx_count_24h", "user_amount_sum_24h", "user_seconds_since_last_tx",
    "device_distinct_users_sofar", "device_tx_count_sofar", "recipient_distinct_users_sofar",
    "label", "label_source",
]


def read_transactions() -> pd.DataFrame:
    """Load the joined source rows, sorted by ``created_at`` (required for windows)."""
    with psycopg.connect(CONNINFO) as conn:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
            df = pd.read_sql(SOURCE_SQL, conn)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["user_created_at"] = pd.to_datetime(df["user_created_at"], utc=True)
    df["card_created_at"] = pd.to_datetime(df["card_created_at"], utc=True)
    df["amount_usd"] = df["amount_usd"].astype(float)
    return df.sort_values("created_at").reset_index(drop=True)


def add_static_features(df: pd.DataFrame) -> None:
    """Row-local features: calendar, ages, amount transform, geo/identity flags."""
    ts = df["created_at"].dt
    df["hour"] = ts.hour.astype("int16")
    df["weekday"] = ts.dayofweek.astype("int16")
    df["is_night"] = ((df["hour"] < 6) | (df["hour"] >= 23)).astype("int16")
    df["log_amount"] = np.log1p(df["amount_usd"])
    df["account_age_days"] = (df["created_at"] - df["user_created_at"]).dt.days
    df["card_age_days"] = (df["created_at"] - df["card_created_at"]).dt.days
    df["geo_mismatch"] = (df["billing_country_code"] != df["ip_country_code"]).astype("int16")
    df["foreign_ip"] = (df["ip_country_code"] != df["user_country"]).astype("int16")
    df["recipient_differs"] = (
        df["email_recipient"].notna() & (df["email_recipient"] != df["email_purchaser"])
    ).astype("int16")
    # NOTE: current-tx status is a *post-authorization* signal — do not feed it to a
    # pre-auth model (see card_declines_24h for the leakage-safe historical version).
    df["is_declined"] = (df["status"] == "declined").astype("int16")


def add_velocity_features(df: pd.DataFrame) -> None:
    """Trailing time-window + expanding features per card and per user.

    Rolling stats are computed on an entity+time-sorted view and written back
    positionally: ``groupby().rolling(on=...)`` indexes its result by the *on*
    column, so index-based realignment would silently produce NaN. Windows
    include the current transaction (its attributes are known at scoring time);
    ``card_declines_24h`` and the ``*_seconds_since_last_tx`` gaps use prior rows.
    """
    # --- per card, on a (card, time)-sorted view ---
    dc = df.sort_values(["card_id", "created_at"])
    by_card = dc.groupby("card_id", sort=False)
    r1h = by_card.rolling("1h", on="created_at")["amount_usd"]
    r24 = by_card.rolling("24h", on="created_at")
    dc["card_tx_count_1h"] = r1h.count().values
    dc["card_tx_count_24h"] = r24["amount_usd"].count().values
    dc["card_amount_sum_24h"] = r24["amount_usd"].sum().values
    declines_incl = by_card.rolling("24h", on="created_at")["is_declined"].sum().values
    dc["card_declines_24h"] = np.clip(declines_incl - dc["is_declined"].to_numpy(), 0, None)
    dc["card_tx_seq"] = by_card.cumcount() + 1
    dc["card_seconds_since_last_tx"] = by_card["created_at"].diff().dt.total_seconds()
    amt = by_card["amount_usd"]
    mean_prior = amt.transform(lambda s: s.expanding().mean().shift(1))
    std_prior = amt.transform(lambda s: s.expanding().std().shift(1))
    dc["card_amount_zscore"] = (
        (dc["amount_usd"] - mean_prior) / std_prior).replace([np.inf, -np.inf], np.nan)
    for col in ("card_tx_count_1h", "card_tx_count_24h", "card_amount_sum_24h",
                "card_declines_24h", "card_tx_seq", "card_seconds_since_last_tx",
                "card_amount_zscore"):
        df[col] = dc[col]

    # --- per user, on a (user, time)-sorted view ---
    du = df.sort_values(["user_id", "created_at"])
    by_user = du.groupby("user_id", sort=False)
    ru = by_user.rolling("24h", on="created_at")
    du["user_tx_count_24h"] = ru["amount_usd"].count().values
    du["user_amount_sum_24h"] = ru["amount_usd"].sum().values
    du["user_seconds_since_last_tx"] = by_user["created_at"].diff().dt.total_seconds()
    for col in ("user_tx_count_24h", "user_amount_sum_24h", "user_seconds_since_last_tx"):
        df[col] = du[col]

    for col in ("card_tx_count_1h", "card_tx_count_24h", "card_declines_24h",
                "card_tx_seq", "user_tx_count_24h"):
        df[col] = df[col].astype("int32")


def add_graph_features(df: pd.DataFrame) -> None:
    """As-of cumulative graph counts (fraud-ring / device-farm signals).

    For each row: how many distinct users / transactions this device has seen so
    far, and how many distinct users share this recipient email so far. Computed
    via first-occurrence cumsum on the time-sorted frame, so it never peeks ahead.
    """
    df["device_tx_count_sofar"] = (
        df.groupby("device_id", sort=False).cumcount() + 1).astype("int32")
    first_dev_user = ~df.duplicated(["device_id", "user_id"])
    df["device_distinct_users_sofar"] = (
        first_dev_user.groupby(df["device_id"], sort=False).cumsum().astype("int32"))

    first_card_merch = ~df.duplicated(["card_id", "merchant_id"])
    df["card_distinct_merchants_sofar"] = (
        first_card_merch.groupby(df["card_id"], sort=False).cumsum().astype("int32"))

    has_rcpt = df["email_recipient"].notna()
    first_rcpt_user = pd.Series(False, index=df.index)
    first_rcpt_user.loc[has_rcpt] = ~df.loc[has_rcpt].duplicated(["email_recipient", "user_id"])
    rcpt = first_rcpt_user.groupby(df["email_recipient"], sort=False).cumsum()
    df["recipient_distinct_users_sofar"] = rcpt.fillna(0).astype("int32")
    df.loc[~has_rcpt, "recipient_distinct_users_sofar"] = 0


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Run every feature stage and return the frame with typed nullable columns."""
    add_static_features(df)
    add_velocity_features(df)
    add_graph_features(df)
    df["label"] = df["label"].astype("Int64")   # nullable: NULL for unlabeled tx
    return df


def write_features(df: pd.DataFrame) -> int:
    """(Re)create the feature table and bulk-load rows via CSV COPY. Returns row count.

    CSV COPY (rather than ``write_row``) avoids psycopg's lack of adapters for
    numpy scalar dtypes and maps NaN/NaT/<NA> to SQL NULL via ``na_rep=''``.
    """
    payload = df[FEATURE_COLUMNS]
    buf = io.StringIO()
    payload.to_csv(buf, index=False, header=False, na_rep="")
    columns = ", ".join(FEATURE_COLUMNS)
    with psycopg.connect(CONNINFO) as conn:
        with conn.cursor() as cur:
            cur.execute(FEATURES_DDL)
            cur.execute("TRUNCATE application.transaction_features")
            with cur.copy(
                f"COPY application.transaction_features ({columns}) "
                f"FROM STDIN WITH (FORMAT csv, NULL '')"
            ) as copy:
                copy.write(buf.getvalue())
        conn.commit()
    return len(payload)


def build_transaction_features() -> int:
    """Airflow entry point: read -> engineer -> load. Returns rows written."""
    df = read_transactions()
    print(f"Read {len(df):,} transactions")
    df = engineer_features(df)
    written = write_features(df)
    print(f"Wrote {written:,} rows to application.transaction_features "
          f"({len(FEATURE_COLUMNS)} feature columns)")
    return written


if __name__ == "__main__":
    build_transaction_features()
