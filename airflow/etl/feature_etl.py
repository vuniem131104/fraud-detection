"""Incremental feature ETL: transactions -> application.transaction_features.

The point-in-time feature set (calendar, account/card static, card & user
velocity, amount z-score, prior declines) is computed **entirely in Postgres**
via window functions and only the rows inside the run's time window are inserted.
Nothing is pulled into pandas, so the job's memory stays flat regardless of how
large the history grows.

Incremental strategy
--------------------
For a daily run over ``[start, end)`` we only need feature rows for transactions
created in that window. But the velocity / expanding features of those rows
depend on each card's & user's *earlier* history, so we can't look at the window
alone. Instead we scope the computation to the **full history of exactly the
cards and users active in the window** (an index seek on ``transactions``), run
the window functions over that scoped set — which is complete for every entity
we insert — and write only the window's rows. Work scales with *active* entities,
not total table size. ``ON CONFLICT DO NOTHING`` makes re-runs idempotent
(point-in-time features never change once computed).

Passing ``start=end=None`` computes over all history (one-off backfill).

Leakage discipline
------------------
Every feature uses only data available at or before the transaction's own
timestamp (trailing time windows, expanding stats framed to prior rows, gaps to
the previous transaction). Calendar parts use UTC to match the original design.
The fraud label is *not* stored here — it lives in ``application.labels`` and is
joined on ``transaction_id`` at training time.
"""

import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv

load_dotenv()

CONNINFO = (
    f"host={os.getenv('POSTGRES_HOST')} port={os.getenv('POSTGRES_PORT')} "
    f"user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} "
    f"dbname={os.getenv('POSTGRES_DB')}"
)

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
    card_declines_24h              INTEGER,
    user_tx_count_24h              INTEGER,
    user_amount_sum_24h            DOUBLE PRECISION,
    user_seconds_since_last_tx     DOUBLE PRECISION,
    computed_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS transaction_features_created_idx ON application.transaction_features (created_at);
-- Supports the per-entity window scans and the active-entity lookup below.
CREATE INDEX IF NOT EXISTS transactions_card_created_idx ON application.transactions (card_id, created_at);
CREATE INDEX IF NOT EXISTS transactions_user_created_idx ON application.transactions (user_id, created_at);
"""

# Feature column order (must match the SELECT aliases in _BUILD_TEMPLATE below).
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
    "card_declines_24h",
    "user_tx_count_24h", "user_amount_sum_24h", "user_seconds_since_last_tx",
]

# The point-in-time feature computation. ``{with_kw}``/``{ctx_where}``/
# ``{insert_where}`` are filled in per-mode (incremental vs backfill); ``{cols}``
# is the shared column list. Named windows (WINDOW clause) keep the frames DRY.
_BUILD_TEMPLATE = """
{with_kw} ctx AS (
    SELECT
        t.id AS transaction_id, t.created_at, t.user_id, t.card_id, t.merchant_id, t.device_id,
        t.amount_usd, t.channel, t.status, t.billing_country_code, t.ip_country_code,
        t.email_purchaser, t.email_recipient,
        u.created_at AS user_created_at, u.country_code AS user_country,
        u.customer_segment, u.kyc_level, u.email_verified,
        c.created_at AS card_created_at, c.brand AS card_brand, c.type AS card_type, c.is_virtual,
        m.category AS merchant_category, m.risk_level AS merchant_risk_level
    FROM application.transactions t
    JOIN application.users u     ON u.id = t.user_id
    JOIN application.cards c     ON c.id = t.card_id
    JOIN application.merchants m ON m.id = t.merchant_id
    {ctx_where}
),
feat AS (
    SELECT
        transaction_id, created_at, user_id, card_id, merchant_id, device_id,
        amount_usd,
        ln(1 + amount_usd) AS log_amount,
        extract(hour from created_at at time zone 'UTC')::smallint AS hour,
        (extract(isodow from created_at at time zone 'UTC')::int - 1)::smallint AS weekday,
        (CASE WHEN extract(hour from created_at at time zone 'UTC') < 6
                OR extract(hour from created_at at time zone 'UTC') >= 23
              THEN 1 ELSE 0 END)::smallint AS is_night,
        channel, card_brand, card_type, is_virtual,
        customer_segment, kyc_level, email_verified,
        merchant_category, merchant_risk_level,
        floor(extract(epoch from (created_at - user_created_at)) / 86400)::int AS account_age_days,
        floor(extract(epoch from (created_at - card_created_at)) / 86400)::int AS card_age_days,
        (billing_country_code IS DISTINCT FROM ip_country_code)::int::smallint AS geo_mismatch,
        (ip_country_code IS DISTINCT FROM user_country)::int::smallint AS foreign_ip,
        (email_recipient IS NOT NULL
            AND email_recipient IS DISTINCT FROM email_purchaser)::int::smallint AS recipient_differs,
        (status = 'declined')::int::smallint AS is_declined,
        (count(*) OVER w_card_1h)::int AS card_tx_count_1h,
        (count(*) OVER w_card_24h)::int AS card_tx_count_24h,
        sum(amount_usd) OVER w_card_24h AS card_amount_sum_24h,
        extract(epoch from (created_at - lag(created_at) OVER w_card)) AS card_seconds_since_last_tx,
        (amount_usd - avg(amount_usd) OVER w_card_prior)
            / nullif(stddev_samp(amount_usd) OVER w_card_prior, 0) AS card_amount_zscore,
        (row_number() OVER w_card)::int AS card_tx_seq,
        greatest((sum((status = 'declined')::int) OVER w_card_24h)
                 - (status = 'declined')::int, 0)::int AS card_declines_24h,
        (count(*) OVER w_user_24h)::int AS user_tx_count_24h,
        sum(amount_usd) OVER w_user_24h AS user_amount_sum_24h,
        extract(epoch from (created_at - lag(created_at) OVER w_user)) AS user_seconds_since_last_tx
    FROM ctx
    WINDOW
        w_card       AS (PARTITION BY card_id ORDER BY created_at),
        w_card_1h    AS (PARTITION BY card_id ORDER BY created_at
                         RANGE BETWEEN INTERVAL '1 hour'  PRECEDING AND CURRENT ROW),
        w_card_24h   AS (PARTITION BY card_id ORDER BY created_at
                         RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW),
        w_card_prior AS (PARTITION BY card_id ORDER BY created_at
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
        w_user       AS (PARTITION BY user_id ORDER BY created_at),
        w_user_24h   AS (PARTITION BY user_id ORDER BY created_at
                         RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW)
)
INSERT INTO application.transaction_features ({cols})
SELECT {cols} FROM feat
{insert_where}
ON CONFLICT (transaction_id) DO NOTHING
"""

_WINDOW_PREDICATE = "created_at >= %(start)s::timestamptz AND created_at < %(end)s::timestamptz"


def _build_sql(incremental: bool) -> str:
    """Assemble the INSERT for either an incremental window or a full backfill."""
    cols = ", ".join(FEATURE_COLUMNS)
    if incremental:
        with_kw = (
            "WITH win AS (\n"
            "    SELECT DISTINCT card_id, user_id FROM application.transactions\n"
            f"    WHERE {_WINDOW_PREDICATE}\n"
            "),"
        )
        ctx_where = (
            "WHERE t.card_id IN (SELECT card_id FROM win)\n"
            "       OR t.user_id IN (SELECT user_id FROM win)"
        )
        insert_where = f"WHERE {_WINDOW_PREDICATE}"
    else:
        with_kw, ctx_where, insert_where = "WITH", "", ""
    return _BUILD_TEMPLATE.format(
        with_kw=with_kw, ctx_where=ctx_where, insert_where=insert_where, cols=cols
    )


def check_source() -> None:
    """Fail fast if the source tables are empty (nothing to build features from)."""
    with psycopg.connect(CONNINFO) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM application.transactions")
        n_tx = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM application.labels")
        n_lbl = cur.fetchone()[0]
    print(f"Source: {n_tx:,} transactions, {n_lbl:,} labels")
    if n_tx == 0:
        raise ValueError("application.transactions is empty — aborting feature build")


def build_features(start: str | datetime | None = None,
                   end: str | datetime | None = None) -> int:
    """Compute & upsert features for ``[start, end)``; both None => full backfill.

    Returns the number of rows actually inserted (0 on an idempotent re-run).
    """
    incremental = start is not None and end is not None
    sql = _build_sql(incremental)
    params = {"start": start, "end": end} if incremental else {}
    scope = f"window [{start}, {end})" if incremental else "FULL history (backfill)"
    print(f"Building features for {scope}")
    with psycopg.connect(CONNINFO) as conn, conn.cursor() as cur:
        cur.execute(FEATURES_DDL)
        cur.execute(sql, params)
        inserted = cur.rowcount
        conn.commit()
    print(f"Inserted {inserted:,} feature rows ({len(FEATURE_COLUMNS)} columns)")
    return inserted


def validate_output(start: str | datetime | None = None,
                    end: str | datetime | None = None) -> None:
    """Assert every source transaction in scope has a matching feature row."""
    incremental = start is not None and end is not None
    with psycopg.connect(CONNINFO) as conn, conn.cursor() as cur:
        if incremental:
            params = {"start": start, "end": end}
            cur.execute(
                f"SELECT count(*) FROM application.transactions WHERE {_WINDOW_PREDICATE}", params)
            expected = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM application.transaction_features WHERE {_WINDOW_PREDICATE}",
                params)
            actual = cur.fetchone()[0]
        else:
            cur.execute("SELECT count(*) FROM application.transactions")
            expected = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM application.transaction_features")
            actual = cur.fetchone()[0]
    print(f"Validate: {actual:,} feature rows for {expected:,} transactions in scope")
    if actual != expected:
        raise ValueError(f"Coverage mismatch: {actual:,} feature rows vs {expected:,} transactions")


if __name__ == "__main__":
    # Standalone/local run: full backfill of the whole history.
    check_source()
    build_features()
    validate_output()
