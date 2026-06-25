"""Initialize the fraud-detection application schema and seed demo data in Postgres.

This script (re)creates the ``application`` schema and its core tables
(``users``, ``cards``, ``transactions``, ``prediction_logs``, ``feature_snapshots``,
``labels``) and then populates them with deterministic, realistic demo data:

* fake users with addresses derived from a fixed set of email domains,
* payment cards distributed across countries/brands/types,
* historical transactions consisting of a bulk of "normal" transactions plus a
  small number of crafted anomalous transactions (high amount, foreign billing,
  rooted/old device, throwaway recipient email) for selected aged cards.

It can run in ``--schema-only`` mode to just (re)build the schema, or seed the
full dataset. All seeded timestamps can be shifted into the past via ``--days-ago``.
The module also contains helpers that build feature-style "history"/"anomaly"
dictionaries used by downstream model tooling.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import random
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import asyncpg

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES_PATH = REPO_ROOT / "src"
sys.path.insert(0, str(SERVICES_PATH))

try:
    from database.postgres import PostgresDatabase
except ModuleNotFoundError:
    uv_path = shutil.which("uv")
    if uv_path and not os.environ.get("INIT_HISTORICAL_DATA_UV_REEXEC"):
        env = {**os.environ, "INIT_HISTORICAL_DATA_UV_REEXEC": "1"}
        os.execvpe(uv_path, [uv_path, "run", "python", __file__, *sys.argv[1:]], env)
    raise


APPLICATION_DDL = """
CREATE SCHEMA IF NOT EXISTS application;

DROP TABLE IF EXISTS application.transactions CASCADE;
DROP TABLE IF EXISTS application.cards CASCADE;
DROP TABLE IF EXISTS application.users CASCADE;

CREATE TABLE application.users (
    id TEXT PRIMARY KEY CHECK (id ~ '^[0-9a-f]{32}$'),
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE application.cards (
    id TEXT PRIMARY KEY CHECK (id ~ '^[0-9a-f]{32}$'),
    user_id TEXT NOT NULL REFERENCES application.users(id) ON DELETE CASCADE,
    issuer_code TEXT NOT NULL,
    country INTEGER NOT NULL,
    brand TEXT NOT NULL,
    type TEXT NOT NULL,
    bin_code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE application.transactions (
    id TEXT PRIMARY KEY CHECK (id ~ '^[0-9a-f]{32}$'),
    user_id TEXT NOT NULL REFERENCES application.users(id) ON DELETE CASCADE,
    card_id TEXT REFERENCES application.cards(id) ON DELETE SET NULL,
    amount_usd NUMERIC(14, 2) NOT NULL CHECK (amount_usd > 0),
    channel TEXT,
    billing_zone INTEGER,
    billing_country INTEGER,
    email_purchaser TEXT,
    email_recipient TEXT,
    device_info TEXT,
    device_type TEXT,
    os_raw TEXT,
    browser_raw TEXT,
    screen_resolution TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE application.prediction_logs (
    id BIGSERIAL PRIMARY KEY,
    tx_id TEXT REFERENCES application.transactions(id),
    model_name TEXT,
    model_version TEXT,
    fraud_score DOUBLE PRECISION,
    prediction INT,
    threshold DOUBLE PRECISION,
    latency_ms DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE application.feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    tx_id TEXT,
    features JSONB,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE application.labels (
    tx_id TEXT PRIMARY KEY REFERENCES application.transactions(id),
    label INT NOT NULL,
    label_source TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX cards_user_id_idx
    ON application.cards (user_id);

CREATE INDEX transactions_card_created_at_idx
    ON application.transactions (card_id, created_at DESC);

CREATE INDEX transactions_user_created_at_idx
    ON application.transactions (user_id, created_at DESC);
"""


EMAIL_BIN = {
    "gmail.com": "google",
    "googlemail.com": "google",
    "yahoo.com": "yahoo",
    "ymail.com": "yahoo",
    "rocketmail.com": "yahoo",
    "hotmail.com": "microsoft",
    "outlook.com": "microsoft",
    "live.com": "microsoft",
    "msn.com": "microsoft",
    "aol.com": "aol",
    "aim.com": "aol",
    "icloud.com": "apple",
    "me.com": "apple",
    "mac.com": "apple",
}
EMAIL_NULLS = {"anonymous.com", "mail.com"}
PBKDF2_ITERATIONS = 210_000
DEFAULT_USER_COUNT = 1_000
DEFAULT_PASSWORD = "demo12345"
FAKE_EMAIL_PREFIX = "fake.user"
FAKE_EMAIL_PATTERN = f"{FAKE_EMAIL_PREFIX}.%@%"

DEFAULT_CARD_COUNT = 1_200
DEFAULT_CARD_SEED = 42
DEFAULT_TRANSACTION_COUNT = 10_000
DEFAULT_TRANSACTION_SEED = 99
HO_CHI_MINH_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

COUNTRIES = [
    (840, 1),  # US, North America
    (124, 1),  # CA, North America
    (826, 2),  # GB, Europe
    (276, 2),  # DE, Europe
    (250, 2),  # FR, Europe
    (704, 3),  # VN, APAC
    (702, 3),  # SG, APAC
    (392, 3),  # JP, APAC
    (410, 3),  # KR, APAC
    (36, 3),  # AU, APAC
]
CARD_BRANDS = ("visa", "mastercard", "amex")
CARD_TYPES = ("credit", "debit")
CHANNELS = ("web", "mobile_app", "pos")
DEVICE_PROFILES = (
    ("mobile", "iOS 17", "Safari Mobile", "390x844"),
    ("mobile", "Android 14", "Chrome Mobile", "412x915"),
    ("desktop", "macOS 14", "Safari", "1440x900"),
    ("desktop", "Windows 11", "Chrome", "1920x1080"),
    ("desktop", "Windows 11", "Edge", "1366x768"),
)

DEFAULT_ANOMALY_COUNT = 100
CHANNEL_TO_MODEL = {
    "web": "W",
    "mobile_app": "C",
    "pos": "R",
}
ABNORMAL_BILLING = [
    (643, 4),  # RU / different zone
    (566, 4),  # NG
    (76, 4),  # BR
    (484, 4),  # MX
    (792, 4),  # TR
]


def domains() -> list[str]:
    """Return the list of email domains used to generate fake user addresses."""
    return [*EMAIL_BIN.keys(), *sorted(EMAIL_NULLS)]


def local_now(days_ago: int = 0) -> datetime:
    """Return the current Ho Chi Minh time, optionally shifted back by ``days_ago`` days."""
    return datetime.now(HO_CHI_MINH_TZ) - timedelta(days=days_ago)


def to_local_time(value: datetime) -> datetime:
    """Convert a datetime to the Ho Chi Minh timezone.

    Naive datetimes are assumed to already be in Ho Chi Minh local time and are
    simply tagged with that timezone; aware datetimes are converted.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=HO_CHI_MINH_TZ)
    return value.astimezone(HO_CHI_MINH_TZ)


def hash_password(password: str) -> str:
    """Return the hex PBKDF2-HMAC-SHA256 hash of ``password`` using a fixed iteration count."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        b"",
        PBKDF2_ITERATIONS,
    ).hex()


def fake_users(count: int, password: str, days_ago: int = 0) -> list[tuple[str, str, str, str, datetime]]:
    """Build ``count`` fake user rows ready for insertion into ``application.users``.

    Each user gets a deterministic name and email (cycling through the available
    domains), the hashed ``password``, and a ``created_at`` staggered one minute
    apart starting just before the (optionally shifted) current time.

    Args:
        count: Number of user rows to generate.
        password: Plaintext password to hash for every user.
        days_ago: Shift the effective "now" back by this many days.

    Returns:
        A list of ``(id, name, email, password_hash, created_at)`` tuples.
    """
    available_domains = domains()
    base_time = local_now(days_ago).replace(microsecond=0) - timedelta(days=count)
    rows = []

    for index in range(1, count + 1):
        domain = available_domains[(index - 1) % len(available_domains)]
        email = f"{FAKE_EMAIL_PREFIX}.{index:06d}@{domain}"
        rows.append(
            (
                uuid4().hex,
                f"Fake User {index:04d}",
                email,
                hash_password(password),
                base_time + timedelta(minutes=index),
            )
        )

    return rows


async def seed_fake_users(
    database: PostgresDatabase,
    count: int,
    password: str,
    *,
    replace: bool = False,
    days_ago: int = 0,
) -> tuple[int, int]:
    """Insert fake users into Postgres, skipping existing emails.

    Optionally deletes previously seeded fake users first (when ``replace`` is
    true), then bulk-inserts freshly generated rows with ``ON CONFLICT DO NOTHING``.

    Args:
        database: Open Postgres database wrapper.
        count: Number of fake users to generate.
        password: Plaintext password to hash for every user.
        replace: If true, delete existing fake users before inserting.
        days_ago: Shift the effective "now" back by this many days.

    Returns:
        A tuple ``(newly_inserted, total_fake_users)`` counting the fake users
        present after the operation.
    """
    available_domains = domains()
    rows = fake_users(count, password, days_ago=days_ago)

    async with database.transaction() as conn:
        if replace:
            await conn.execute(
                """
                DELETE FROM application.users
                WHERE email LIKE $1
                  AND split_part(email, '@', 2) = ANY($2)
                """,
                FAKE_EMAIL_PATTERN,
                available_domains,
            )

        before = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM application.users
            WHERE email LIKE $1
              AND split_part(email, '@', 2) = ANY($2)
            """,
            FAKE_EMAIL_PATTERN,
            available_domains,
        )

        await conn.executemany(
            """
            INSERT INTO application.users
                (id, name, email, password_hash, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (email) DO NOTHING
            """,
            rows,
        )

        after = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM application.users
            WHERE email LIKE $1
              AND split_part(email, '@', 2) = ANY($2)
            """,
            FAKE_EMAIL_PATTERN,
            available_domains,
        )

    return after - before, after


def issuer_code(country: int, index: int) -> str:
    """Build a synthetic card issuer code from a country and an index."""
    return f"ISS-{country}-{index % 97:02d}"


def bin_code(brand: str, rng: random.Random) -> str:
    """Generate a random BIN whose leading digit matches the card ``brand``."""
    if brand == "visa":
        return f"4{rng.randint(10000, 99999)}"
    if brand == "mastercard":
        return f"5{rng.randint(10000, 99999)}"
    return f"3{rng.randint(10000, 99999)}"


def normal_amount_usd(rng: random.Random) -> float:
    """Draw a realistic "normal" transaction amount in USD, clamped to ``[3, 350]``."""
    amount = rng.lognormvariate(3.35, 0.65)
    return round(min(max(amount, 3.0), 350.0), 2)


def country_zone(country: int) -> int:
    """Return the geographic zone for a numeric country code, defaulting to zone 1."""
    for country_code, zone in COUNTRIES:
        if country_code == country:
            return zone
    return 1


def card_created_at(user_created_at: datetime, rng: random.Random, now: datetime) -> datetime:
    """Pick a plausible card creation time after the owning user was created.

    Most cards are aged 60-720 days; ~20% are recently created (1-30 days). The
    result is always at least one day after ``user_created_at`` and no later than
    ``now``.
    """
    age_days = rng.randint(1, 30) if rng.random() < 0.2 else rng.randint(60, 720)
    target = now - timedelta(days=age_days, hours=rng.randint(0, 23))
    earliest = user_created_at + timedelta(days=1)
    if target <= earliest:
        available_seconds = max(int((now - earliest).total_seconds()), 3600)
        return earliest + timedelta(seconds=rng.randint(0, available_seconds))
    return target


def transaction_created_at(card_created_at_value: datetime, rng: random.Random, now: datetime) -> datetime:
    """Pick a plausible transaction time after the card was created.

    Weights recency so ~70% of transactions fall within the last 29 days, ~25%
    within 31-180 days, and ~5% within 181-540 days. The result is always at
    least one hour after the card creation time and no later than ``now``.
    """
    bucket = rng.random()
    if bucket < 0.7:
        age = timedelta(days=rng.randint(0, 29), seconds=rng.randint(0, 86_399))
    elif bucket < 0.95:
        age = timedelta(days=rng.randint(31, 180), seconds=rng.randint(0, 86_399))
    else:
        age = timedelta(days=rng.randint(181, 540), seconds=rng.randint(0, 86_399))

    target = now - age
    earliest = card_created_at_value + timedelta(hours=1)
    if target <= earliest:
        available_seconds = max(int((now - earliest).total_seconds()), 60)
        return earliest + timedelta(seconds=rng.randint(0, available_seconds))
    return target


def normal_billing(card_country: int, rng: random.Random) -> tuple[int, int]:
    """Choose a billing ``(country, zone)`` for a normal transaction.

    Usually returns the card's own country/zone, but ~6% of the time picks a
    different country within the same zone to add mild geographic variation.
    """
    zone = country_zone(card_country)
    if rng.random() >= 0.06:
        return card_country, zone

    same_zone = [
        (country_code, country_zone_value)
        for country_code, country_zone_value in COUNTRIES
        if country_zone_value == zone and country_code != card_country
    ]
    return rng.choice(same_zone) if same_zone else (card_country, zone)


def numeric_uuid(identifier: str) -> int:
    """Map a hex UUID string to a stable 32-bit-range integer."""
    return int(identifier, 16) % 2_147_483_647


def card_device_profile(card_id: str, rng: random.Random) -> tuple[str, str, str, str]:
    """Select a (device_type, os, browser, resolution) profile for a card.

    The choice is mostly deterministic per card (derived from the card id) with a
    small random jitter so a card occasionally appears on an adjacent profile.
    """
    profile_index = (numeric_uuid(card_id) + rng.randint(0, 1)) % len(DEVICE_PROFILES)
    return DEVICE_PROFILES[profile_index]


async def seed_cards(
    database: PostgresDatabase,
    *,
    count: int,
    seed: int,
    replace: bool,
    days_ago: int = 0,
) -> tuple[int, int]:
    """Seed payment cards for the existing fake users.

    Distributes cards deterministically across countries, brands and types, only
    inserting enough new cards to reach ``count`` total for the fake users.
    When ``replace`` is true, existing transactions and cards for those users are
    deleted first. Card creation times are constrained to follow the owning user.

    Args:
        database: Open Postgres database wrapper.
        count: Target total number of cards across fake users.
        seed: RNG seed for reproducible card generation.
        replace: If true, delete existing cards/transactions for the users first.
        days_ago: Shift the effective "now" back by this many days.

    Returns:
        A tuple ``(user_count, cards_inserted)``.
    """
    rng = random.Random(seed)
    now = local_now(days_ago).replace(microsecond=0)

    async with database.transaction() as conn:
        users = await conn.fetch(
            """
            SELECT id, email, created_at
            FROM application.users
            WHERE email LIKE $1
            ORDER BY id
            """,
            FAKE_EMAIL_PATTERN,
        )

        user_ids = [row["id"] for row in users]
        if not user_ids:
            return 0, 0

        if replace:
            await conn.execute(
                """
                DELETE FROM application.transactions
                WHERE user_id = ANY($1)
                """,
                user_ids,
            )
            await conn.execute(
                """
                DELETE FROM application.cards
                WHERE user_id = ANY($1)
                """,
                user_ids,
            )

        existing_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM application.cards
            WHERE user_id = ANY($1)
            """,
            user_ids,
        )

        cards_to_insert = max(count - existing_count, 0)
        card_rows = []

        for offset in range(cards_to_insert):
            index = existing_count + offset + 1
            user = users[offset % len(users)]
            country, billing_zone = COUNTRIES[(index - 1) % len(COUNTRIES)]
            brand = CARD_BRANDS[(index - 1) % len(CARD_BRANDS)]
            card_type = CARD_TYPES[(index - 1) % len(CARD_TYPES)]
            card_rows.append(
                (
                    uuid4().hex,
                    user["id"],
                    issuer_code(country, index),
                    country,
                    brand,
                    card_type,
                    bin_code(brand, rng),
                    card_created_at(user["created_at"], rng, now),
                )
            )

        if card_rows:
            await conn.executemany(
                """
                INSERT INTO application.cards
                    (id, user_id, issuer_code, country, brand, type, bin_code, created_at)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                card_rows,
            )

    return len(users), len(card_rows)


def transaction_row(
    *,
    card: dict,
    amount_usd: float,
    channel: str,
    billing_zone: int,
    billing_country: int,
    email_purchaser: str,
    email_recipient: str,
    device_info: str,
    device_type: str,
    os_raw: str,
    browser_raw: str,
    screen_resolution: str,
    created_at: datetime,
) -> tuple:
    """Assemble a transaction tuple in the column order used for bulk insertion.

    Generates a fresh transaction id and copies the card's ``user_id``/``id`` along
    with the supplied transaction attributes into the positional tuple expected by
    :func:`insert_transaction_rows`.
    """
    return (
        uuid4().hex,
        card["user_id"],
        card["id"],
        amount_usd,
        channel,
        billing_zone,
        billing_country,
        email_purchaser,
        email_recipient,
        device_info,
        device_type,
        os_raw,
        browser_raw,
        screen_resolution,
        created_at,
    )


async def insert_transaction_rows(conn: asyncpg.Connection, rows: list[tuple]) -> None:
    """Bulk-insert transaction tuples into ``application.transactions`` (no-op if empty)."""
    if not rows:
        return
    await conn.executemany(
        """
        INSERT INTO application.transactions
            (
                id, user_id, card_id, amount_usd, channel,
                billing_zone, billing_country, email_purchaser,
                email_recipient, device_info, device_type, os_raw,
                browser_raw, screen_resolution, created_at
            )
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        """,
        rows,
    )


async def seed_transactions(
    database: PostgresDatabase,
    *,
    transaction_count: int,
    anomaly_count: int,
    seed: int,
    replace: bool,
    days_ago: int = 0,
) -> tuple[int, int]:
    """Seed historical transactions, including a small set of crafted anomalies.

    Generates ``transaction_count`` total transactions for the fake users' cards:
    ``transaction_count - anomaly_count`` normal transactions (with realistic
    amounts, billing and device profiles) plus ``anomaly_count`` anomalous ones.
    For each anomalous card it first inserts three "normal" history transactions
    (at ~28/14/3 days ago) and then a single high-risk transaction (large amount,
    foreign billing, rooted/old device, throwaway recipient email). Anomalous
    cards are sampled only from cards older than 35 days.

    Args:
        database: Open Postgres database wrapper.
        transaction_count: Total number of transactions to seed.
        anomaly_count: Number of anomalous cards/transactions to craft.
        seed: RNG seed for reproducible generation.
        replace: If true, delete existing transactions for these cards first.
        days_ago: Shift the effective "now" back by this many days.

    Returns:
        A tuple ``(normal_count, anomaly_count)``.

    Raises:
        ValueError: If ``anomaly_count`` is not strictly less than
            ``transaction_count``, or if there is not enough room to keep at least
            three normal history transactions per anomaly.
        RuntimeError: If fewer than ``anomaly_count`` cards are older than 35 days.
    """
    if anomaly_count >= transaction_count:
        raise ValueError("--anomaly-count must be lower than --transaction-count")

    rng = random.Random(seed)
    now = local_now(days_ago).replace(microsecond=0)
    normal_count = transaction_count - anomaly_count
    minimum_history_count = anomaly_count * 3
    if normal_count < minimum_history_count:
        raise ValueError(
            "--transaction-count must leave at least 3 normal history transactions per anomaly"
        )

    async with database.transaction() as conn:
        cards = await conn.fetch(
            """
            SELECT
                c.id, c.user_id, c.issuer_code, c.country, c.brand, c.type, c.bin_code,
                c.created_at AS card_created_at, u.email
            FROM application.cards c
            JOIN application.users u ON u.id = c.user_id
            WHERE u.email LIKE $1
            ORDER BY c.id
            """,
            FAKE_EMAIL_PATTERN,
        )

        eligible_anomaly_cards = [
            card
            for card in cards
            if card["card_created_at"] <= now - timedelta(days=35)
        ]
        if len(eligible_anomaly_cards) < anomaly_count:
            raise RuntimeError(
                f"Need at least {anomaly_count} cards older than 35 days, "
                f"found {len(eligible_anomaly_cards)} eligible cards"
            )

        card_ids = [card["id"] for card in cards]
        if replace:
            await conn.execute(
                """
                DELETE FROM application.transactions
                WHERE card_id = ANY($1)
                """,
                card_ids,
            )

        anomaly_cards = rng.sample(eligible_anomaly_cards, anomaly_count)
        anomaly_card_ids = {card["id"] for card in anomaly_cards}
        normal_cards = [card for card in cards if card["id"] not in anomaly_card_ids]
        if not normal_cards:
            normal_cards = cards

        rows: list[tuple] = []
        for card in anomaly_cards:
            device_type, os_raw, browser_raw, screen_resolution = card_device_profile(
                card["id"], rng
            )
            for days_ago in (28, 14, 3):
                billing_country, billing_zone = normal_billing(int(card["country"]), rng)
                created_at = max(
                    card["card_created_at"] + timedelta(hours=1),
                    now - timedelta(days=days_ago, hours=rng.randint(0, 12)),
                )
                rows.append(
                    transaction_row(
                        card=card,
                        amount_usd=normal_amount_usd(rng),
                        channel=rng.choice(CHANNELS),
                        billing_zone=billing_zone,
                        billing_country=billing_country,
                        email_purchaser=card["email"],
                        email_recipient=card["email"],
                        device_info=f"{device_type}:{os_raw}:{browser_raw}",
                        device_type=device_type,
                        os_raw=os_raw,
                        browser_raw=browser_raw,
                        screen_resolution=screen_resolution,
                        created_at=created_at,
                    )
                )

        while len(rows) < normal_count:
            card = rng.choice(normal_cards)
            device_type, os_raw, browser_raw, screen_resolution = card_device_profile(
                card["id"], rng
            )
            billing_country, billing_zone = normal_billing(int(card["country"]), rng)
            rows.append(
                transaction_row(
                    card=card,
                    amount_usd=normal_amount_usd(rng),
                    channel=rng.choice(CHANNELS),
                    billing_zone=billing_zone,
                    billing_country=billing_country,
                    email_purchaser=card["email"],
                    email_recipient=card["email"],
                    device_info=f"{device_type}:{os_raw}:{browser_raw}",
                    device_type=device_type,
                    os_raw=os_raw,
                    browser_raw=browser_raw,
                    screen_resolution=screen_resolution,
                    created_at=transaction_created_at(card["card_created_at"], rng, now),
                )
            )

        rng.shuffle(rows)
        await insert_transaction_rows(conn, rows)

        for index, card in enumerate(anomaly_cards, start=1):
            billing_country, billing_zone = ABNORMAL_BILLING[(index - 1) % len(ABNORMAL_BILLING)]
            if billing_country == card["country"]:
                billing_country, billing_zone = 643, 4

            await conn.execute(
                """
                INSERT INTO application.transactions
                    (
                        id, user_id, card_id, amount_usd, channel,
                        billing_zone, billing_country, email_purchaser,
                        email_recipient, device_info, device_type, os_raw,
                        browser_raw, screen_resolution, created_at
                )
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                uuid4().hex,
                card["user_id"],
                card["id"],
                round(rng.uniform(1800, 9500), 2),
                "mobile_app",
                billing_zone,
                billing_country,
                card["email"],
                f"cashout.{card['id']}@protonmail.com",
                "UnknownRootedAndroid X999",
                "mobile",
                "Android 4.4",
                "unknown browser 0.1",
                "720x1280",
                now - timedelta(minutes=anomaly_count - index),
            )

    return normal_count, anomaly_count


def iso_local(dt: datetime) -> str:
    """Return the ISO-8601 string of ``dt`` rendered in Ho Chi Minh local time."""
    return to_local_time(dt).isoformat()


def email_domain(email: str) -> str:
    """Return the lowercase domain part of an email address (or the whole value if no ``@``)."""
    return email.split("@", 1)[1].lower() if "@" in email else email.lower()


def issuer_numeric(issuer_code_value: str) -> float:
    """Extract the digits from an issuer code and return them as a float (0 if none)."""
    digits = "".join(ch for ch in str(issuer_code_value) if ch.isdigit())
    return float(digits or 0)


def model_channel(channel: str | None) -> str:
    """Map a stored channel name to its single-letter model code, defaulting to ``"W"``."""
    return CHANNEL_TO_MODEL.get(str(channel), "W")


def history_row(tx: dict, card: dict, history_index: int) -> dict:
    """Build a model-feature dictionary representing a normal "history" transaction.

    Combines transaction and card fields into the flat feature schema (numeric
    identifiers, normalized email domains, card-age/recency proxies, and benign
    C/D/M feature values) used by downstream model tooling.

    Args:
        tx: Transaction row mapping.
        card: Owning card row mapping.
        history_index: Sequence index used to populate the ``C13`` count feature.

    Returns:
        A dict of feature name to value for one normal transaction.
    """
    card_age_days = max((tx["created_at"].date() - card["card_created_at"].date()).days, 0)
    return {
        "tx_id": numeric_uuid(tx["id"]),
        "event_timestamp": iso_local(tx["created_at"]),
        "amount_usd": float(tx["amount_usd"]),
        "channel": model_channel(tx["channel"]),
        "card_id": numeric_uuid(card["id"]),
        "issuer_code": issuer_numeric(card["issuer_code"]),
        "card_country": float(card["country"]),
        "card_brand": card["brand"],
        "bin_code": float(card["bin_code"]),
        "card_type": card["type"],
        "billing_zone": float(tx["billing_zone"]),
        "billing_country": float(tx["billing_country"]),
        "email_purchaser": email_domain(tx["email_purchaser"]),
        "email_recipient": email_domain(tx["email_recipient"]),
        "card_age_days": int(card_age_days),
        "days_since_last_tx": int(card_age_days),
        "device_type": tx["device_type"],
        "device_info": tx["device_info"],
        "os_raw": tx["os_raw"],
        "browser_raw": tx["browser_raw"],
        "screen_resolution": tx["screen_resolution"],
        "C1": 1,
        "C2": 1,
        "C4": 0,
        "C5": 0,
        "C6": 1,
        "C7": 0,
        "C8": 0,
        "C9": 1,
        "C10": 0,
        "C11": 1,
        "C12": 0,
        "C13": history_index,
        "C14": 1,
        "D1": int(card_age_days),
        "D2": int(card_age_days),
        "D3": int(card_age_days),
        "D4": int(card_age_days),
        "D5": int(card_age_days),
        "D8": 0,
        "D10": int(card_age_days),
        "D11": int(card_age_days),
        "D15": int(card_age_days),
        "M1": "T",
        "M2": "T",
        "M3": "T",
        "M4": "M2",
        "M5": "F",
        "M6": "F",
        "M7": "T",
        "M8": "T",
        "M9": "T",
    }


def anomaly_row(tx: dict, card: dict, history_count: int) -> dict:
    """Build a model-feature dictionary representing an anomalous transaction.

    Mirrors :func:`history_row` but uses the actual time since the card's last
    transaction for the recency feature and emits inflated/high-risk C/D/M
    feature values characteristic of fraudulent activity.

    Args:
        tx: Transaction row mapping.
        card: Owning card row mapping (expects ``last_tx_at`` and ``card_created_at``).
        history_count: Prior transaction count used to derive the ``C13`` feature.

    Returns:
        A dict of feature name to value for one anomalous transaction.
    """
    last_tx_at = card["last_tx_at"] or card["card_created_at"]
    card_age_days = max((tx["created_at"].date() - card["card_created_at"].date()).days, 0)
    days_since_last_tx = max((tx["created_at"] - last_tx_at).total_seconds() / 86400, 0.0)
    return {
        "tx_id": numeric_uuid(tx["id"]),
        "event_timestamp": iso_local(tx["created_at"]),
        "amount_usd": float(tx["amount_usd"]),
        "channel": model_channel(tx["channel"]),
        "card_id": numeric_uuid(card["id"]),
        "issuer_code": issuer_numeric(card["issuer_code"]),
        "card_country": float(card["country"]),
        "card_brand": card["brand"],
        "bin_code": float(card["bin_code"]),
        "card_type": card["type"],
        "billing_zone": float(tx["billing_zone"]),
        "billing_country": float(tx["billing_country"]),
        "email_purchaser": email_domain(tx["email_purchaser"]),
        "email_recipient": email_domain(tx["email_recipient"]),
        "card_age_days": int(card_age_days),
        "days_since_last_tx": round(float(days_since_last_tx), 4),
        "device_type": tx["device_type"],
        "device_info": tx["device_info"],
        "os_raw": tx["os_raw"],
        "browser_raw": tx["browser_raw"],
        "screen_resolution": tx["screen_resolution"],
        "C1": 45,
        "C2": 39,
        "C4": 14,
        "C5": 0,
        "C6": 31,
        "C7": 12,
        "C8": 17,
        "C9": 1,
        "C10": 20,
        "C11": 34,
        "C12": 13,
        "C13": max(90, history_count + 80),
        "C14": 25,
        "D1": int(card_age_days),
        "D2": int(card_age_days),
        "D3": 0,
        "D4": 1,
        "D5": 0,
        "D8": 0,
        "D10": 1,
        "D11": 1,
        "D15": 1,
        "M1": "F",
        "M2": "F",
        "M3": "F",
        "M4": "M0",
        "M5": "T",
        "M6": "T",
        "M7": "F",
        "M8": "F",
        "M9": "F",
    }


async def init_application_schema(database: PostgresDatabase) -> None:
    """Execute the DDL that (re)creates the ``application`` schema and its tables."""
    await database.execute(APPLICATION_DDL)


async def run_init_db() -> int:
    """Initialize only the Postgres ``application`` schema and return an exit code."""
    database = PostgresDatabase.from_env()
    await database.open()
    try:
        await init_application_schema(database)
        print("Initialized Postgres schema: application")
        return 0
    finally:
        await database.close()


async def run_all(args: argparse.Namespace) -> int:
    """Run the full pipeline: build schema, then seed users, cards and transactions.

    Validates the count arguments, opens a Postgres connection, and runs each
    seeding step in turn while printing progress summaries.

    Args:
        args: Parsed CLI arguments controlling counts, seeds, password and day shift.

    Returns:
        Process exit code (0 on success).

    Raises:
        ValueError: If any of the count arguments is not greater than 0.
    """
    if args.user_count <= 0:
        raise ValueError("--user-count must be greater than 0")
    if args.card_count <= 0:
        raise ValueError("--card-count must be greater than 0")
    if args.transaction_count <= 0:
        raise ValueError("--transaction-count must be greater than 0")
    if args.anomaly_count <= 0:
        raise ValueError("--anomaly-count must be greater than 0")

    database = PostgresDatabase.from_env()
    await database.open()
    try:
        await init_application_schema(database)
        print("Initialized Postgres schema: application")

        inserted, total = await seed_fake_users(
            database,
            args.user_count,
            args.password,
            replace=False,
            days_ago=args.days_ago,
        )
        print(
            f"Seeded fake users: inserted={inserted}, total_fake_users={total}, "
            f"domains={len(domains())}"
        )

        users, cards = await seed_cards(
            database,
            count=args.card_count,
            seed=args.card_seed,
            replace=False,
            days_ago=args.days_ago,
        )
        print(f"Seeded cards: users={users}, cards_inserted={cards}")

        normal_transactions, anomalies = await seed_transactions(
            database,
            transaction_count=args.transaction_count,
            anomaly_count=args.anomaly_count,
            seed=args.transaction_seed,
            replace=False,
            days_ago=args.days_ago,
        )
        print(
            f"Seeded transactions: approved={normal_transactions}, "
            f"anomalous_review={anomalies}, total={normal_transactions + anomalies}"
        )
        print(f"Seeded anomalous transactions: inserted={anomalies}")
        print(f"Default fake user password: {args.password}")
        return 0
    finally:
        await database.close()


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the historical-data initialization CLI."""
    parser = argparse.ArgumentParser(
        description=(
            "Initialize the application schema, seed fake users, seed cards, "
            "and seed realistic historical transactions."
        )
    )
    parser.add_argument("--user-count", type=int, default=DEFAULT_USER_COUNT)
    parser.add_argument("--card-count", type=int, default=DEFAULT_CARD_COUNT)
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument("--card-seed", type=int, default=DEFAULT_CARD_SEED)
    parser.add_argument("--transaction-count", type=int, default=DEFAULT_TRANSACTION_COUNT)
    parser.add_argument("--transaction-seed", type=int, default=DEFAULT_TRANSACTION_SEED)
    parser.add_argument("--anomaly-count", type=int, default=DEFAULT_ANOMALY_COUNT)
    parser.add_argument(
        "--days-ago",
        type=int,
        default=0,
        help="Shift the effective 'now' back by N days so all seeded data pre-dates (now - N days).",
    )
    parser.add_argument("--schema-only", action="store_true")
    return parser


async def main_async() -> int:
    """Parse CLI arguments and dispatch to schema-only or full seeding, returning an exit code."""
    args = build_parser().parse_args()
    if args.schema_only:
        return await run_init_db()
    return await run_all(args)


def main() -> int:
    """Synchronous entry point that runs :func:`main_async` via ``asyncio.run``."""
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
