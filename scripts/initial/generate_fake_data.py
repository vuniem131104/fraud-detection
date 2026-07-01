"""Generate a realistic fake fraud-detection dataset in PostgreSQL.

This standalone script (re)creates the six-table ``application`` schema below and
bulk-loads a large, correlated, reproducible synthetic dataset suitable for
training an XGBoost/LightGBM fraud model or prototyping a feature pipeline:

    users -> cards ------\
                          --> transactions --> labels
    devices -------------/
    merchants -----------/

What makes the fraud "sophisticated" (not per-row dice)
------------------------------------------------------
Fraud is generated as **episodes attached to an entity over time**, so the
signal lives in *sequences* and *graphs* rather than a single give-away column:

* **card_testing** -- a burst of 12-45 tiny ($0.2-6) authorisations on ONE card
  across many merchants within minutes; declines ramp up. Creates real velocity.
* **account_takeover** -- an aged account with clean history hits a change-point:
  new (farm) device + foreign/VPN IP, then 1-5 escalating high-value cash-outs.
* **bust_out** -- a card behaves normally for weeks (warm-up), then a single day
  of max-out charges then goes silent. Deliberately domestic + own-device, so it
  is only visible in the temporal pattern (breaks "aged card = safe").
* **fraud_ring** -- a small pool of devices + cash-out emails shared across many
  distinct victims. Pure graph signal (one device -> many users).

Realism knobs (``--difficulty full``):
* **Overlap** -- fraud amounts overlap legit (card-testing is tiny, ATO/bust-out
  sit in the legit tail); only ~30% of fraud shows geo-mismatch (VPN mimics the
  victim country); some fraud rides *aged* cards.
* **Label noise** -- ~10% of true fraud is never labelled (chargeback not filed),
  ~3% is mislabelled legit, and a sliver of legit is disputed (friendly fraud).
  Label source/delay depend on the archetype.

The six-table contract is unchanged -- every pattern emerges from the transaction
stream (shared device/email = repeated values; velocity = derivable).

Usage::

    uv run python scripts/initial/generate_fake_data.py                 # full 300k
    uv run python scripts/initial/generate_fake_data.py --transactions 4000 \
        --users 800 --merchants 120 --devices 700                       # smoke test
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import asyncpg
import numpy as np

HCM_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

# --------------------------------------------------------------------------- #
# Schema (exactly the requested six-table contract, schema-qualified)         #
# --------------------------------------------------------------------------- #

DDL = """
CREATE SCHEMA IF NOT EXISTS application;

DROP TABLE IF EXISTS application.labels CASCADE;
DROP TABLE IF EXISTS application.transactions CASCADE;
DROP TABLE IF EXISTS application.cards CASCADE;
DROP TABLE IF EXISTS application.devices CASCADE;
DROP TABLE IF EXISTS application.merchants CASCADE;
DROP TABLE IF EXISTS application.users CASCADE;

-- Users: customer profile information.
CREATE TABLE application.users (
    id                  TEXT PRIMARY KEY,
    email               TEXT NOT NULL UNIQUE,
    country_code        VARCHAR(2) NOT NULL,
    customer_segment    TEXT NOT NULL,          -- normal, premium, vip
    kyc_level           SMALLINT NOT NULL,      -- 0,1,2
    email_verified      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT users_id_check CHECK (id ~ '^[0-9a-f]{32}$')
);

-- Cards: payment cards owned by users.
CREATE TABLE application.cards (
    id                  TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    issuer_code         TEXT NOT NULL,
    country_code        VARCHAR(2) NOT NULL,
    brand               TEXT NOT NULL,          -- Visa, Mastercard, Amex
    type                TEXT NOT NULL,          -- debit, credit
    bin_code            VARCHAR(8) NOT NULL,
    is_virtual          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT cards_user_fk FOREIGN KEY (user_id) REFERENCES application.users(id),
    CONSTRAINT cards_id_check CHECK (id ~ '^[0-9a-f]{32}$')
);

-- Merchants: merchant information for category/risk features.
CREATE TABLE application.merchants (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    category            TEXT NOT NULL,
    country_code        VARCHAR(2) NOT NULL,
    risk_level          SMALLINT NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT merchants_id_check CHECK (id ~ '^[0-9a-f]{32}$')
);

-- Devices: device fingerprints (many transactions may share one device).
CREATE TABLE application.devices (
    id                  TEXT PRIMARY KEY,
    fingerprint         TEXT NOT NULL UNIQUE,
    device_type         TEXT NOT NULL,          -- desktop/mobile/tablet
    os                  TEXT NOT NULL,
    browser             TEXT NOT NULL,
    screen_resolution   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT devices_id_check CHECK (id ~ '^[0-9a-f]{32}$')
);

-- Transactions: raw payment transactions. Intentionally carries NO fraud label.
CREATE TABLE application.transactions (
    id                      TEXT PRIMARY KEY,
    user_id                 TEXT NOT NULL,
    card_id                 TEXT NOT NULL,
    merchant_id             TEXT NOT NULL,
    device_id               TEXT NOT NULL,
    amount_usd              NUMERIC(14,2) NOT NULL,
    currency                VARCHAR(3) NOT NULL,
    channel                 TEXT NOT NULL,      -- web, mobile_app, pos
    billing_country_code    VARCHAR(2) NOT NULL,
    ip_country_code         VARCHAR(2) NOT NULL,
    email_purchaser         TEXT,
    email_recipient         TEXT,
    status                  TEXT NOT NULL,      -- approved, declined
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT transactions_user_fk     FOREIGN KEY (user_id)     REFERENCES application.users(id),
    CONSTRAINT transactions_card_fk     FOREIGN KEY (card_id)     REFERENCES application.cards(id),
    CONSTRAINT transactions_device_fk   FOREIGN KEY (device_id)   REFERENCES application.devices(id),
    CONSTRAINT transactions_merchant_fk FOREIGN KEY (merchant_id) REFERENCES application.merchants(id),
    CONSTRAINT transactions_amount_check CHECK (amount_usd > 0),
    CONSTRAINT transactions_id_check     CHECK (id ~ '^[0-9a-f]{32}$')
);

-- Labels: ground truth, separated because fraud outcomes arrive post-hoc.
CREATE TABLE application.labels (
    transaction_id      TEXT PRIMARY KEY,
    label               SMALLINT NOT NULL,      -- 0 = legitimate, 1 = fraud
    label_source        TEXT NOT NULL,          -- manual_review, chargeback, rule_engine
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT labels_transaction_fk FOREIGN KEY (transaction_id) REFERENCES application.transactions(id),
    CONSTRAINT labels_value_check CHECK (label IN (0,1))
);
"""

POST_LOAD_INDEXES = """
CREATE INDEX transactions_user_created_idx     ON application.transactions (user_id, created_at DESC);
CREATE INDEX transactions_card_created_idx     ON application.transactions (card_id, created_at DESC);
CREATE INDEX transactions_merchant_created_idx ON application.transactions (merchant_id, created_at DESC);
CREATE INDEX transactions_device_idx           ON application.transactions (device_id);
CREATE INDEX transactions_created_idx          ON application.transactions (created_at);
CREATE INDEX cards_user_idx                    ON application.cards (user_id);
CREATE INDEX labels_label_idx                  ON application.labels (label);
"""

# --------------------------------------------------------------------------- #
# Reference pools                                                              #
# --------------------------------------------------------------------------- #

# (ISO-2 country, currency, selection weight) for "home" markets.
HOME_COUNTRIES = [
    ("US", "USD", 34), ("GB", "GBP", 10), ("DE", "EUR", 9), ("FR", "EUR", 7),
    ("CA", "CAD", 6), ("AU", "AUD", 5), ("SG", "SGD", 5), ("JP", "JPY", 5),
    ("KR", "KRW", 4), ("VN", "VND", 8), ("NL", "EUR", 3), ("ES", "EUR", 4),
]
# Countries that show up as fraudulent IP / billing origins (geo-mismatch).
RISK_COUNTRIES = ["RU", "NG", "BR", "MX", "TR", "UA", "ID", "PK", "PH", "RO"]

SEGMENTS = [("normal", 0.80), ("premium", 0.15), ("vip", 0.05)]
CARD_BRANDS = [("Visa", 0.55, "4"), ("Mastercard", 0.33, "5"), ("Amex", 0.12, "3")]
CARD_TYPES = ["credit", "debit"]

# Merchant category -> base risk level (1 low .. 3 high).
MERCHANT_CATEGORIES = {
    "grocery": 1, "restaurant": 1, "utilities": 1, "pharmacy": 1, "fashion": 1,
    "electronics": 2, "travel": 2, "subscription": 2, "digital_goods": 2,
    "gaming": 2, "gambling": 3, "crypto": 3, "money_transfer": 3,
}

# Device type -> (OS choices, browser choices, resolution choices).
DEVICE_PROFILES = {
    "mobile": (
        ["iOS 17", "iOS 16", "Android 14", "Android 13"],
        ["Safari Mobile", "Chrome Mobile", "Samsung Internet"],
        ["390x844", "393x873", "412x915", "360x800"],
    ),
    "desktop": (
        ["Windows 11", "Windows 10", "macOS 14", "Ubuntu 22.04"],
        ["Chrome", "Edge", "Firefox", "Safari"],
        ["1920x1080", "1440x900", "1366x768", "2560x1440"],
    ),
    "tablet": (
        ["iPadOS 17", "Android 14"],
        ["Safari", "Chrome"],
        ["810x1080", "1024x1366", "800x1280"],
    ),
}
DEVICE_TYPE_WEIGHTS = [("mobile", 0.55), ("desktop", 0.40), ("tablet", 0.05)]

# "Just under" round-number amounts fraudsters use to dodge review thresholds.
THRESHOLDS = [199, 299, 499, 999, 1999, 2999, 4999]

HOUR_WEIGHTS = np.array(
    [2, 1, 1, 1, 1, 2, 4, 7, 10, 12, 13, 13, 12, 12, 13, 14, 15, 16, 16, 14, 11, 8, 5, 3],
    dtype=float,
)
HOUR_WEIGHTS /= HOUR_WEIGHTS.sum()
HOUR_CDF = np.cumsum(HOUR_WEIGHTS)

# --- fraud-infrastructure sizing / label-noise rates (full realism) -------- #
RING_DEVICES = 30           # small shared device pool -> strong graph signal
RING_EMAILS = 50            # shared cash-out recipient emails
FAMILY_DEVICES = 200        # legit devices shared by 2-3 users (mild, as noise)
RING_ROUTE_PROB = 0.6       # share of episodes routed through ring infra


# --------------------------------------------------------------------------- #
# Small helpers                                                               #
# --------------------------------------------------------------------------- #

def new_id() -> str:
    """Return a fresh 32-char lowercase hex id matching the ``id`` CHECK constraint."""
    return uuid4().hex


def weighted_choice(rng: random.Random, items: list[tuple]) -> object:
    """Pick the first element of a ``(value, weight, ...)`` tuple by weight."""
    return rng.choices([i[0] for i in items], weights=[i[1] for i in items], k=1)[0]


def load_dotenv(path: Path) -> None:
    """Populate ``os.environ`` from a ``.env`` file for keys not already set."""
    if not path.exists():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _pick_country(rng: random.Random) -> tuple[str, str, int]:
    """Weighted pick of a (country, currency, weight) home market."""
    choice = rng.choices(HOME_COUNTRIES, weights=[w for _, _, w in HOME_COUNTRIES], k=1)[0]
    return choice[0], choice[1], choice[2]


def _pick_brand(rng: random.Random) -> tuple[str, float, str]:
    choice = rng.choices(CARD_BRANDS, weights=[w for _, w, _ in CARD_BRANDS], k=1)[0]
    return choice[0], choice[1], choice[2]


def _email_domain(rng: random.Random) -> str:
    return rng.choice(["gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
                       "icloud.com", "proton.me", "gmx.com", "mail.com"])


def _diurnal_hour(rng: random.Random) -> int:
    """Sample an hour-of-day from the diurnal activity profile."""
    return int(np.searchsorted(HOUR_CDF, rng.random()))


def _time_between(rng: random.Random, start: datetime, end: datetime) -> datetime:
    """Uniform timestamp in ``[start, end]`` (returns ``start`` if the span is empty)."""
    span = (end - start).total_seconds()
    return start if span <= 0 else start + timedelta(seconds=rng.uniform(0, span))


def _channel_for(dtype: str, rng: random.Random) -> str:
    """Pick a plausible channel given the device type."""
    if dtype in ("mobile", "tablet"):
        return rng.choices(["mobile_app", "web", "pos"], weights=[0.75, 0.15, 0.10])[0]
    return rng.choices(["web", "pos", "mobile_app"], weights=[0.70, 0.20, 0.10])[0]


def _near_threshold(rng: random.Random) -> float:
    """Amount snapped to just under a round review threshold."""
    return float(rng.choice(THRESHOLDS) - rng.uniform(1, 6))


# --------------------------------------------------------------------------- #
# In-memory entity records                                                     #
# --------------------------------------------------------------------------- #

@dataclass
class User:
    id: str
    email: str
    country: str
    currency: str
    segment: str
    created_at: datetime
    spend_mu: float           # lognormal mu for this user's typical amount


@dataclass
class Card:
    id: str
    user_idx: int
    created_at: datetime


@dataclass
class Merchant:
    id: str
    country: str
    risk: int


# --------------------------------------------------------------------------- #
# Entity generators                                                            #
# --------------------------------------------------------------------------- #

def generate_users(n: int, rng: random.Random, now: datetime) -> tuple[list[User], list[tuple]]:
    """Build ``n`` users plus their COPY-ready rows."""
    users: list[User] = []
    rows: list[tuple] = []
    seg_bonus = {"normal": 0.0, "premium": 0.5, "vip": 1.1}
    for i in range(n):
        country, currency, _ = _pick_country(rng)
        segment = weighted_choice(rng, SEGMENTS)
        if segment == "vip":
            kyc = 2
        elif segment == "premium":
            kyc = rng.choices([1, 2], weights=[0.4, 0.6])[0]
        else:
            kyc = rng.choices([0, 1, 2], weights=[0.35, 0.5, 0.15])[0]
        email_verified = rng.random() < (0.99 if segment != "normal" else 0.82)
        age_days = rng.uniform(1, 30) if rng.random() < 0.2 else rng.uniform(30, 900)
        created = now - timedelta(days=age_days, seconds=rng.randrange(86_400))
        spend_mu = rng.uniform(2.9, 3.9) + seg_bonus[segment]  # ~$18-130 typical
        email = f"user{i:06d}@{_email_domain(rng)}"
        uid = new_id()
        users.append(User(uid, email, country, currency, segment, created, spend_mu))
        rows.append((uid, email, country, segment, kyc, email_verified, created))
    return users, rows


def generate_devices(n: int, rng: random.Random, now: datetime) -> list[tuple]:
    """Build ``n`` device fingerprints as COPY-ready rows."""
    rows: list[tuple] = []
    for _ in range(n):
        dtype = weighted_choice(rng, DEVICE_TYPE_WEIGHTS)
        os_list, browsers, resolutions = DEVICE_PROFILES[dtype]
        resolution = rng.choice(resolutions) if rng.random() > 0.05 else None
        created = now - timedelta(days=rng.uniform(0, 900), seconds=rng.randrange(86_400))
        rows.append((new_id(), f"fp_{uuid4().hex}", dtype, rng.choice(os_list),
                     rng.choice(browsers), resolution, created))
    return rows


def generate_merchants(n: int, rng: random.Random, now: datetime) -> tuple[list[Merchant], list[tuple]]:
    """Build ``n`` merchants plus COPY-ready rows."""
    merchants: list[Merchant] = []
    rows: list[tuple] = []
    categories = list(MERCHANT_CATEGORIES.items())
    for i in range(n):
        category, base_risk = rng.choice(categories)
        risk = min(3, base_risk + (1 if rng.random() < 0.1 else 0))
        country, _, _ = _pick_country(rng)
        mid = new_id()
        created = now - timedelta(days=rng.uniform(30, 1200), seconds=rng.randrange(86_400))
        name = f"{category.replace('_', ' ').title()} {i:04d}"
        merchants.append(Merchant(mid, country, risk))
        rows.append((mid, name, category, country, risk, created))
    return merchants, rows


def generate_cards(
    users: list[User], rng: random.Random, now: datetime
) -> tuple[list[Card], list[list[int]], list[tuple]]:
    """Assign 1-3 cards per user; return flat cards, per-user indices and COPY rows."""
    cards: list[Card] = []
    per_user: list[list[int]] = [[] for _ in users]
    rows: list[tuple] = []
    for uidx, user in enumerate(users):
        n_cards = rng.choices([1, 2, 3], weights=[0.92, 0.06, 0.02])[0]
        for _ in range(n_cards):
            brand, _, prefix = _pick_brand(rng)
            card_type = "credit" if brand == "Amex" else rng.choice(CARD_TYPES)
            bin_code = prefix + "".join(str(rng.randint(0, 9)) for _ in range(5))
            is_virtual = rng.random() < 0.08
            max_age = max((now - user.created_at).days - 1, 1)
            age = rng.uniform(1, min(30, max_age)) if rng.random() < 0.2 \
                else rng.uniform(1, min(720, max_age))
            created = now - timedelta(days=age, seconds=rng.randrange(86_400))
            if created <= user.created_at:
                created = user.created_at + timedelta(hours=1)
            country = user.country if rng.random() < 0.95 else _pick_country(rng)[0]
            issuer = f"ISS-{country}-{rng.randint(0, 96):02d}"
            cid = new_id()
            cards.append(Card(cid, uidx, created))
            per_user[uidx].append(len(cards) - 1)
            rows.append((cid, user.id, issuer, country, brand, card_type,
                         bin_code, is_virtual, created))
    return cards, per_user, rows


# --------------------------------------------------------------------------- #
# Transaction + label generation (episode-based fraud)                        #
# --------------------------------------------------------------------------- #

_UNSET = object()


def generate_transactions(
    n: int,
    users: list[User],
    cards: list[Card],
    per_user_cards: list[list[int]],
    merchants: list[Merchant],
    device_ids: list[str],
    device_types: list[str],
    rng: random.Random,
    nprng: np.random.Generator,
    now: datetime,
    days: int,
    fraud_rate: float,
    label_cutoff_days: int,
    patterns: list[str],
    difficulty: str,
) -> tuple[list[tuple], list[tuple], dict]:
    """Generate ``n`` transactions (legit background + fraud episodes) and labels.

    Fraud is emitted as entity/time episodes so velocity and graph signals are
    real. Returns ``(tx_rows, label_rows, stats)``.
    """
    n_users, n_merch, n_dev = len(users), len(merchants), len(device_ids)
    full = difficulty == "full"

    # Long-tail user activity + Pareto "whale" merchants.
    user_weights = nprng.lognormal(0.0, 1.15, n_users); user_weights /= user_weights.sum()
    merch_weights = nprng.pareto(1.16, n_merch) + 0.05; merch_weights /= merch_weights.sum()

    # Fraud infrastructure vs benign shared ("family") devices.
    ring_dev = [int(x) for x in nprng.choice(n_dev, size=min(RING_DEVICES, n_dev), replace=False)]
    ring_set = set(ring_dev)
    non_ring = [i for i in range(n_dev) if i not in ring_set]
    family_dev = [int(x) for x in nprng.choice(non_ring, size=min(FAMILY_DEVICES, len(non_ring)), replace=False)]
    ring_emails = [f"cashout{i:03d}@proton.me" for i in range(RING_EMAILS)]

    # Each user's habitual device (a few share a family device).
    home_device = [rng.choice(family_dev) if rng.random() < 0.03 else rng.choice(non_ring)
                   for _ in range(n_users)]

    now_minus_60 = now - timedelta(days=60)
    eligible_ato = [u for u in range(n_users)
                    if any(cards[c].created_at <= now_minus_60 for c in per_user_cards[u])] or list(range(n_users))

    tx_rows: list[tuple] = []
    flags: list[tuple] = []           # parallel (is_fraud, archetype)
    arch_counts: dict[str, int] = {}
    episode_counts: dict[str, int] = {}

    def emit(*, user, card, merch, device_idx, amount, created, is_fraud, archetype=None,
             billing=None, ip=None, purchaser=_UNSET, recipient=_UNSET, status=None):
        billing = billing or user.country
        ip = ip or user.country
        if purchaser is _UNSET:
            purchaser = user.email if rng.random() > 0.02 else None
        if recipient is _UNSET:
            recipient = purchaser if rng.random() < 0.5 else None
        if status is None:
            status = "declined" if rng.random() < 0.055 else "approved"
        # keep card.created_at < created <= now, jittered (never a fixed collision value)
        if created <= card.created_at:
            created = card.created_at + timedelta(seconds=rng.uniform(1, 120))
        elif created > now:
            created = now - timedelta(seconds=rng.uniform(1, 1800))
        device_id = device_ids[device_idx]
        channel = _channel_for(device_types[device_idx], rng)
        tx_rows.append((
            new_id(), user.id, card.id, merch.id, device_id, Decimal(f"{max(amount, 0.5):.2f}"),
            user.currency, channel, billing, ip, purchaser, recipient, status, created,
        ))
        flags.append((is_fraud, archetype))
        if is_fraud:
            arch_counts[archetype] = arch_counts.get(archetype, 0) + 1

    def legit_amount(user: User) -> float:
        a = rng.lognormvariate(user.spend_mu, 0.6)
        if rng.random() < 0.05:                    # occasional big legit buy (overlap)
            a *= rng.uniform(3, 10)
        return float(min(a, 9000.0))

    def fresh_device() -> int:
        return rng.choice(non_ring)

    def episode_device(uidx: int, own_ok: bool) -> int:
        """Device for a fraud episode: ring-shared, victim's own, or fresh."""
        if rng.random() < RING_ROUTE_PROB:
            return rng.choice(ring_dev)
        if own_ok and rng.random() < 0.5:
            return home_device[uidx]
        return fresh_device()

    def fraud_ip(user: User) -> str:
        """IP for a fraud tx: with full realism many fraudsters VPN to the victim country."""
        mismatch_p = 0.30 if full else 0.65
        return rng.choice(RISK_COUNTRIES) if rng.random() < mismatch_p else user.country

    # ----------------------------- episodes -------------------------------- #
    def card_testing() -> int:
        uidx = int(nprng.choice(n_users, p=user_weights))
        card = cards[rng.choice(per_user_cards[uidx])]
        user = users[uidx]
        dev = episode_device(uidx, own_ok=False)
        base = _time_between(rng, max(card.created_at, now - timedelta(days=days)), now - timedelta(hours=2))
        k = rng.randint(12, 45)
        t = base
        for j in range(k):
            t = t + timedelta(seconds=rng.uniform(4, 90))
            ramp = min(0.85, 0.08 + j * 0.03)      # declines climb as issuer reacts
            emit(user=user, card=card, merch=merchants[rng.randrange(n_merch)], device_idx=dev,
                 amount=round(rng.uniform(0.2, 6.0), 2), created=t, is_fraud=True,
                 archetype="card_testing", ip=fraud_ip(user), recipient=user.email,
                 status="declined" if rng.random() < ramp else "approved")
        return k

    def account_takeover() -> int:
        uidx = rng.choice(eligible_ato)
        user = users[uidx]
        aged = [c for c in per_user_cards[uidx] if cards[c].created_at <= now_minus_60] or per_user_cards[uidx]
        card = cards[rng.choice(aged)]
        changepoint = _time_between(rng, max(card.created_at + timedelta(days=20), now - timedelta(days=30)),
                                    now - timedelta(hours=1))
        # Clean history BEFORE the takeover (home device, home country).
        for _ in range(rng.randint(3, 7)):
            ht = _time_between(rng, card.created_at + timedelta(hours=1), changepoint)
            emit(user=user, card=card, merch=merchants[int(nprng.choice(n_merch, p=merch_weights))],
                 device_idx=home_device[uidx], amount=legit_amount(user), created=ht, is_fraud=False)
        # The takeover: new device + foreign/VPN IP, escalating cash-outs.
        dev = episode_device(uidx, own_ok=False)
        recipient = (rng.choice(ring_emails) if rng.random() < 0.5
                     else f"cashout+{uuid4().hex[:8]}@proton.me")
        typical = math.exp(user.spend_mu)
        k = rng.randint(1, 5)
        t = changepoint
        for j in range(k):
            t = t + timedelta(minutes=rng.uniform(3, 180))
            amt = _near_threshold(rng) if rng.random() < 0.4 else typical * rng.uniform(6, 25) * (1 + 0.4 * j)
            emit(user=user, card=card, merch=merchants[rng.randrange(n_merch)], device_idx=dev,
                 amount=min(amt, 15000.0), created=t, is_fraud=True, archetype="account_takeover",
                 ip=fraud_ip(user), billing=user.country if rng.random() < 0.7 else rng.choice(RISK_COUNTRIES),
                 recipient=recipient, status="declined" if rng.random() < 0.25 else "approved")
        return k

    def bust_out() -> int:
        uidx = int(nprng.choice(n_users, p=user_weights))
        user = users[uidx]
        card = cards[rng.choice(per_user_cards[uidx])]
        # Warm-up: normal, domestic spend over prior weeks builds "trust".
        warm_start = max(card.created_at + timedelta(hours=1), now - timedelta(days=rng.randint(21, 45)))
        bust_day = _time_between(rng, warm_start + timedelta(days=10), now - timedelta(hours=2))
        for _ in range(rng.randint(4, 9)):
            emit(user=user, card=card, merch=merchants[int(nprng.choice(n_merch, p=merch_weights))],
                 device_idx=home_device[uidx], amount=legit_amount(user),
                 created=_time_between(rng, warm_start, bust_day), is_fraud=False)
        # Bust: same day max-out. Domestic + own device -> only temporal signal.
        dev = home_device[uidx] if rng.random() < 0.7 else episode_device(uidx, own_ok=True)
        k = rng.randint(4, 10)
        t = bust_day
        for j in range(k):
            t = t + timedelta(minutes=rng.uniform(2, 40))
            amt = _near_threshold(rng) if rng.random() < 0.3 else rng.uniform(600, 5000)
            emit(user=user, card=card, merch=merchants[rng.randrange(n_merch)], device_idx=dev,
                 amount=amt, created=t, is_fraud=True, archetype="bust_out",
                 ip=user.country if rng.random() < 0.85 else rng.choice(RISK_COUNTRIES),
                 recipient=user.email, status="declined" if rng.random() < (0.1 + 0.06 * j) else "approved")
        return k

    def fraud_ring() -> int:
        """One shared device + cash-out email hit across many distinct victims."""
        dev = rng.choice(ring_dev)
        email = rng.choice(ring_emails)
        emitted = 0
        base = _time_between(rng, now - timedelta(days=days), now - timedelta(days=2))
        for _ in range(rng.randint(5, 15)):
            uidx = int(nprng.choice(n_users, p=user_weights))
            user = users[uidx]
            card = cards[rng.choice(per_user_cards[uidx])]
            for _ in range(rng.randint(1, 2)):
                t = _time_between(rng, base, min(base + timedelta(days=3), now))
                emit(user=user, card=card, merch=merchants[rng.randrange(n_merch)], device_idx=dev,
                     amount=_near_threshold(rng) if rng.random() < 0.3 else rng.uniform(50, 1500),
                     created=t, is_fraud=True,
                     archetype="fraud_ring", ip=fraud_ip(user), recipient=email,
                     status="declined" if rng.random() < 0.3 else "approved")
                emitted += 1
        return emitted

    dispatch = {"card_testing": card_testing, "account_takeover": account_takeover,
                "bust_out": bust_out, "fraud_ring": fraud_ring}
    enabled = [p for p in patterns if p in dispatch] or list(dispatch)
    ep_weight = {"card_testing": 0.30, "account_takeover": 0.34, "bust_out": 0.14, "fraud_ring": 0.22}

    fraud_target = int(n * fraud_rate)
    fraud_emitted = 0
    guard = 0
    while fraud_emitted < fraud_target and guard < fraud_target * 5 + 1000:
        guard += 1
        pattern = rng.choices(enabled, weights=[ep_weight[p] for p in enabled], k=1)[0]
        fraud_emitted += dispatch[pattern]()
        episode_counts[pattern] = episode_counts.get(pattern, 0) + 1

    # --------------------------- legit background -------------------------- #
    remaining = max(n - len(tx_rows), 0)
    if remaining:
        user_pick = nprng.choice(n_users, size=remaining, p=user_weights)
        merch_pick = nprng.choice(n_merch, size=remaining, p=merch_weights)
        for k in range(remaining):
            uidx = int(user_pick[k])
            user = users[uidx]
            card = cards[rng.choice(per_user_cards[uidx])]
            r = rng.random()
            dev = home_device[uidx] if r < 0.85 else (rng.choice(family_dev) if r < 0.87 else fresh_device())
            ip = user.country
            if rng.random() < 0.03:                      # legit travel / VPN noise
                ip = _pick_country(rng)[0] if rng.random() < 0.6 else rng.choice(RISK_COUNTRIES)
            emit(user=user, card=card, merch=merchants[int(merch_pick[k])], device_idx=dev,
                 amount=legit_amount(user), created=_draw_created_at(now, card.created_at, rng, days),
                 is_fraud=False, ip=ip)

    # ------------------------------- labels -------------------------------- #
    label_cutoff = now - timedelta(days=label_cutoff_days)
    label_rows: list[tuple] = []
    unlabeled_fraud = mislabeled_fraud = friendly = labeled_fraud = 0
    src_by_arch = {
        "card_testing": ("rule_engine", 0.05, 2.0),
        "account_takeover": ("chargeback", 7.0, 30.0),
        "bust_out": ("chargeback", 14.0, 55.0),
        "fraud_ring": ("chargeback", 5.0, 40.0),
    }
    for i, (is_fraud, arch) in enumerate(flags):
        created = tx_rows[i][13]
        if created > label_cutoff:                       # too recent to have ground truth
            continue
        tx_id = tx_rows[i][0]
        max_delay = max((now - created).days - 0.1, 0.1)
        if is_fraud:
            roll = rng.random()
            if full and roll < 0.10:                     # never caught -> no label row
                unlabeled_fraud += 1
                continue
            if full and roll < 0.13:                     # missed -> mislabelled legit
                mislabeled_fraud += 1
                label_rows.append((tx_id, 0, "rule_engine",
                                   created + timedelta(days=min(rng.uniform(0.1, 5), max_delay))))
                continue
            src, lo, hi = src_by_arch.get(arch, ("chargeback", 5.0, 40.0))
            if rng.random() < 0.15:
                src = "manual_review"
            labeled_fraud += 1
            label_rows.append((tx_id, 1, src, created + timedelta(days=min(rng.uniform(lo, hi), max_delay))))
        else:
            if full and rng.random() < 0.0006:           # friendly fraud on legit
                friendly += 1
                label_rows.append((tx_id, 1, "chargeback",
                                   created + timedelta(days=min(rng.uniform(10, 50), max_delay))))
            else:
                src = rng.choices(["rule_engine", "manual_review"], weights=[0.85, 0.15])[0]
                label_rows.append((tx_id, 0, src, created + timedelta(days=min(rng.uniform(0.1, 7), max_delay))))

    stats = {
        "true_fraud": sum(1 for f in flags if f[0]),
        "labeled_fraud": labeled_fraud,
        "unlabeled_fraud": unlabeled_fraud,
        "mislabeled_fraud": mislabeled_fraud,
        "friendly_fraud": friendly,
        "archetypes": arch_counts,
        "episodes": episode_counts,
    }
    return tx_rows, label_rows, stats


def _draw_created_at(now: datetime, floor: datetime, rng: random.Random, days: int) -> datetime:
    """Recency-weighted timestamp *within the card's actual lifetime*.

    Draws inside ``(card.created_at, now]`` (bounded by the ``days`` history
    window), biased toward recent activity, with a diurnal hour profile and full
    sub-second precision. Drawing within the real lifetime — rather than over the
    global window and then clamping — avoids piling a young card's transactions
    into a fixed post-creation window (which produced identical timestamps).
    """
    start = floor + timedelta(minutes=1)               # earliest the card can transact
    if start >= now:                                   # card younger than a minute
        return now - timedelta(seconds=rng.uniform(1, 60))
    bucket = rng.random()                              # recency-weighted over the window
    if bucket < 0.5:
        age = rng.uniform(0, min(30, days))
    elif bucket < 0.85:
        age = rng.uniform(30, min(90, days))
    else:
        age = rng.uniform(90, days)
    created = (now - timedelta(days=age)).replace(
        hour=min(_diurnal_hour(rng), 23), minute=rng.randrange(60),
        second=rng.randrange(60), microsecond=rng.randrange(1_000_000))
    if not (start <= created <= now):                  # predates the card: spread over its real life
        created = start + timedelta(seconds=rng.uniform(0, (now - start).total_seconds()))
    return created


# --------------------------------------------------------------------------- #
# Loading + reporting                                                          #
# --------------------------------------------------------------------------- #

async def copy_rows(conn: asyncpg.Connection, table: str, columns: list[str], rows: list[tuple]) -> None:
    """Bulk-insert ``rows`` into ``application.<table>`` via binary COPY."""
    if rows:
        await conn.copy_records_to_table(table, schema_name="application", columns=columns, records=rows)


async def quality_report(conn: asyncpg.Connection, stats: dict) -> None:
    """Print a data-quality / realism report emphasising the sophisticated signals."""
    print("\n" + "=" * 64)
    print("QUALITY REPORT")
    print("=" * 64)
    for table in ("users", "cards", "merchants", "devices", "transactions", "labels"):
        print(f"  {table:<14}: {await conn.fetchval(f'SELECT COUNT(*) FROM application.{table}'):>9,}")

    fraud = await conn.fetchval("SELECT COUNT(*) FROM application.labels WHERE label = 1")
    total = await conn.fetchval("SELECT COUNT(*) FROM application.labels")
    if total:
        print(f"\n  labeled fraud rate   : {fraud / total:.3%}  ({fraud:,}/{total:,})")
    print(f"  true fraud tx        : {stats['true_fraud']:,}")
    print(f"  label noise          : unlabeled={stats['unlabeled_fraud']:,}, "
          f"mislabeled_legit={stats['mislabeled_fraud']:,}, friendly_fraud={stats['friendly_fraud']:,}")

    share = await conn.fetchval(
        """WITH pm AS (SELECT merchant_id, COUNT(*) c FROM application.transactions GROUP BY 1),
                r AS (SELECT c, NTILE(100) OVER (ORDER BY c DESC) pct FROM pm)
           SELECT SUM(c) FILTER (WHERE pct=1)::float / SUM(c) FROM r""")
    print(f"  top-1% merchant share: {share:.1%}")

    print("\n  --- separation (harder now: signals overlap) ---")
    for r in await conn.fetch(
        """SELECT l.label, AVG((t.billing_country_code<>t.ip_country_code)::int)::float mm,
                  AVG(t.amount_usd)::float amt, AVG((t.status='declined')::int)::float decl
           FROM application.transactions t JOIN application.labels l ON l.transaction_id=t.id
           GROUP BY 1 ORDER BY 1"""):
        tag = "fraud" if r["label"] == 1 else "legit"
        print(f"    {tag}: geo_mismatch={r['mm']:.1%}, avg_amount=${r['amt']:,.2f}, declined={r['decl']:.1%}")

    print("\n  --- VELOCITY (card-testing / bust-out bursts) ---")
    vr = await conn.fetchrow(
        """SELECT MAX(c) mx, AVG(c)::float av FROM (
             SELECT card_id, date_trunc('hour', created_at) h, COUNT(*) c
             FROM application.transactions GROUP BY 1, 2) s""")
    print(f"    max tx by one card in a single hour: {vr['mx']}  (avg per card-hour: {vr['av']:.2f})")
    dm = await conn.fetchval(
        """SELECT MAX(d) FROM (
             SELECT card_id, date_trunc('hour', created_at) h, COUNT(DISTINCT merchant_id) d
             FROM application.transactions GROUP BY 1, 2) s""")
    print(f"    max distinct merchants hit by one card in an hour: {dm}")

    print("\n  --- GRAPH (fraud ring / device farm) ---")
    gr = await conn.fetch(
        """SELECT device_id, COUNT(DISTINCT user_id) u, COUNT(*) tx
           FROM application.transactions GROUP BY 1 ORDER BY u DESC LIMIT 3""")
    for r in gr:
        print(f"    device {r['device_id'][:12]}...: {r['u']} distinct users, {r['tx']} tx")
    re = await conn.fetchval(
        """SELECT MAX(u) FROM (
             SELECT email_recipient, COUNT(DISTINCT user_id) u FROM application.transactions
             WHERE email_recipient IS NOT NULL GROUP BY 1) s""")
    print(f"    max distinct users sharing one recipient email: {re}")

    if stats.get("archetypes"):
        print("\n  fraud tx by archetype (episodes):")
        for name, c in sorted(stats["archetypes"].items(), key=lambda x: -x[1]):
            print(f"    {name:<20}: {c:,} tx across {stats['episodes'].get(name, 0)} episodes")
    print("=" * 64)


# --------------------------------------------------------------------------- #
# Orchestration                                                               #
# --------------------------------------------------------------------------- #

async def run(args: argparse.Namespace) -> int:
    """Create the schema, generate every table in memory, load and report."""
    rng = random.Random(args.seed)
    nprng = np.random.default_rng(args.seed)
    now = datetime.now(HCM_TZ).replace(microsecond=0)
    patterns = [p.strip() for p in args.patterns.split(",") if p.strip()]

    print("Generating entities in memory ...")
    users, user_rows = generate_users(args.users, rng, now)
    device_rows = generate_devices(args.devices, rng, now)
    device_ids = [row[0] for row in device_rows]
    device_types = [row[2] for row in device_rows]
    merchants, merchant_rows = generate_merchants(args.merchants, rng, now)
    cards, per_user_cards, card_rows = generate_cards(users, rng, now)
    print(f"  users={len(user_rows):,} devices={len(device_rows):,} "
          f"merchants={len(merchant_rows):,} cards={len(card_rows):,}")

    print(f"Generating ~{args.transactions:,} transactions ({args.difficulty}) "
          f"with patterns={patterns} ...")
    tx_rows, label_rows, stats = generate_transactions(
        args.transactions, users, cards, per_user_cards, merchants, device_ids, device_types,
        rng, nprng, now, args.days, args.fraud_rate, args.label_cutoff_days, patterns, args.difficulty)
    print(f"  transactions={len(tx_rows):,} labels={len(label_rows):,} "
          f"true_fraud={stats['true_fraud']:,} episodes={sum(stats['episodes'].values())}")

    conn = await asyncpg.connect(
        host=os.environ["POSTGRES_HOST"], port=int(os.environ["POSTGRES_PORT"]),
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DB"])
    try:
        print("Creating schema (application.*) ...")
        await conn.execute(DDL)
        print("Loading via COPY ...")
        async with conn.transaction():
            await copy_rows(conn, "users", ["id", "email", "country_code",
                            "customer_segment", "kyc_level", "email_verified", "created_at"], user_rows)
            await copy_rows(conn, "devices", ["id", "fingerprint", "device_type",
                            "os", "browser", "screen_resolution", "created_at"], device_rows)
            await copy_rows(conn, "merchants", ["id", "name", "category",
                            "country_code", "risk_level", "created_at"], merchant_rows)
            await copy_rows(conn, "cards", ["id", "user_id", "issuer_code",
                            "country_code", "brand", "type", "bin_code", "is_virtual", "created_at"], card_rows)
            await copy_rows(conn, "transactions", ["id", "user_id", "card_id",
                            "merchant_id", "device_id", "amount_usd", "currency", "channel",
                            "billing_country_code", "ip_country_code", "email_purchaser",
                            "email_recipient", "status", "created_at"], tx_rows)
            await copy_rows(conn, "labels", ["transaction_id", "label",
                            "label_source", "created_at"], label_rows)
        print("Creating indexes + ANALYZE ...")
        await conn.execute(POST_LOAD_INDEXES)
        await conn.execute("ANALYZE application.transactions")
        await quality_report(conn, stats)
    finally:
        await conn.close()
    print("\nDone.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser with production-scale defaults."""
    p = argparse.ArgumentParser(description="Generate sophisticated fake fraud-detection data in Postgres.")
    p.add_argument("--users", type=int, default=25_000)
    p.add_argument("--merchants", type=int, default=1_500)
    p.add_argument("--devices", type=int, default=30_000)
    p.add_argument("--transactions", type=int, default=300_000)
    p.add_argument("--days", type=int, default=180, help="History window in days.")
    p.add_argument("--fraud-rate", type=float, default=0.005,
                   help="Target TRUE fraud fraction of tx (labeled rate ~0.5% after noise).")
    p.add_argument("--difficulty", choices=["full", "moderate"], default="full",
                   help="'full' = realistic overlap + label noise; 'moderate' = more separable.")
    p.add_argument("--patterns", default="card_testing,account_takeover,bust_out,fraud_ring",
                   help="Comma-separated fraud archetypes to enable.")
    p.add_argument("--label-cutoff-days", type=int, default=3,
                   help="Transactions newer than this stay unlabeled (label delay).")
    p.add_argument("--seed", type=int, default=42)
    return p


def main() -> int:
    """Load env, parse args and run the async generator."""
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    return asyncio.run(run(build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
