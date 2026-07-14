# Historical Data Generator

Concrete, reproducible generator that fills a PostgreSQL `application` schema with
a realistic fraud-detection dataset for training and pipeline prototyping.

- **Script**: [`scripts/initial/generate_fake_data.py`](../../scripts/initial/generate_fake_data.py)
- **Default output**: 25k users · 30k devices · 1.5k merchants · ~27.5k cards · **300k transactions** · ~266k labels
- **Loader**: `asyncpg` binary `COPY` — full 300k run in **~17s**
- **Reproducible**: single RNG seed (default `42`)

The headline property: **fraud is generated as episodes attached to an entity over
time**, not as an independent per-row coin flip. The signal therefore lives in
*sequences* (velocity) and *graphs* (shared device/email), which is what a real
fraud model must learn — no single column is a give-away.

---

## 1. Goals & non-goals

Goals:
1. Match a fixed six-table contract exactly (see §2).
2. Guarantee referential + temporal integrity (a tx never predates its card).
3. Produce *learnable but noisy* fraud with sophisticated patterns (§5) so
   feature engineering (velocity, graph, change-point) actually pays off.
4. Be fast, deterministic and CLI-configurable.

Non-goals: streaming/Kafka ingestion, multi-currency FX modelling, PII. Amounts
are USD-normalised (`amount_usd`); `currency` records the original ISO code only.

---

## 2. Schema (six tables)

Created fresh each run (`DROP ... CASCADE` then `CREATE`), schema-qualified under
`application`. Full DDL lives in the script (`DDL` constant).

| Table | Grain | Key columns |
|---|---|---|
| `users` | one per customer | `id` (hex32), `email` (unique), `country_code`, `customer_segment`, `kyc_level`, `email_verified` |
| `cards` | one per card | `id`, `user_id → users`, `issuer_code`, `country_code`, `brand`, `type`, `bin_code`, `is_virtual` |
| `merchants` | one per merchant | `id`, `name`, `category`, `country_code`, `risk_level` |
| `devices` | one per fingerprint | `id`, `fingerprint` (unique), `device_type`, `os`, `browser`, `screen_resolution` |
| `transactions` | one per payment | `id`, `user_id`, `card_id`, `merchant_id`, `device_id`, `amount_usd`, `currency`, `channel`, `billing_country_code`, `ip_country_code`, `email_purchaser`, `email_recipient`, `status` |
| `labels` | ≤ one per tx | `transaction_id → transactions`, `label` (0/1), `label_source` |

Design notes:
- **`labels` is split out on purpose** — fraud ground truth arrives *after* the
  transaction (chargeback/review). Recent transactions are intentionally left
  **unlabeled** (§6).
- Every `id` is a 32-char lowercase hex (`uuid4().hex`), enforced by a `CHECK`.
- `transactions` carries **no label column** — the model must join `labels`.
- Post-load indexes cover the FK + `created_at` access paths used by feature
  engineering; `ANALYZE` runs after load.

---

## 3. Entity model

All distributions are drawn from a seeded `random.Random` + `numpy.default_rng`.

**Users** (`generate_users`)
- `customer_segment`: normal 80% / premium 15% / vip 5%.
- `kyc_level` correlates with segment (vip → 2; normal skews 0–1).
- `email_verified`: 82% (normal) / 99% (premium, vip).
- `spend_mu`: per-user lognormal μ for typical amount (segment-scaled) → each user
  has a stable spend level (≈ $18–130 typical), which makes overlap realistic.
- `created_at`: 20% "new" (≤30d), rest spread up to ~2.5y.

**Cards** (`generate_cards`)
- 1 card 92% / 2 cards 6% / 3 cards 2% per user.
- `brand` Visa 55% / Mastercard 33% / Amex 12%; `bin_code` prefix matches brand;
  Amex ⇒ credit. `is_virtual` ~8%.
- `created_at` after the owning user, mostly aged (≈20% recent).

**Merchants** (`generate_merchants`)
- `category` drawn from an MCC-like set; `risk_level` 1–3 derived from category
  (gambling/crypto/money_transfer high), +1 drift 10% of the time.
- **Whale skew**: transaction volume is assigned by a Pareto weight so the top ~1%
  of merchants take a large share of traffic.

**Devices** (`generate_devices`)
- `device_type` mobile 55% / desktop 40% / tablet 5%; `os`/`browser`/
  `screen_resolution` are consistent with the type; ~5% null resolution.

---

## 4. Transaction model — legit background

The bulk (~298k) is normal traffic (`generate_transactions`, legit fill loop):
- **User** sampled by a long-tail lognormal weight (few users are very active).
- **Merchant** sampled by the Pareto whale weight.
- **Device**: the user's *home* device 85%, a shared *family* device 2%, else a
  random non-ring device — so benign device sharing exists but is small-degree.
- **Amount**: lognormal around the user's `spend_mu`, with a 5% heavy-tail
  multiplier (big legit purchases) so amounts overlap fraud.
- **Geo**: `billing = ip = user.country`; ~3% "travel" noise flips `ip`.
- **Status**: ~5.5% declined (see §7 for the known simplification here).
- `created_at`: recency-weighted over the history window with a diurnal hour
  profile; clamped to `(card.created_at, now]`.

---

## 5. Fraud model — episodes (the sophisticated part)

Fraud is emitted as **episodes** until a target fraud count is reached
(`--fraud-rate`, default targets ~0.5% *labeled* after noise). Episodes are
weighted; each writes several rows that share entity/time/graph structure.

| Pattern | Mechanic | Primary signal it creates |
|---|---|---|
| **card_testing** | 12–45 tiny ($0.2–6) auths on **one card** across many merchants within minutes; declines ramp 8%→85% | **Velocity** (tx/card/hour, distinct merchants/hour) |
| **account_takeover** | Aged account with clean history hits a **change-point**: new device + foreign/VPN IP, then 1–5 escalating cash-outs (often just under a round threshold) to a cash-out email | **Change-point** (device/geo shift vs history) |
| **bust_out** | Card behaves normally for weeks (warm-up), then one day of max-out charges then silence. Deliberately domestic + own device | **Temporal** only (aged card ≠ safe) |
| **fraud_ring** | One shared device + cash-out email hit across **many distinct victims** | **Graph** (users-per-device, users-per-email) |

### 5.1 Fraud infrastructure

To create graph signal, fraud is routed through a small shared pool
(module constants):

- `RING_DEVICES = 30` — shared "device farm"; **never** used by legit traffic, so
  a ring device accumulates many distinct users.
- `RING_EMAILS = 50` — shared cash-out recipient emails.
- `RING_ROUTE_PROB = 0.6` — share of episodes routed through the ring.
- `FAMILY_DEVICES = 200` — benign legit-shared devices (small degree), so "shared
  device" alone is not a perfect fraud flag — the model must learn *degree*.

---

## 6. Realism knobs (`--difficulty full`)

**Overlap** (kills the easy separation):
- Only ~30% of fraud shows geo-mismatch — the rest VPN into the victim country.
- Card-testing amounts are *below* the legit median; ATO/bust-out sit in the
  legit tail. Average fraud amount is a few× legit, not orders of magnitude.
- Bust-out rides *aged* cards, breaking "new card = risky".

**Label noise** (honest ground truth):
- ~10% of true fraud is **never labeled** (chargeback not filed) → no `labels` row.
- ~3% of true fraud is **mislabeled legit** (missed).
- ~0.06% of legit is **friendly fraud** (disputed legit charge → label 1).
- Label `source`/delay depend on archetype: card_testing → `rule_engine` (hours);
  ATO/bust_out/ring → `chargeback` (1–8 weeks); 15% of fraud → `manual_review`.
- Transactions newer than `--label-cutoff-days` (default 3) stay unlabeled.

`--difficulty moderate` keeps more separation (65% fraud mismatch, no label noise)
for an easier first model.

---

## 7. Emergent signals (measured on the default run)

These come from the transaction stream itself — **no extra columns**. Numbers are
from the shipped default (seed 42, 300k, `full`):

```
labeled fraud rate      0.493%   (1,174 / 238,146)
true fraud tx           1,502    (label noise: 111 unlabeled, 34 mislabeled, 121 friendly)
top-1% merchant share   21.8%
```

**Separation (deliberately hard):**

| | geo-mismatch | avg amount | declined |
|---|---|---|---|
| legit | 2.7% | $57 | 5.4% |
| fraud | 27% | $408 | 42.3% |

**Velocity** (card_testing): max **41 tx by one card in a single hour** across
**40 distinct merchants** (avg 1.06 tx/card-hour) — a real burst, not a timestamp
artifact (see §9).

**Graph** (fraud_ring): top device shared across **56 distinct users**; one
recipient email shared across **26 users**.

Verification queries:

```sql
-- velocity: busiest card-hour
SELECT MAX(c) FROM (
  SELECT card_id, date_trunc('hour', created_at) h, COUNT(*) c
  FROM application.transactions GROUP BY 1, 2) s;

-- graph: device shared across the most distinct users
SELECT device_id, COUNT(DISTINCT user_id) u
FROM application.transactions GROUP BY 1 ORDER BY u DESC LIMIT 5;

-- all fraud transactions
SELECT t.* FROM application.transactions t
JOIN application.labels l ON l.transaction_id = t.id
WHERE l.label = 1;
```

---

## 8. `status` is not `label`

`status` (approved/declined) is the **authorization outcome at transaction time**;
`label` (fraud/legit) is **ground truth learned later**. They are correlated, not
equal — confusion matrix on the default run:

| | legit | fraud |
|---|---|---|
| approved | 224,107 | **677** (fraud that got through) |
| declined | **12,865** (false decline) | 497 |

`P(approved | fraud) = 58%`, `P(declined | legit) = 5.4%`.

**Leakage caveat**: for *pre-authorization* scoring the current row's `status`
does not yet exist (the fraud score often *feeds* the approve/decline decision) —
do not use it as a feature. Its real value is in **historical velocity** features
(e.g. `card_declines_1h`). See §9 for a known limitation.

---

## 9. Known simplifications / future work

- **Legit declines are a flat ~5.5% coin flip** with no reason attached — a $5 and
  a $6,000 legit purchase decline at the same rate. Real declines are dominated by
  *insufficient funds* / *do-not-honor* / *expired card* and scale with amount.
  A realistic upgrade would condition declines on amount-vs-typical, card expiry,
  AVS mismatch and velocity, optionally adding a `decline_reason` column.
- **IP is country-level only** (`ip_country_code`) — no IP-address graph; ring
  graph signal is carried by `device_id` and `email_recipient`.
- Timestamps are drawn *within each card's real lifetime* with sub-second
  precision, so there are no duplicate `(card_id, created_at)` rows. Very active
  young cards can still show elevated per-hour counts (a small, legitimate tail).

---

## 10. Usage

```bash
# full 300k, all four patterns, ~0.5% labeled fraud (defaults)
uv run python scripts/initial/generate_fake_data.py

# easier, more separable data for a first model
uv run python scripts/initial/generate_fake_data.py --difficulty moderate

# subset of patterns / different rate / smaller volume
uv run python scripts/initial/generate_fake_data.py --patterns card_testing,fraud_ring
uv run python scripts/initial/generate_fake_data.py --fraud-rate 0.01
uv run python scripts/initial/generate_fake_data.py --users 800 --transactions 4000
```

| Flag | Default | Meaning |
|---|---|---|
| `--users` / `--merchants` / `--devices` / `--transactions` | 25k / 1.5k / 30k / 300k | entity + tx volumes |
| `--days` | 180 | history window |
| `--fraud-rate` | 0.0057 | target **true** fraud fraction (labeled ≈ 0.5% after noise) |
| `--difficulty` | `full` | `full` = overlap + label noise; `moderate` = more separable |
| `--patterns` | all four | comma-separated archetypes to enable |
| `--label-cutoff-days` | 3 | transactions newer than this stay unlabeled |
| `--seed` | 42 | RNG seed (full determinism) |

Connection is read from `.env` (`POSTGRES_*`). Each run prints a quality report
(counts, fraud rate, separation, velocity, graph, archetype breakdown).
