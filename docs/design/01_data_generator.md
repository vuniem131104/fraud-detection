# Phantom Ledger — Data Generator Design

## 1. Domain Overview

Phantom Ledger simulates the transaction stream of a mid-size payment processor (think Stripe / VNPay). The generator produces:

- **Offline historical data** (Parquet on GCS / MinIO): account dimension, merchant dimension, historical transactions, chargebacks.
- **Streaming events** (Avro on Redpanda/Kafka): live transactions and delayed chargeback reports.

It feeds a downstream fraud-detection ML system. To stress that system, the generator deliberately injects skew, bursts, late arrivals, duplicates, label delay, and adversarial drift.

Scope boundaries:
- No real PII. Card PAN is a synthetic 16-digit token, never persisted in clear.
- No actual money movement; chargeback reason codes follow Visa CB groups but values are synthetic.
- Currency in USD only (multi-currency = future work).

---

## 2. Offline Dataset Design

### 2.1 Offline Tables

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `dim_account` | one per account | account_id, kyc_level, country, signup_ts, tier, email_purchaser, billing_zone, billing_country |
| `dim_card` | one per (account × card) | card_id, account_id, card_brand, card_type, card_country, issuer_code, bin_code, first_seen_ts |
| `dim_merchant` | one per merchant | merchant_id, mcc, country, risk_tier, onboarded_ts |
| `dim_device` | one per device fingerprint | device_id, device_type, os_raw, browser_raw, screen_resolution, first_seen_ts |
| `fact_transactions_hist` | one per transaction | tx_id, account_id, card_id, merchant_id, device_id, amount_usd, channel, mcc, event_ts, created_ts, idempotency_key, days_since_last_tx |
| `fact_chargebacks` | one per chargeback | cb_id, tx_id, reason_code, reported_ts, amount_usd |

`dim_card.first_seen_ts` is the source of truth for `card_age_days` at serve time: `card_age_days = (event_ts.date - first_seen_ts.date).days`. The generator must keep this anchor stable across long horizons because it directly drives the `uid2` magic feature in [04.1 §3.3](04.1_ml_design_example.md): `uid2 = card_id + billing_zone + (event_day − card_age_days)`. If `first_seen_ts` drifts for an existing card, `uid2` shifts and the model loses signal — generator unit tests must assert this invariant.

### 2.2 Offline Data Problems

**Compulsory:**
- **Skew**: 1% of merchants account for 40% of transaction volume (whale skew); 80% of accounts have ≤ 5 transactions ("long tail").
- **High cardinality**: account_id, merchant_id, device_id, tx_id mostly unique; card_id × merchant_id join space ~ 10⁸.
- **Schema evolution**: transactions before `2024-01-01` lack `device_id` and `idempotency_key` columns; consumers must default to NULL.

**Optional chosen:** 0.8% duplicate transactions in older partitions (same `idempotency_key` within 60s) representing legacy retry bugs.

**Output**: Parquet partitioned by `event_date`, clustered on `account_id`. Snappy compression.

### 2.3 Label Contract

`fact_chargebacks.reported_ts` is **7–60 days after** `fact_transactions_hist.event_ts`. A transaction is labeled `fraud=1` if any chargeback with reason ∈ {`10.4`, `10.5`, `11.2`, `11.3`} (fraud-family CB groups) is linked to it. Labels are **only stable for transactions older than 60 days**. This delay must be respected by training pipelines.

---

## 3. Streaming Dataset Design

### 3.1 Streaming Topics

Two Kafka topics on Redpanda:

| Topic | Partition Key | Schema | Retention |
|-------|---------------|--------|-----------|
| `tx.events` | account_id | `Transaction` (Avro) | 7 days |
| `cb.events` | tx_id | `Chargeback` (Avro) | 30 days |

`tx.events` schema. The schema is wider than a minimal "transaction event" because it must carry every raw field the ML model consumes; otherwise scoring-api would have nothing to feed the booster (see [04.1 §3.3](04.1_ml_design_example.md)).

```
# Identifiers
tx_id              : string (UUIDv7)
account_id         : string
merchant_id        : string
device_id          : string?                     # nullable, ~5% missing
idempotency_key    : string

# Timing
event_timestamp    : timestamp-millis            # when tx was attempted
created_ts         : timestamp-millis            # when row entered Kafka

# Money
amount_usd         : double
mcc                : string
channel            : enum(WEB, MOBILE_APP, POS, RECURRING)

# Card attributes (model features)
card_id            : string                      # tokenized PAN (HMAC), high-card
card_brand         : enum(visa, mastercard, amex, discover, ...)
card_type          : enum(credit, debit, charge)
card_country       : string                      # ISO-2
issuer_code        : string?                     # bank issuer code (BIN-derived)
bin_code           : string?                     # BIN range
card_age_days      : int                         # days since card first seen at processor

# Address (model features)
billing_zone       : string                      # zip / region
billing_country    : string                      # ISO-2

# Email (model features; uid building)
email_purchaser    : string?                     # domain only, never local-part
email_recipient    : string?                     # domain only

# Identity (~25% coverage; populated when device fingerprint available)
device_type        : enum(desktop, mobile, tablet)?
device_info        : string?                     # raw device descriptor
os_raw             : string?                     # e.g. "iOS 17.4"
browser_raw        : string?                     # e.g. "mobile safari 17.4"
screen_resolution  : string?                     # "WIDTHxHEIGHT"

# Behavioural (model features)
days_since_last_tx : int?                        # nullable for first-ever tx

# Risk
ip_hash            : string                      # HMAC-SHA256 of source IP
```

PII guarantees:
- `card_id` is never the raw PAN — it is a deterministic HMAC token. `pan_last4` may be carried alongside but is not used by the model.
- `email_*` carries only the **domain**; local-part is dropped at the API edge before generator output.
- `ip_hash`, not raw IP.

Schema evolution: additive only. New optional fields require Schema Registry BACKWARD compatibility. Breaking changes go to `tx.events.v2`.

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursty traffic**: baseline 200 tx/s, with scheduled bursts 10× volume (2000 tx/s) for 5 minutes at 12:00 and 20:00 local time, plus randomized "flash sale" bursts.
- **Late arrivals**: 3% of events have `created_ts - event_timestamp > 60s`, max delay 30 minutes (mobile offline mode).
- **Out-of-order**: per-partition order is not guaranteed; consumers must use `event_timestamp` for windowing.

**Optional chosen:**
- 1.2% **duplicate events** (same `idempotency_key`, re-emitted within 5s — simulating retry storms).
- 5% **missing `device_id`** (older mobile SDK, web without fingerprinting).

### 3.3 Chargeback Stream

`cb.events` is published with simulated delay drawn from `LogNormal(μ=ln(14d), σ=0.7)` capped at 60d, so labels arrive long after the original transaction. Joins must be point-in-time correct.

---

## 4. Generator Architecture

```
                ┌──────────────────────┐
                │  Config (YAML)       │
                │  fraud_rate, drift,  │
                │  burst_schedule, tps │
                └──────────┬───────────┘
                           ▼
   ┌─────────────────────────────────────────────────┐
   │           generator/main.py                     │
   │  ┌──────────────┐  ┌──────────────────────────┐ │
   │  │ Offline mode │  │ Stream mode              │ │
   │  │ (backfill)   │  │ (asyncio + aiokafka)     │ │
   │  └──────┬───────┘  └─────────────┬────────────┘ │
   │         ▼                        ▼              │
   │  Parquet → MinIO/GCS      Avro → Redpanda       │
   └─────────────────────────────────────────────────┘
```

Modes:
- `--mode backfill --days 180`: writes Parquet history + chargebacks (deterministic seed).
- `--mode stream --duration 1h`: pumps live events at configured TPS.
- `--mode replay --from <ts>`: replays a Parquet window into Kafka with original timestamps.

---

## 5. Generation Controls

```yaml
# generator config
random_seed: 42
n_accounts: 500_000
n_merchants: 8_000
n_devices: 250_000
days_history: 180

# economics
fraud_rate: 0.005            # base prior; can be overridden per drift profile
burst_multiplier: 10
burst_windows: ["12:00-12:05", "20:00-20:05"]
flash_sale_random_per_day: 1

# streaming
target_tps: 200
late_arrival_pct: 0.03
late_delay_log_mean_sec: 60
duplicate_pct: 0.012

# data quality
null_device_pct: 0.05
schema_change_date: "2024-01-01"
legacy_dup_pct: 0.008

# label delay
chargeback_delay_lognormal: { mu_days: 14, sigma: 0.7, cap_days: 60 }

# drift (Section 03 expands these)
drift_profile: "baseline"
```

---

## 6. Realism Mechanisms

- **Account behaviour clusters** via Gaussian mixture over (avg amount, tx/day, country). New accounts inherit cluster centroid + noise.
- **Merchant velocity** follows Pareto so a few "whale" merchants dominate volume — stresses partitioning and high-cardinality joins.
- **Card-to-account fan-out**: 92% of accounts have 1 card, 6% have 2, 2% have ≥3. Cards on the same account share `email_purchaser` and `billing_zone` so they collapse to the same `uid1` but distinct `uid2` (different `card_age_days`) — exercises the uid hierarchy.
- **Email/device reuse**: 1% of legitimate accounts share an email domain pattern (corporate); 0.5% of accounts reuse a device_id (family device). These create natural collisions in `uid3` / `uid4` that the model must learn to tolerate.
- **Fraud patterns** at baseline:
  - *card-testing*: many small tx (< $5) on one card across many merchants in 10 minutes. Visible to model via `card_tx_count_so_far`, `distinct_merchants_1h`, low `card_age_days`.
  - *account takeover*: large tx after dormant period and country change. Visible via `amount_zscore_card`, `country_change_flag`, large `days_since_last_tx`.
  - *merchant collusion*: high-amount tx on a single low-tier merchant. Visible via `merchant_chargeback_rate_30d`, `amount_zscore_merchant`.
  - *bust-out*: brand-new card ramps small legitimate-looking tx for ~14 days, then a single large fraud. Specifically targets the `(day − card_age_days)` anchor used in `uid2` — mid-quality solutions miss this.
- Each pattern is a parametrized generator function, mixed via weights so prevalence is configurable. Patterns are designed so that **no single feature uniquely identifies them** — the model must combine signals across families (transaction-level + uid + identity), and Section 03 S3 (adversarial mimicry) can suppress any one family in isolation.

---

## 7. Quality Report (deliverable per run)

The generator writes `evidence/01_quality_report.json` containing:

- skew: top-1% merchant volume share, top-1% account share
- cardinality: `approx_count_distinct(account_id, merchant_id, device_id, idempotency_key)`
- duplicate ratio (offline + stream) before dedup
- null rate per column
- late-arrival distribution percentiles (p50/p95/p99)
- realized fraud rate vs configured

A Great Expectations suite runs against the Parquet output and produces `evidence/01_ge_report.html`.

---

## 8. Failure Handling

- Stream mode: bounded asyncio queue + back-pressure; on Kafka unavailability, write to `dlq/` Parquet and exit non-zero.
- Backfill mode: writes per-day partitions atomically (`_SUCCESS` marker); reruns are idempotent on `(event_date, idempotency_key)`.
- Crash recovery: state checkpoint `state.json` with last `tx_id` watermark.

---

## 9. Observability

- Structured JSON logs (`logging` + `python-json-logger`), correlation id per run.
- Prometheus metrics on a sidecar port (`/metrics`):
  - `generator_events_emitted_total{topic}`
  - `generator_lag_seconds`
  - `generator_drops_total{reason}`
- Trace span per batch with OpenTelemetry → OTLP collector (optional in local).

---

## 10. Security

- No real PII used. Email/IP are hashed (`HMAC-SHA256` with salt from env).
- PAN tokens never logged; only last-4 surfaced in samples.
- Kafka clients use SASL/SCRAM in cluster; secrets from K8s `Secret` mounted as files.
- Generator container runs as non-root, read-only filesystem except `/tmp`.

---

## 11. CI/CD

- `pytest` unit tests: schema conformance, fraud-rate within tolerance, duplicate rate within tolerance, deterministic with fixed seed.
- Property-based tests (`hypothesis`) on transformation utilities.
- GitHub Actions: lint (`ruff`, `mypy`) → tests → build → push to GAR on tag.
- Helm chart `charts/generator/` deploys as a `Deployment` (stream) or `Job` (backfill) with config via `ConfigMap`.

---

## 12. Deliverables

1. `services/generator/` Python package with CLI.
2. Sample outputs: `evidence/01_sample.parquet` (10k rows), `evidence/01_sample.avro` (1k events).
3. Quality report JSON + Great Expectations HTML.
4. Helm chart + container image in GAR.
5. README with `make demo` reproducing 5-minute local run.
