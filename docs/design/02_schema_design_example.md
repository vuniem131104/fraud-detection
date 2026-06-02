# Phantom Ledger — Schema & Pipeline Design

## 1. Goals

Design storage layout and pipelines that:
1. Ingest the offline + streaming data from Section 01.
2. Produce **point-in-time-correct** features for online scoring and offline training.
3. Enforce data contracts and SLAs explicit enough to detect drift (Section 03) and retraining triggers (Section 04.1).

Non-goals: multi-region replication, exactly-once cross-system semantics. We target effectively-once via idempotent writes.

---

## 2. Storage Layout (Medallion)

```
gs://phantom-ledger-<env>/
├── bronze/
│   ├── tx_events/           event_date=YYYY-MM-DD/   raw avro→parquet
│   └── cb_events/           reported_date=YYYY-MM-DD/
├── silver/
│   ├── fact_transactions/   event_date=YYYY-MM-DD/   cleaned, joined with dims
│   ├── fact_chargebacks/    reported_date=YYYY-MM-DD/
│   ├── dim_account/         (SCD type 2)
│   ├── dim_merchant/        (SCD type 2)
│   └── dim_device/
├── gold/
│   ├── feat_card_rolling/   feature_date=…
│   ├── feat_merchant_rolling/
│   ├── feat_velocity/
│   ├── feat_uid2/           feature_date=…           uid2 group aggregates
│   ├── feat_uid3/           feature_date=…           uid3 group aggregates
│   ├── feat_uid4/           feature_date=…           uid4 group aggregates
│   └── ml_training_set/     cutoff_date=…           training table snapshots
└── decisions/               decision_date=…           scoring-api decision log
```

Online store (Redis):
- `feat:account:{account_id}` — hash of latest rolling features, TTL 7d.
- `feat:account:{account_id}:pre@{tx_id}` — pre-event snapshot for race-free scoring (TTL 5m).
- `feat:merchant:{merchant_id}` — hash, TTL 30d.
- `feat:uid2:{hash}` / `feat:uid3:{hash}` / `feat:uid4:{hash}` — uid group aggregates (`mean`, `std` of `amount_usd`, `C13`, `D15`, `D4`), TTL 30d. Hash key is a 64-bit FNV1a of the uid string to keep memory bounded.
- `model:meta` — current model alias and version.

The uid hash space is pre-computed at training time and persisted in `feature_schema.json#freq_tables` so unseen uids get a deterministic "rare" floor (count = 1) at serve time without crashing.

---

## 3. Bronze Layer — Raw Capture

Purpose: lossless capture of source data. No schema transformation beyond what is needed to land it.

| Table | Source | Write Pattern |
|---|---|---|
| `bronze.tx_events` | Redpanda topic `tx.events` | Kafka Connect S3 sink → Parquet, partition by `event_date = date(created_ts)` |
| `bronze.cb_events` | Redpanda topic `cb.events` | same, partition by `reported_date` |
| `bronze.dim_*_snapshot` | generator backfill | nightly Parquet dump |

Contracts:
- Schema registered in Avro Schema Registry; Bronze keeps the raw Avro envelope.
- Append-only. No deletes. Late events go to today's partition; reprocessing uses `event_date` partition.
- DQ: file-level checksums + `_SUCCESS` marker per partition.

---

## 4. Silver Layer — Cleaned & Conformed

### 4.1 `silver.fact_transactions`

```sql
CREATE TABLE silver.fact_transactions (
  tx_id              STRING NOT NULL,        -- PK
  account_id         STRING NOT NULL,
  merchant_id        STRING NOT NULL,
  device_id          STRING,                  -- nullable
  amount_usd         DOUBLE NOT NULL,
  mcc                STRING NOT NULL,
  channel            STRING NOT NULL,
  event_timestamp    TIMESTAMP NOT NULL,
  created_ts         TIMESTAMP NOT NULL,
  ingested_ts        TIMESTAMP NOT NULL,
  idempotency_key    STRING NOT NULL,
  account_country    STRING,                  -- denormalized as-of dim_account
  account_kyc_level  STRING,
  merchant_mcc       STRING,
  merchant_risk_tier STRING,
  is_late            BOOLEAN,                 -- created_ts - event_ts > 60s
  PRIMARY KEY (tx_id, event_timestamp)        -- logical
)
PARTITIONED BY event_date = DATE(event_timestamp)
CLUSTERED BY account_id;
```

Build job (`batch-jobs/bronze_to_silver_tx.py`, DuckDB):

1. Read bronze partition window `[event_date - 1, event_date]` (covers late arrivals).
2. **Deduplicate** by `(idempotency_key, account_id)` keeping `MIN(created_ts)`.
3. **As-of join** with `dim_account` and `dim_merchant` on `event_timestamp` (SCD2).
4. Compute `is_late`.
5. Write to silver with idempotent partition overwrite.

### 4.2 `silver.fact_chargebacks`

```
cb_id, tx_id, reason_code, reason_group, reported_ts, amount_usd
```

Joined back to `silver.fact_transactions.tx_id` only at gold layer (to keep silver narrow).

### 4.3 SCD2 Dimensions

`dim_account`, `dim_merchant`, `dim_device` use `(natural_key, valid_from, valid_to, is_current)`. Daily merge job consumes new rows, expires the prior record.

---

## 5. Gold Layer — Feature & Serving Tables

### 5.1 Rolling Features (`gold.feat_card_rolling`)

Computed for every `(account_id, feature_ts)` pair, where `feature_ts` is bucketed every minute for streaming and every hour for batch backfill.

Windows: 1m, 5m, 1h, 24h, 7d, 30d.

| Feature | Type | Window |
|---|---|---|
| `tx_count` | int | each window |
| `amount_sum_usd` | double | each window |
| `amount_max_usd` | double | each window |
| `distinct_merchants` | int | 1h, 24h |
| `distinct_countries` | int | 24h, 7d |
| `distinct_mccs` | int | 24h |
| `velocity_sec_p50` | double | 1h |
| `time_since_signup_d` | double | snapshot |
| `chargeback_rate_30d` | double | 30d (excludes labels < 60d old) |

Streaming compute path (`stream-processor/`, Bytewax):
- subscribe `tx.events` → tumbling/sliding windows by event time → write to:
  - Redis (online), key `feat:account:{account_id}`, atomic `HSET`.
  - GCS gold partition every 1 minute via micro-batch.

Batch compute path (`batch-jobs/gold_card_rolling.py`):
- nightly recompute over 30d trailing window for **point-in-time correctness on training**; reconciles any stream gaps.

### 5.1.1 UID Rolling Features (`gold.feat_uid{2,3,4}`)

Mirror tables for the user-identification proxies introduced in [04.1 §3.3](04.1_ml_design_example.md). Each row keyed by `(uid_hash, feature_ts)`:

| Feature | Type | Notes |
|---|---|---|
| `<uid>_amount_usd_mean` | double | rolling per uid, all-time within retention |
| `<uid>_amount_usd_std`  | double | same |
| `<uid>_C13_mean` / `_std` | double | counting-feature aggregate |
| `<uid>_D15_mean` / `_std` | double | days-since-last-tx aggregate |
| `<uid>_D4_mean` / `_std`  | double | optional, depends on availability |
| `<uid>_freq` | int | count of tx seen with this uid |

Construction recipe (the **same** in both training and serving):
- `uid1 = card_id + "_" + billing_zone`
- `uid2 = uid1 + "_" + (event_day − card_age_days)`     ← anchors first-seen
- `uid3 = uid2 + "_" + email_purchaser`
- `uid4 = uid3 + "_" + device_brand`

Streaming compute path: `stream-processor/` recomputes each uid's running mean/std (Welford's algorithm) on every event, persists to `feat:uid{2,3,4}:{hash}` in Redis. Batch path nightly reconciles from gold.

Why uid2 is the most-used in production: it survives data gaps (the `(day − D1)` anchor stays stable across train/test windows), unlike uid3/4 which depend on email/device fingerprinting.

### 5.2 Training Snapshots (`gold.ml_training_set`)

```sql
SELECT
  t.tx_id,
  t.event_timestamp,
  AS_OF(feat_card_rolling, t.account_id, t.event_timestamp) AS card_features,
  AS_OF(feat_merchant_rolling, t.merchant_id, t.event_timestamp) AS mer_features,
  COALESCE(cb.is_fraud, FALSE) AS label,
  cb.reason_group
FROM silver.fact_transactions t
LEFT JOIN (
  SELECT tx_id, TRUE AS is_fraud, reason_group
  FROM silver.fact_chargebacks
  WHERE reason_group IN ('fraud_cnp','fraud_ato','fraud_lost_stolen')
) cb USING(tx_id)
WHERE t.event_date BETWEEN :start AND :cutoff
  AND t.event_date <= DATEADD(day, -60, CURRENT_DATE);  -- label stability
```

Snapshotted per training run, written to `gold.ml_training_set/cutoff_date=…`.

### 5.3 Business Serving Views

| View | Audience | Refresh |
|---|---|---|
| `gold.v_daily_fraud_kpi` | Risk Ops | hourly |
| `gold.v_merchant_risk_leaderboard` | Risk Analyst | daily |
| `gold.v_decision_audit` | Compliance | streaming (decision log) |

Naming convention: `<layer>.<entity>_<grain>` for tables; `<layer>.v_<purpose>` for views.

---

## 6. Data Contracts

All schemas live in `contracts/` as protobuf and are code-generated for Python (Pydantic) and Avro.

Contract enforcement points:

| Layer | Tool | Action on violation |
|---|---|---|
| Producer (generator, services) | Pydantic + Schema Registry compatibility check | reject publish |
| Bronze | Schema Registry (BACKWARD compatibility) | poison-pill → DLQ topic |
| Silver build | Great Expectations suite | fail job, alert |
| Gold build | dbt-style tests (uniqueness, not_null, accepted_values) | fail job, alert |
| Online API | Pydantic on request/response | 4xx |

Allowed schema evolution: additive only (new optional fields). Breaking change requires versioned topic (`tx.events.v2`).

---

## 7. Data Quality Checks (concrete)

Per silver build:

- `fact_transactions.tx_id` unique.
- `amount_usd` ∈ (0, 100_000].
- `account_id` FK presence in `dim_account` ≥ 99.5%.
- Null rate `device_id` ≤ 8%.
- `event_timestamp` within `[partition_date - 1d, partition_date + 1d]`.
- Duplicate rate (post-dedup) = 0.

Per gold build:
- Feature null rate ≤ 1%.
- PSI(feature_today, feature_baseline_30d) computed and stored.

---

## 8. SLA & Update Policy

| Pipeline | Latency target | Freshness target | Update mode |
|---|---|---|---|
| `tx.events` → online features (Redis) | p99 < 30 s | continuous | streaming upsert |
| `tx.events` → silver | < 30 min | hourly partition | micro-batch |
| silver → gold rolling | < 1 h | hourly | incremental |
| gold ml_training_set | n/a | daily snapshot | full overwrite per cutoff |
| chargeback labels | < 1 h after report | T+60d stable | append + late-update |

---

## 9. Backfill Strategy

- All silver/gold jobs are **partition-idempotent**: keyed by `event_date` (or `cutoff_date`).
- `make backfill FROM=2024-01-01 TO=2024-06-30` enumerates partitions and runs in parallel (Prefect map).
- Online store (Redis) is **not backfilled** — it only holds present-day rolling features. Cold-start is handled by the scoring API: missing key → returns default vector + flag.
- Chargeback late updates: a daily `silver.fact_transactions` patch job re-stamps `is_fraud` on transactions whose label arrived in the last 24h.

---

## 10. Failure Handling & Recovery

- **Generator outage**: stream gap detected by `created_ts` lag > 5 min → alert; silver job tolerates gaps (just an empty hour).
- **Bronze schema mismatch**: poison-pill quarantined to `dlq.tx_events`; consumer continues.
- **Silver job failure**: Prefect retry × 3 with exponential backoff; on persistent fail, partition stays empty and downstream gold job skips it (and alerts).
- **Redis unavailable**: scoring API falls back to `feat_card_rolling` parquet via DuckDB (degraded latency, still correct).
- **Replay**: any silver partition can be re-derived from bronze; gold from silver.

---

## 11. Observability

- Per-job Prometheus metrics: `pipeline_rows_in`, `rows_out`, `duration_seconds`, `dq_violations_total{check}`.
- OpenTelemetry trace per Prefect flow; spans link Bronze→Silver→Gold for one partition.
- Logs in JSON to stdout → Cloud Logging on GKE.
- Dashboards (Grafana): pipeline freshness, DQ violation rate, partition completeness heatmap.

Alerts (Alertmanager):
- silver freshness > 2× SLA: page.
- DQ violation rate > 1%: warn.
- DLQ topic depth > 1000: page.

---

## 12. Security

- IAM: separate GSA for generator (write Bronze only), pipelines (read Bronze, write Silver/Gold), serving (read Gold + Redis), training (read Gold, write MLflow). Bound via Workload Identity.
- Bucket-level uniform access; CMEK with Cloud KMS key per environment.
- PII: `email_hash`, `ip_hash` only — raw never lands in Bronze (hashed at producer).
- Card PAN: only `pan_token` (HMAC) and `pan_last4` retained; raw discarded at the API edge before generator output.
- Secrets: `External Secrets Operator` syncing Google Secret Manager into K8s `Secret`.
- RBAC (BigQuery / future): `risk_analyst` reads gold views; only `risk_engineer` reads silver.

---

## 13. CI/CD

- `dbt test` (or equivalent SQL tests) on every PR against a sandbox dataset.
- Schema Registry compatibility check is a required PR check (`buf` for protobuf, `avro-tools` for Avro).
- Helm umbrella chart `charts/phantom-ledger/` versions all pipeline images together; promotion via Git tag.
- Migrations (e.g., new gold view) run as a one-off K8s Job before the upgraded service deploys (Helm `pre-upgrade` hook).

---

## 14. Deliverables

1. `contracts/` with protobuf + Avro schemas and generated Python.
2. `services/stream-processor/` (Bytewax app) and `services/batch-jobs/` (DuckDB scripts) implementing Bronze→Silver→Gold.
3. Great Expectations + dbt-style tests + run reports under `evidence/02_*`.
4. Sample queries in `notebooks/02_serving_views_demo.ipynb`.
5. Helm chart values for dev/staging/prod overlays.
