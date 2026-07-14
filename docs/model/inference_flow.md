# Inference Flow — From Phantom Ledger Stream to Score

How a single transaction emitted by the Phantom Ledger generator becomes a fraud score returned by the scoring API. This is the *serve-time* counterpart to [train_flow.md](train_flow.md). Same model, very different mechanics.

The training run produces two artifacts that fully define the contract:

- `outputs/model.txt` — LightGBM booster (Phase B, fit on full data).
- `outputs/feature_schema.json` — every transform mapping the booster needs.

`scoring-api` loads exactly these two files and nothing else.

---

## 0. Why training and serving look so different

| Concern | Training | Serving |
|---|---|---|
| Data shape | a 1.1M-row dataframe in memory | one transaction at a time |
| Latency budget | minutes per run | < 100 ms p99 per request |
| Aggregations | `groupby + transform` over full history | precomputed in Redis, single `HMGET` |
| Encoding | fit `value_counts`, save dict | dict lookup (unseen → 1 / "missing") |
| `account_id` | reconstructed via the `card1+addr1+D1` trick | comes from the generator natively |
| Test set view | concat for global stats | only the row in hand + Redis state |

Anything done at training but not reproducible at serve time would be **train/serve skew** — the most common silent failure mode in fraud ML. The `feature_schema.json` exists so that every transform applied at training has a deterministic, single-row equivalent at serving.

---

## 1. Where transactions come from

The Phantom Ledger generator (`services/generator/`) emits transactions to two Kafka topics on Redpanda:

- `tx.events` — partitioned by `account_id`, Avro envelope.
- `cb.events` — chargeback reports (delayed, not used at score time).

A transaction payload (after Avro deserialisation) looks like:

```json
{
  "tx_id":           "0192c3f0-...-7d0c",
  "account_id":      "acct_88e21",
  "merchant_id":     "merch_4421",
  "device_id":       "dev_a7f1",
  "amount_usd":      129.50,
  "mcc":             "5411",
  "channel":         "MOBILE_APP",
  "event_timestamp": "2026-05-01T18:42:11.310Z",
  "created_ts":      "2026-05-01T18:42:12.044Z",
  "idempotency_key": "ik_3b...",
  "ip_hash":         "h_91...",
  "card_brand":      "visa",
  "card_type":       "credit",
  "card_country":    "US",
  "billing_zone":    "94107",
  "billing_country": "US",
  "email_purchaser": "u_7f...@gmail.com",
  "email_recipient": "u_7f...@gmail.com",
  "device_info":     "iPhone",
  "os_raw":          "iOS 17.4",
  "browser_raw":     "mobile safari 17.4",
  "screen_resolution": "390x844",
  "device_type":     "mobile"
}
```

Two paths consume this event:

1. **Stream-processor** (`services/stream-processor/`, Bytewax) — updates online features in Redis.
2. **Scoring API** (`services/scoring-api/`, FastAPI) — reads features from Redis and returns a decision in real time.

---

## 2. Stream-processor — keeping Redis warm

The stream-processor is the serve-time twin of the training-time `groupby` and `cumsum` operations. For every event:

1. Look up the current per-account state in Redis: `feat:account:{account_id}` (hash).
2. Update rolling aggregates with the new event:
   - increment `tx_count_so_far`,
   - update `amount_sum_so_far`, `amount_mean_so_far`, `amount_zscore_card`,
   - update window-bucketed counts (1m, 5m, 1h, 24h, 7d, 30d).
3. Update per-uid rolling aggregates: `feat:uid2:{uid2_hash}`, `feat:uid3:{uid3_hash}`, `feat:uid4:{uid4_hash}` — the same uids built at training, but rebuilt on the live event using the same recipe (see §3).
4. `HSET` the updated state back to Redis with TTL (7d for account, 30d for merchant).
5. Forward the raw event to the bronze sink (GCS Parquet) for offline backfill.

Crucially, **the rolling features written to Redis exclude the current event itself**. This mirrors the `cumsum − amount_current` trick in training: the model must see only the past.

When `scoring-api` is later asked to score this same event, it will find the updated Redis state for *next* events but will compute the current-event features as the **previous** state. To make this race-free, the scoring API doesn't poll Redis after the stream-processor; instead the stream-processor publishes a tombstoned `feat:account:{account_id}:pre@{tx_id}` snapshot which is the "as-of-just-before-this-tx" state, and the scoring API reads that.

---

## 3. Scoring API — request lifecycle

```
POST /score                                              ┌─────────────────┐
   { …raw transaction… }                                 │   Acquirer      │
            │                                            └────────┬────────┘
            ▼                                                     │
   ┌──────────────────┐                                           │ approve / decline / 3DS
   │  scoring-api     │                                           │
   │  (FastAPI pod)   │ ◄─────────────────────────────────────────┘
   └────────┬─────────┘
            │
            ├── (1)  Pydantic validate payload
            │
            ├── (2)  apply rename_at_ingest          (schema)
            │
            ├── (3)  HMGET feat:account:<id>:pre@<tx_id>   ──► Redis
            │       HMGET feat:uid2:<hash>          ──► Redis
            │       HMGET feat:uid3:<hash>          ──► Redis
            │       HMGET feat:uid4:<hash>          ──► Redis
            │       HMGET feat:merchant:<id>        ──► Redis
            │
            ├── (4)  build derived features          (in-process)
            │       hour_of_day, amount_log,
            │       device_brand, os_family, …
            │
            ├── (5)  freq_encode lookups             (schema.freq_tables)
            │       categorical encode lookups       (schema.categorical_encoders)
            │
            ├── (6)  assemble vector in feat_cols order
            │
            ├── (7)  booster.predict(vector) → score
            │
            ├── (8)  threshold → decision
            │
            └── (9)  log decision to GCS Parquet     (async writer)

response: { tx_id, score, decision, model_version, cold_start }
```

### 3.1 Validation
Pydantic model derived from the same protobuf as the generator. Reject unknown fields. p99 < 1 ms.

### 3.2 Rename at ingest
The booster expects Phantom Ledger names (`amount_usd`, `card_id`, etc.). The acquirer payload may use legacy names; `schema.rename_at_ingest` is applied. In the IEEE-CIS training source the rename also covered `TransactionAmt → amount_usd` etc.; in production this map is mostly identity.

### 3.3 Redis lookups
A single pipelined `HMGET` round-trip retrieves all online features in one network call (~2–4 ms p99 to local Redis). Missing keys → all-NaN dict; the request is flagged `cold_start=true`.

### 3.4 Derived (in-process) features
Cheap row-local transforms identical to training:

```python
hour_of_day  = ts.hour
day_of_week  = ts.dayofweek
is_weekend   = day_of_week >= 5
amount_log   = log1p(amount_usd)
amount_cents = amount_usd - int(amount_usd)
device_brand = _device_brand(device_info)
os_family    = _os_family(os_raw)
os_version   = parse_first_number(os_raw)
browser_family, browser_version = …
screen_w, screen_h, screen_area = parse_screen(screen_resolution)
```

The parser functions are imported from a shared `phantom_features` package — same code used by the training script (`add_identity_features` in `fraud_lgbm.py`). One source of truth, no duplication.

### 3.5 UID reconstruction at serve time
Same recipe as `build_features` (training), but on a single row:

```python
day_of_year   = event_timestamp.dayofyear
first_seen    = day_of_year - card_age_days_or_0
uid1 = f"{card_id}_{billing_zone}"
uid2 = f"{uid1}_{first_seen}"
uid3 = f"{uid2}_{email_purchaser}"
uid4 = f"{uid3}_{device_brand}"
```

These are then **looked up** in `feat_uid{2,3,4}:<hash>` Redis hashes (already populated by the stream-processor at step §2). The booster sees `uid2_amount_usd_mean`, `uid3_C13_std`, etc. — same column names as training.

### 3.6 Frequency + categorical encoding
For every column in `schema.freq_columns`:

```python
v = row[col]
row[f"{col}_freq"] = schema.freq_tables[col].get(v, 1)
```

Unseen values get count = 1 (a deliberate floor — no zero division, treated as "rare"). For categoricals:

```python
mapping = schema.categorical_encoders[col]
row[col] = mapping.get(str(row[col]), mapping["missing"])
```

Unseen categories collapse into the `"missing"` bucket the training pass already encoded. This means a brand-new merchant on day 1 of production is scored as "unfamiliar" rather than crashing the request.

### 3.7 Vector assembly
The booster strictly expects `schema.feature_columns` in **that exact order** (250 elements after selection). The API constructs a numpy array of shape `(1, 250)` with NaNs where data is missing — LightGBM handles NaN natively (categoricals included).

### 3.8 Predict
```python
score = booster.predict(vector)[0]
```
Single-row predict on a small booster (~5–15 MB): typically 0.3–1 ms.

### 3.9 Decision
Threshold tuning on val sets two cut-offs:

```python
if score >= τ_high:    decision = "decline"          # ~0.5% FPR target
elif score >= τ_low:   decision = "step_up_3ds"      # ~8% of traffic
else:                  decision = "approve"
```

`τ_high` and `τ_low` are stored alongside the model version and reloaded on each model swap.

### 3.10 Decision log
Append `{tx_id, score, decision, model_version, cold_start, features_hash, ts}` to an in-process ring buffer; flushed every 1 second to `gs://…/decisions/decision_date=…/scoring-api-<pod>.parquet` by an async writer. This log is the input to monitoring (PSI, KS, calibration) and Section 03 drift evaluation.

---

## 4. Latency budget (p99)

| Step | Target |
|---|---|
| Pydantic validation | < 1 ms |
| Rename + parsers | < 1 ms |
| Redis pipelined HMGET (3–5 keys) | < 5 ms |
| Encoding lookups (freq + categorical) | < 1 ms |
| Vector assembly | < 1 ms |
| Booster predict | < 2 ms |
| Threshold + log enqueue | < 0.5 ms |
| **Total in-process** | **< 12 ms** |
| End-to-end with network (acquirer → API → response) | < 100 ms |

Internal measurement targets ≤ 25 ms p99 to leave head-room for tail latency (Redis hiccups, GC pauses, network).

---

## 5. Cold-start handling

A transaction is `cold_start = true` when:
- the account has no Redis state (new account, first ever transaction), **or**
- the uid2 hash has < 2 prior events (rolling stats degenerate).

Behaviour:
- Score is computed normally (model has features for everything else: amount, channel, identity).
- Decision is **forced to `approve`** unless score ≥ τ_high (extreme score → still decline). This avoids friction for genuine new users.
- The transaction is flagged in the decision log and surfaces on a "new-account watchlist" dashboard for manual review next day.

Configurable per acquirer.

---

## 6. Differences from training, in one table

| Aspect | Training (`fraud_lgbm.py`) | Serving (`scoring-api`) |
|---|---|---|
| `event_ts_offset_s` | seconds since `2017-12-01` | real Unix `event_timestamp` |
| Rolling aggregates | `groupby.cumsum() − current` | Redis `HMGET` of pre-computed snapshot |
| UID group means/stds | `groupby.transform("mean")` over full data | Redis lookup; updated by stream-processor |
| Frequency encoding | `value_counts()` over train+test | `freq_tables.get(value, 1)` |
| Categorical encoding | `{value: int}` from `unique()` | `categorical_encoders.get(str(v), encoders["missing"])` |
| Identity parsers | applied on a 1.1M dataframe | applied on one row, same code |
| Output | `model.txt`, `submission.csv`, `feature_schema.json` | `{score, decision}` per request |

The `phantom_features` Python package contains the parsers (`_device_brand`, `_os_family`, `_browser_family`, screen parser, uid builder). Both `fraud_lgbm.py` (training) and `services/scoring-api` (serving) import from it. **A change to a parser is a single PR that affects both sides** — the only sustainable way to prevent train/serve skew.

---

## 7. Failure modes & responses

| Failure | Detection | Response |
|---|---|---|
| Redis unavailable | health probe + 5xx spike | fall back to all-NaN feature vector + `feature_unavailable=true`; auto-page |
| Schema version mismatch | request header `x-model-version` ≠ loaded | 503 + reload model from registry |
| Unseen categorical | encoder lookup misses | map to `"missing"` bucket, log counter |
| Stream-processor lag > 30 s | metric `stream_lag_seconds > 30` | scoring still works (uses last-known Redis state); alert |
| Booster file corrupt | startup probe fails | keep prior version, page |
| Score distribution shift | daily PSI job (Section 03) | trigger retrain via Prefect flow |

---

## 8. Reproducing a single inference, locally

```bash
# 1. Train (produces outputs/model.txt + feature_schema.json)
python3 fraud_lgbm.py

# 2. Start a Redis instance + stream-processor + scoring-api
docker compose up redis stream-processor scoring-api

# 3. Send a synthetic transaction to the API
curl -X POST http://localhost:8000/score \
  -H 'content-type: application/json' \
  -d @samples/example_tx.json

# 4. Inspect response
# { "tx_id": "...", "score": 0.0123, "decision": "approve",
#   "model_version": "v1.0.0", "cold_start": true }
```

For end-to-end through the Kafka stream:

```bash
docker compose up generator                # pumps tx.events at 200 tps
# scoring-api will see the same events via the consumer side-car
# and log decisions to ./decisions/
```

---

## 9. Where this fits in the wider system

- [01_data_generator.md](../design/01_data_generator.md) — what the generator emits.
- [02_schema_design_example.md](../design/02_schema_design_example.md) — Bronze/Silver/Gold layers and the online feature store contract.
- [03_data_generator_improvement.md](../design/03_data_generator_improvement.md) — drift scenarios that will hit this serving path.
- [04.1_ml_design_example.md](../design/04.1_ml_design_example.md) — the full ML system design (rollout/rollback, monitoring, retraining).
- [train_flow.md](train_flow.md) — how `model.txt` and `feature_schema.json` were produced.
