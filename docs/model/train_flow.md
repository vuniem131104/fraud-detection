# Training Flow — `fraud_lgbm.py`

End-to-end description of the LightGBM training pipeline that produces the production model for Phantom Ledger fraud detection. The script ([fraud_lgbm.py](../fraud_lgbm.py)) is one file, ~440 lines, so the entire flow is visible at a glance.

For how the trained model is consumed at serving time, see [inference_flow.md](inference_flow.md).

---

## 0. Inputs and outputs at a glance

**Inputs** (under `dataset/`, IEEE-CIS Fraud Detection):
- `train_transaction.csv` — ~590k rows, has `isFraud` label.
- `train_identity.csv` — device/browser metadata for ~25% of train rows.
- `test_transaction.csv` — ~507k rows, no label (Kaggle holdout).
- `test_identity.csv` — same coverage as train identity.

**Outputs** (under `outputs/`):

| File | Purpose | Consumer |
|---|---|---|
| `model.txt` | LightGBM booster from **Phase B** (full data) | `scoring-api` (`lgb.Booster(model_file=...)`) |
| `feature_schema.json` | Renames, encoders, freq tables, feature order | `scoring-api`, stream-processor |
| `metrics.json` | Phase A val ROC-AUC / PR-AUC / precision@1%, Phase B round count | MLflow run logging |
| `feature_importance.csv` | Gain-ranked feature list (from Phase B model) | Drift scenario S3 (adversarial mimicry) |
| `feature_selection_gain.csv` | Full ranking from the selection pass | Audit which features got dropped |
| `submission.csv` | Kaggle submission (`TransactionID, isFraud`) | Sanity-check vs. public leaderboard |

The script is **idempotent**: same seed, same data → same outputs.

---

## 1. Pipeline overview

```
                              load_split("train")        load_split("test")
                                       │                          │
                                       └────────┬─────────────────┘
                                                ▼
                                  pd.concat(train, test)        ◄─ (label NaN on test rows)
                                                │
                                                ▼
                            ┌─────────  build_features  ─────────┐
                            │ time-of-day, amount transforms,    │
                            │ identity parsers,                  │
                            │ email cleanup,                     │
                            │ uid1/uid2/uid3/uid4 construction,  │
                            │ uid group aggregations,            │
                            │ point-in-time rolling stats        │
                            └─────────────┬──────────────────────┘
                                          ▼
                                    freq_encode               ◄─ persists freq_tables
                                          ▼
                                encode_categoricals           ◄─ persists encoders
                                          ▼
                              split back: train_df / test_df
                                          ▼
                              select_features (top 250)         ◄─ quick LGBM, gain-rank
                                          ▼
                              train_phase_a                     ◄─ first 85% train,
                                          ▼                       last 15% val (early stop)
                              train_phase_b                     ◄─ refit on FULL labeled set
                                          ▼                       for best_iter × 1.10 rounds
                              write_submission                  ──►  submission.csv
                                          ▼
                          dump model + schema + importance      ──►  model.txt, feature_schema.json,
                                                                     feature_importance.csv
```

**Two-phase training — why.** Phase A puts the validation slice at the *end* of the timeline, the closest analogue to the Kaggle test window, so early stopping picks an iteration that generalises forward. Phase B then refits on **all** labeled rows (no holdout) for a fixed round count. This is standard Kaggle/production practice: never throw away the most recent ~15% of data when fitting the model that will actually serve.

---

## 2. Step-by-step

### 2.1 `load_split(name)` — transaction + identity merge

For each split (`train`, `test`):
1. Read `{name}_transaction.csv`.
2. Read `{name}_identity.csv` and normalize columns (`id-XX → id_XX`; the test file uses dashes).
3. **Left join** on `TransactionID`. Identity is only present for ~25% of rows; the rest get NaN, which LightGBM handles natively.
4. Apply the `RENAME` map: every column name becomes the Phantom Ledger schema name. From this point on, no IEEE-CIS naming exists in the codebase.

### 2.2 `pd.concat([train, test])` — combined feature engineering

Concatenated so that uid groups, freq tables, and categorical encoders see the **full** population. Test rows carry `label = NaN`; they will be filtered out before training.

This is a deliberate design choice: at production time, the equivalent operation is a one-time fit over historical labeled data, with serve-time lookups against the persisted tables.

### 2.3 `build_features(df)`

The heart of the pipeline. Applied once on the combined frame.

**(a) Time decomposition.** `event_ts_offset_s` is seconds since `2017-12-01` (Vesta convention, not Unix). Derived features: `hour_of_day`, `day_of_week`, `is_weekend`.

**(b) Amount transforms.** `amount_log = log1p(amount_usd)` to tame the heavy tail; `amount_cents = decimal part`, since fraud amounts are often FX-converted and have non-zero cents.

**(c) Identity parsers** (`add_identity_features`). Raw strings → bucketed features:
- `device_info` → `device_brand` (samsung, moto, lg, huawei, ...).
- `os_raw` → `os_family` (windows/ios/android/mac/linux) + `os_version` (numeric).
- `browser_raw` → `browser_family` (chrome/safari/firefox/edge/...) + `browser_version`.
- `screen_resolution` (`"1920x1080"`) → `screen_width`, `screen_height`, `screen_area`.

The raw columns are kept too — they get freq- and label-encoded — so the model has both the parsed family and the high-card fingerprint.

**(d) Email cleanup.** `anonymous.com` and `mail.com` are replaced with NaN **before** uid construction. They are placeholders carrying no identification signal; if left in, they would lump unrelated users into one uid bucket.

**(e) User identification (the magic).** Four nested uids:

| UID | Composition | Intent |
|---|---|---|
| `uid1` | `card_id + billing_zone` | basic card-region key |
| `uid2` | `uid1 + (day − card_age_days)` | anchors to **first-seen day** of the card; stable across train/test gap |
| `uid3` | `uid2 + email_purchaser` | separates same-cohort users by email |
| `uid4` | `uid3 + device_brand` | same person on same device cluster |

Why `(day − card_age_days)` instead of `D3`: D3 is days-since-previous-tx, which is broken by the train/test gap window. D1 (`card_age_days`) is anchored to an absolute reference point and stays stable.

**(f) UID group aggregations.** For each `uid ∈ {uid1, uid2, uid3, uid4}` × each `target ∈ {amount_usd, C13, D15, D4}`, compute group `mean` and `std` via `groupby.transform`. ~32 features capturing each row's deviation from "this user's typical behaviour".

**(g) Point-in-time-correct rolling stats.** Sort by `event_ts_offset_s`, then per `uid2`:
- `card_tx_count_so_far = cumcount()`
- `card_amount_sum_so_far = cumsum(amount) − amount_current`  *(subtract self to avoid leak)*
- `card_amount_mean_so_far`, `amount_zscore_card`

These mirror the streaming feature store: at score time, the model sees only the past.

### 2.4 `freq_encode(df)`

For each high-card column in `FREQ_COLS` (cards, addresses, emails, uids, devices), compute `value_counts` and create `<col>_freq`. The count tables are persisted into `feature_schema.json` so `scoring-api` can apply the same mapping at serve time (unseen value → 1).

The raw `uid1..uid4` string columns are dropped after encoding; the model sees only their frequency.

### 2.5 `encode_categoricals(df)`

Every remaining `object` column gets a deterministic `{value: int}` mapping. The mapping is persisted (`encoders` in the schema) so train and serve produce identical integers. NaN → `"missing"` → encoded as a real category.

### 2.6 Split back

```python
train_df = full.iloc[:n_train]   # has labels
test_df  = full.iloc[n_train:]   # label = NaN
assert train_df[TARGET].notna().all()
```

### 2.7 `select_features(train_df, cat_cols, top_k=250)`

After feature engineering the frame has ~490 columns; most of the V* family is mutually redundant. We do **one fast LightGBM pass** (lr 0.05, ≤2000 rounds, early stop 100) on the same temporal split as Phase A, rank features by `gain`, and keep the top 250.

The full ranking is written to `feature_selection_gain.csv` so it is auditable; the cut-off `min gain` is logged.

This step alone typically lifts the public-LB score ~0.005 because a smaller feature set lets the model spend its splits on signal rather than noise, and reduces overfitting in Phase A.

### 2.8 `train_phase_a(train_df, feat_cols, cat_cols)` — early stopping

**Why a separate Phase A.** Early stopping needs a validation slice that *resembles* the production / Kaggle test window. The Kaggle test set is the **future** relative to train. So:

```
| ───── train (oldest 85%) ───── | ── val (newest 15%) ── |
```

Random KFold or a middle-of-timeline val both pick a `best_iter` that is too small for forward generalisation. Putting val at the end fixes this.

**Class imbalance.** `scale_pos_weight = n_neg / n_pos ≈ 27`. Cost-sensitive learning, no resampling — preserves calibration.

**Training.**
- LightGBM, up to 5000 rounds, early stopping after 200 rounds with no val-AUC improvement.
- 256 leaves, lr 0.02, feature_fraction 0.5, bagging_fraction 0.85.
- `categorical_feature` declared so LightGBM splits high-card categoricals optimally (no one-hot).

**Reporting.** Metrics on val:
- ROC-AUC — reference, comparable to Kaggle leaderboard.
- **PR-AUC** — primary metric (insensitive to majority class).
- **precision@1%** — operational metric matching Risk Ops review-queue capacity.

Returns `best_iter` for Phase B and `val_metrics` for the metrics dump.

### 2.9 `train_phase_b(train_df, feat_cols, cat_cols, best_iter)` — refit on full data

This is the model that actually ships. We refit on the **entire labeled set** (no holdout) for `n_rounds = best_iter × 1.10` (`FINAL_ROUND_BUMP`). The bump compensates for the +18% data the booster now sees per round.

No early stopping — there is nothing to stop on, by design.

The Phase B booster is saved to `outputs/model.txt`. It is the artifact uploaded to MLflow and loaded by `scoring-api`.

### 2.10 `write_submission(test_df, model, feat_cols)`

Predict on `test_df` (Kaggle holdout) using the Phase B booster (no `num_iteration` — Phase B has no early stopping). Emit:

```
TransactionID,isFraud
3663549,0.0034
3663550,0.0012
...
```

Plus a sanity log: prediction min/max/mean. A healthy distribution has mean ≈ 0.03–0.08; mean ≈ 0.5 means something is broken.

### 2.11 Persist schema + metrics

`feature_schema.json` is the **contract** between training and serving. It contains everything the scoring service needs to reproduce the input transformation:

```json
{
  "version": 1,
  "training_reference_ts": "2017-12-01",
  "rename_at_ingest": {...},
  "email_bin": {...},
  "email_nulls": ["anonymous.com", "mail.com"],
  "uid_columns": ["uid1", "uid2", "uid3", "uid4"],
  "uid_agg_targets": ["amount_usd", "C13", "D15", "D4"],
  "freq_columns": [...],
  "freq_tables": {col: {value: count}},
  "categorical_features": [...],
  "categorical_encoders": {col: {value: int}},
  "feature_columns": [...],   # exactly the 250 selected, in order
  "target": "label"
}
```

`metrics.json` records Phase A val metrics, the chosen `best_iteration`, and the Phase B `n_rounds` and full-train row count. Logged to MLflow as part of the training run.

---

## 3. Why these choices (design alignment)

| Choice | Reason | Where in design |
|---|---|---|
| Temporal split, val at END of timeline | val must mirror future Kaggle test window | `04.1` §3.2 |
| Two-phase training (A early-stop, B refit on full) | don't waste the most recent 15% of labeled data on the final model | `04.1` §3.6 |
| Top-K feature selection (250 of ~490) | most V* are mutually redundant; smaller set generalises better | `04.1` §3.3 |
| PR-AUC + precision@1% as primary | class imbalance ~0.5%; review queue capacity | `04.1` §3.5 |
| `scale_pos_weight` not oversampling | preserves calibration; no synthetic data | `04.1` §3.4 |
| Point-in-time rolling features | mirrors streaming feature store; no train/serve skew | `04.1` §3.3 |
| Persist freq + categorical tables | scoring-api needs identical transforms | `04.1` §3.7 |
| Combined train+test FE | matches "fit once on history, lookup at serve" pattern | `02` §5 |
| `feature_importance.csv` exported | input to drift scenario S3 (adversarial mimicry) | `03` §5 |

---

## 4. Expected metrics (reference)

Phase A val numbers are reported on the *newest* 15% — strictly harder than a random CV slice. Don't compare them to a previous middle-of-timeline val.

| Metric | Target | Notes |
|---|---|---|
| Phase A val ROC-AUC | ≥ 0.93 | Honest, forward-generalising number |
| Phase A val PR-AUC | ≥ 0.50 | Primary metric |
| Phase A val precision@1% | ≥ 0.35 | Operational |
| Kaggle Public LB | ≥ 0.955 | After Phase B refit |
| Kaggle Private LB | ≥ 0.92 | Roughly Public − 0.025 |

A run substantially below these usually means: (1) train/test concat skipped (encoder mismatch), (2) val slice fell in the middle of the timeline (forgot to use last `VAL_FRAC`), (3) identity merge silently failed (check the load log), or (4) Phase B was skipped and submission used the Phase A model.

---

## 5. Failure modes to watch for

- **Submission `mean ≈ 0.5`** — the model is collapsing. Most often caused by a bad merge that lost most features.
- **Phase A val AUC suspiciously high (> 0.97)** — the val window probably leaked into training (check that `temporal_split` puts the *newest* rows in val).
- **Public LB drops vs. last run** despite higher Phase A AUC — Phase B `n_rounds` is too high; lower `FINAL_ROUND_BUMP` from 1.10 toward 1.05.
- **`precision@1% < 0.30`** despite high ROC-AUC — calibration issue; PR-AUC matters more than ROC-AUC for this product.
- **Schema JSON > 200 MB** — too many high-card encoders persisted; consider hashing instead of label-encoding for `uid4`.

---

## 6. Reproducing a run

```bash
python3 fraud_lgbm.py
ls outputs/
# model.txt, feature_schema.json, metrics.json,
# feature_importance.csv, submission.csv

cat outputs/metrics.json

kaggle competitions submit -c ieee-fraud-detection \
  -f outputs/submission.csv \
  -m "phantom-ledger v1: uid magic + identity"
```

For the production deployment, `model.txt` + `feature_schema.json` are pushed to the MLflow registry under alias `candidate`, then promoted via the shadow → canary → production ladder described in [04.1 §3.9](../design/04.1_ml_design_example.md).
