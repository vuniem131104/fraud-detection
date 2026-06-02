# Phantom Ledger — Generator Improvement & Drift Scenarios

## 1. Motivation

Section 01's generator produces a *stationary* world: fraud prevalence and tactics are fixed. Real fraud is **adversarial** — attackers evolve as soon as defenders adapt. A static generator cannot exercise the things that matter most for an ML system: distribution drift, concept drift, label-delay drift, and feedback loops.

This section adds four drift scenarios. Each is parametrized, reproducible (seeded), and produces measurable downstream impact on the model from Section 04.1.

---

## 2. Scenario Catalog

| # | Name | Type | Key Knob | Downstream Impact |
|---|------|------|----------|-------------------|
| S1 | Tactic shift: card-testing → ATO | Concept drift (P(y\|x) changes) | `tactic_mix_curve` | Feature `time_since_signup_d` loses discriminative power |
| S2 | Channel migration: web → mobile | Covariate drift (P(x) changes) | `channel_mix_curve` | Feature distribution shift even with stable fraud rate |
| S3 | Adversarial mimicry | Concept + covariate, model-aware | `adversary_strength` | Defender's top features systematically eroded |
| S4 | Label delay extension | Label-process drift | `cb_delay_log_mean` | Training cutoff stale → recent retrains see fewer labels |

S1, S2, S4 are **passive**: configured curves over wall time. S3 is **active**: the generator reads the current production model's feature importance and shifts feature distributions to reduce its discriminative power.

---

## 3. Scenario S1 — Tactic Shift (Concept Drift)

### Setup
- Weeks 1–2: 70% of fraud is *card-testing* (many sub-$5 tx in 10 min, new accounts).
- Weeks 3–4: tactic mix linearly shifts to 70% *account takeover* (large tx after dormancy + country change).
- Configurable via `tactic_mix_curve: [(week, {card_testing: w1, ato: w2, collusion: w3})]`.

### Why it matters for AI
- A model trained on Weeks 1–2 learns "small amount + new account ⇒ fraud." In Weeks 3–4 the dominant fraud is *large amount + old account*, so the same rule misfires both ways: many false positives on legitimate small purchases, many false negatives on big ATO tx.
- Pure feature-distribution monitoring (PSI on `amount_usd`) does **catch** this — but only after the shift has produced enough labels (60d delay), which is exactly the point: the system must rely on faster proxy signals (e.g. score distribution drift, manual-review queue precision) to detect concept drift earlier than chargebacks confirm it.

### Evidence
- PSI on `amount_usd`, `time_since_signup_d`: expected to rise above 0.25 by Week 3.
- Test-set PR-AUC drop ≥ 0.10 vs baseline if model is not retrained.

---

## 4. Scenario S2 — Channel Migration (Covariate Drift)

### Setup
- Channel mix `WEB:MOBILE_APP:POS` drifts from `60:30:10` → `25:65:10` over 2 weeks (a marketing campaign moves users to the app).
- Fraud rate **per channel** unchanged. Overall `P(y)` constant.

### Why it matters for AI
- Features like `device_id null rate`, `channel`, `velocity_sec_p50` shift even though the underlying fraud probability hasn't changed. A naive drift alarm based on "feature PSI > 0.2 → retrain" would over-trigger here. The system must distinguish covariate drift (model often still calibrated) from concept drift (it isn't).
- Demonstrates that **drift detection alone is insufficient** — we need a calibration check (Brier score on delayed labels) to decide whether to retrain.

### Evidence
- PSI on `channel_*` features high; PSI on score distribution mild; calibration intact.
- Decision rule: do **not** retrain — the system must show this judgement.

---

## 5. Scenario S3 — Adversarial Mimicry (the headline scenario)

### Setup
The generator at every drift step:
1. Pulls the current production model's gain-ranked `feature_importance` from MLflow (or directly from the training-run artifact `outputs/feature_importance.csv`, which is the Phase B booster's gain ranking — see [04.1 §3.6](04.1_ml_design_example.md)). Take the top N (default 10).
2. For the fraud-generating subroutines, perturbs feature distributions toward the legitimate-traffic median for those top-N features, scaled by `adversary_strength ∈ [0,1]`.
3. The remaining features (lower-importance) keep their fraudulent signature.

This emulates real-world behaviour where attackers learn which signals are being scrutinized and "blend in" on those, while fraud must still happen on *some* axis.

**Note on which features get attacked.** With the current feature set (see [04.1 §3.3](04.1_ml_design_example.md)), the top-10 by gain typically includes a mix of: rolling card aggregates (`amount_zscore_card`, `card_amount_mean_so_far`), uid-group features (`uid2_amount_usd_mean`, `uid3_C13_std`, `uid2_freq`), identity (`device_brand`, `os_family`), and a handful of raw IEEE-CIS-derived columns (`amount_log`, `card_age_days`, `D15`). S3 perturbation should target *all* families, not just the trivially perturbable ones — for example, mimicking a high `uid2_freq` requires the adversary to actually generate prior tx volume on that uid, which is itself a costly behavioural change. The adversary cost asymmetry is the lesson.

### Why it matters for AI
- Top features lose information; the model's reliance on them becomes a weakness.
- Tests whether retraining recovers performance (it should, partially) and whether **feature engineering** must rotate (introducing new features the adversary hasn't yet learned to mimic).
- Forces the LLD to address **model robustness**: ensembling, feature dropout during training, monotonic constraints.

### Implementation
```python
def adversarial_perturb(fraud_row, top_features, legit_dist, strength):
    for f in top_features:
        legit_p50 = legit_dist[f].quantile(0.5)
        fraud_row[f] = (1-strength)*fraud_row[f] + strength*legit_p50
    return fraud_row
```

The model snapshot (`feature_importance.csv` + the booster used to compute pre-/post-S3 metrics) is fetched once per generator run, not per row. Pin a specific MLflow `model_uri` in the drift profile so the experiment is bit-reproducible.

### Evidence
- Run model A (baseline) on pre-S3 data: PR-AUC = X.
- Activate S3 with `strength=0.7`: PR-AUC drops to ≈ X − 0.15.
- Retrain on post-S3 data: PR-AUC partially recovers to ≈ X − 0.05; new feature importance now spreads across previously-low-rank features.
- This three-step before/after table is the centerpiece of the Section 03 deliverable.

---

## 6. Scenario S4 — Label Delay Extension

### Setup
- `chargeback_delay_lognormal.mu_days` shifts from 14 → 28 over 4 weeks (regulatory change, slower bank dispute pipeline).

### Why it matters for AI
- Effective training cutoff (`event_date ≤ today − 60d`) becomes too aggressive: by the new delay distribution, ≥ 90% of fraud labels for `today−60d` are *not yet* reported. Training on this window understates fraud rate, biasing the model toward under-flagging.
- Forces the LLD to make the cutoff dynamic: `cutoff = today − P95(cb_delay_distribution)`.
- Demonstrates that **label process** is itself a system component that drifts.

### Evidence
- `cb_delay_p95` metric tracked in monitoring; a panel shows the cutoff auto-extending.
- Training set size for fixed cutoff shrinks; we show the alternative dynamic-cutoff training set is stable.

---

## 7. Configuration & Reproducibility

```yaml
drift_profiles:
  baseline:
    tactic_mix: { card_testing: 0.5, ato: 0.3, collusion: 0.2 }
    channel_mix: { web: 0.6, mobile_app: 0.3, pos: 0.1 }
    adversary_strength: 0.0
    cb_delay_mu_days: 14

  scenario_s1:
    tactic_mix_curve:
      - { day: 0,  weights: { card_testing: 0.7, ato: 0.2, collusion: 0.1 } }
      - { day: 28, weights: { card_testing: 0.2, ato: 0.7, collusion: 0.1 } }

  scenario_s3_adv07:
    adversary_strength: 0.7
    model_uri: "models:/fraud_lgbm@production"
    refresh_top_features_every: "1d"

  combined_2024Q3:
    inherits: [scenario_s1, scenario_s2, scenario_s4_slow]
```

Profiles compose. Every run records the resolved profile + git SHA + model URI snapshot into `evidence/03_run_manifest.json` so any drift demo is bit-reproducible.

---

## 8. Failure Modes Introduced

| Risk | Mitigation |
|---|---|
| S3 reads a stale or wrong model | profile pins explicit `model_uri`; generator fails fast if not resolvable |
| Drift curves accidentally make fraud_rate ~0 | invariant assertion `0.001 ≤ realized_rate ≤ 0.05`; abort run on violation |
| Combined profiles produce contradictory mixes | normalize and warn; log the resolved mix |

---

## 9. Demonstration Procedure

The Section 03 deliverable is a runnable experiment:

```
make drift-demo PROFILE=scenario_s3_adv07
```

Which:
1. Spins up a 14-day simulated window in 14 minutes (compressed time).
2. Trains a baseline model on day-0 data.
3. Streams subsequent days through the existing scoring API.
4. Runs daily Evidently report comparing today vs baseline.
5. Triggers retrain at day-7, redeploys to a shadow alias, compares champion vs challenger from day-8 onward.
6. Emits `evidence/03_drift_report.html` and `evidence/03_kpi_timeseries.csv`.

---

## 10. Observability for Drift

New metrics emitted:

- `drift_psi{feature}` (gauge, hourly)
- `drift_score_ks` (gauge) — KS statistic between today's and last-week's score distributions
- `drift_calibration_brier` — on the rolling labeled window
- `cb_delay_p95_days` (gauge)
- `model_top_feature_set` (info metric, label-set hash) — alerts if top-features change set in unexpected ways

Alert rules:
- `drift_psi > 0.25 for 6h` AND `drift_calibration_brier increased > 20%` → page Risk-ML oncall.
- `cb_delay_p95_days > training_cutoff_days * 0.9` → warn (cutoff needs re-tuning).

---

## 11. Why These Four Together

The four scenarios are deliberately chosen so that each requires a *different* operator response:

| Scenario | Right response |
|---|---|
| S1 | Retrain on recent labels |
| S2 | Do NOT retrain; just verify calibration |
| S3 | Retrain + add new features; consider ensembling |
| S4 | Adjust training cutoff dynamically |

A system that responds the same way to all four is broken. The Section 03 evidence must show the system distinguishing them.

---

## 12. Deliverables

1. Generator extended with `drift_profile` mechanism (`services/generator/drift/`).
2. `evidence/03_run_manifest.json` per demo run.
3. Evidently HTML report, KPI CSV, and a short write-up `evidence/03_findings.md` (≤ 1 page) explaining what the model did and why.
4. Updated tests verifying each scenario produces the expected feature-distribution change (e.g., `assert PSI(amount_usd) > 0.2 in scenario_s1`).
