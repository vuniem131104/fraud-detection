# A/B Testing Demo — Fraud Detection Model

> **Status: DEMO / chưa implement.** Toàn bộ code trong file này là bản thiết kế minh hoạ,
> viết khớp với code thật của repo (tên bảng, env vars, cấu trúc chart) để khi cần
> có thể copy vào implement ngay.

Mục tiêu: chạy 2 version model song song trong production, chia traffic có kiểm soát,
thu thập metrics theo từng version và quyết định promote/rollback dựa trên số liệu.

**Nguyên tắc thiết kế: chỉ deploy model là traffic tự chia.** Việc split là của KServe
(`canaryTrafficPercent`), app không biết gì về experiment — không routing logic,
không hash user, không config A/B trong scoring service.

---

## 1. Bug hiện tại: `model_version` lấy từ env var

[predict.py](../src/fraud_detection/core/predict.py) đang ghi `model_version` vào Kafka payload
bằng `os.getenv("MODEL_VERSION")` — tức là "version tôi *nghĩ* tôi đang gọi", không phải
"version *thực sự* đã chấm điểm". Chỉ cần `helm upgrade` serving với storageUri mới là
mọi log trong lúc rollout đã ghi sai version, chưa cần A/B gì cả. Khi bật canary split
thì 20% log sai hoàn toàn → không phân tích được.

**Fix đúng gốc: model tự khai version của nó trong inference response**, service chỉ ghi
lại những gì response nói. Attribution đúng 100% bất kể traffic được chia kiểu gì.

```
Client ──► predict.py ──► KServe (canary split 80/20 ở tầng revision)
                │              ├── revision N   (model v2) ─┐ response body có
                │              └── revision N+1 (model v3) ─┘ model_version THẬT
                │
                └──► Kafka (payload + model_version từ response)
                          └──► postgres_worker ──► prediction_logs
                                                        │
              Prometheus (metrics theo revision_name)   ▼
                          │                    cron/Airflow: ab_analysis.py
                          ▼                    (GROUP BY model_version)
              Grafana panel "A vs B"                    │
                                                        ▼
                                    đủ evidence → promote (canary.enabled=false)
                                    guardrail vỡ → rollback (trafficPercent=0)
```

---

## 2. Model artifact tự mang version — `model-settings.json`

MLServer (runtime serve LightGBM v2 protocol) tự đọc `model-settings.json` nằm cạnh
model artifact; field `version` được trả về trong **mọi** inference response.
Thêm vào bước cuối của training pipeline:

```python
# ===== bước cuối của training pipeline (Airflow retrain) =====
import mlflow

with mlflow.start_run() as run:
    ...  # train + log model như hiện tại
    model_settings = {
        "name": "fraud-detection",
        "implementation": "mlserver_lightgbm.LightGBMModel",
        "parameters": {
            "uri": "./model.bst",
            "version": run.info.run_id[:8],   # version = mlflow run id
        },
    }
    mlflow.log_dict(model_settings, "model-settings.json")
```

Response của KServe v2 giờ thành:

```json
{
  "model_name": "fraud-detection",
  "model_version": "e546307b",
  "outputs": [{"name": "output-0", "data": [0.35]}]
}
```

---

## 3. predict.py — sửa 3 dòng (fix bug, không phải thêm feature)

```python
# ===== predict_with_kserve trả thêm version từ response =====
    result = response.json()
    probability = float(result["outputs"][0]["data"][0])
    model_version = result.get("model_version")        # ← version THẬT đã serve
    return probability, model_version

# trong predict():
    probability, served_version = await self.predict_with_kserve(vector)
    ...
    # Kafka payload
    "model_version": served_version or os.getenv("MODEL_VERSION"),  # fallback env
```

Hết phần code. Không if/else variant, không env A/B nào trong app. Kể cả deploy thường
không A/B, log từ giờ luôn đúng version.

`prediction_logs` không cần migration: cột `model_version` đã có sẵn, giờ nó chứa
giá trị đúng thay vì giá trị env tĩnh.

---

## 4. Hạ tầng: canary split bằng KServe

### 4.1. Chart serving hỗ trợ canary + tag routing

[inference-service.yaml](../infra/k8s/helm/serving/templates/inference-service.yaml):

```yaml
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: {{ .Values.modelName }}
  annotations:
    autoscaling.knative.dev/min-scale: "{{ .Values.autoscaling.minReplicas }}"
    autoscaling.knative.dev/max-scale: "{{ .Values.autoscaling.maxReplicas }}"
    autoscaling.knative.dev/target: "{{ .Values.autoscaling.scaleTarget }}"
    serving.kserve.io/enable-tag-routing: "true"   # smoke test đích danh revision qua tag URL
spec:
  predictor:
    serviceAccountName: {{ .Values.serviceAccount.name }}
    {{- if .Values.canary.enabled }}
    canaryTrafficPercent: {{ .Values.canary.trafficPercent }}
    {{- end }}
    model:
      ...  # giữ nguyên
```

[values.yaml](../infra/k8s/helm/serving/values.yaml) thêm:

```yaml
canary:
  enabled: false
  trafficPercent: 20
```

### 4.2. Cách KServe canary hoạt động

Vẫn chỉ có **một** InferenceService. Mỗi lần spec predictor thay đổi, Knative tạo một
**revision** mới (immutable snapshot). KServe nhớ revision nào đang là "latest rolled out"
(từng nhận 100% traffic):

- Đổi `storageUri` + set `canaryTrafficPercent: 20` → revision cũ nhận 80%,
  revision mới nhận 20%. Mỗi request chỉ đi vào **đúng một** revision (split thật,
  không phải copy).
- Chỉ đổi `trafficPercent` **không** tạo revision mới — KServe chỉ điều chỉnh tỷ lệ
  giữa 2 revision sẵn có, nên tăng dần rất rẻ.
- Promote = bỏ `canaryTrafficPercent` → revision mới thành rolled out, revision cũ scale về 0.
- Rollback = `canaryTrafficPercent: 0` → traffic về hết revision cũ, revision mới vẫn
  đứng đó để debug.

### 4.3. Vòng đời một lần release

```bash
# 1. Model mới ra canary 20% — chỉ một lệnh, không đụng configmap/API
helm upgrade fraud-serving ./infra/k8s/helm/serving \
  --set predictor.storageUri="gs://fraud-detection-modelss/mlflow-artifacts/1/<new_run>/artifacts" \
  --set canary.enabled=true --set canary.trafficPercent=20 \
  --rollback-on-failure

# Kiểm tra split thực tế
kubectl get isvc fraud-detection -n serving \
  -o jsonpath='{.status.components.predictor.traffic}' | jq
# tag "prev"   → revision cũ  (80%)
# tag "latest" → revision mới (20%)

# 2. Smoke test đích danh model mới qua tag URL (không đụng traffic thật)
curl http://latest-fraud-detection.serving.svc.cluster.local/v2/models/fraud-detection/infer \
  -d @sample_payload.json

# 3. Tăng dần / promote / rollback — chỉ là đổi số
helm upgrade fraud-serving ./infra/k8s/helm/serving --reuse-values --set canary.trafficPercent=50
helm upgrade fraud-serving ./infra/k8s/helm/serving --reuse-values --set canary.enabled=false   # promote
helm upgrade fraud-serving ./infra/k8s/helm/serving --reuse-values --set canary.trafficPercent=0 # rollback
```

---

## 5. Hai tầng metrics

### Tầng 1 — Operational (real-time, Prometheus/Grafana)

Trả lời "model mới có làm hỏng hệ thống không" — biết sau vài phút.
Knative export sẵn label `revision_name`:

```promql
# latency p99 theo revision
histogram_quantile(0.99,
  sum(rate(revision_app_request_latencies_bucket{configuration_name="fraud-detection-predictor"}[5m]))
  by (le, revision_name))

# error rate theo revision
sum(rate(revision_app_request_count{response_code_class="5xx"}[5m])) by (revision_name)
```

### Tầng 2 — ML quality (từ `prediction_logs`)

Trả lời "model mới có *tốt hơn* không". Chia làm hai vì fraud label về chậm:

**Proxy metrics — có ngay, không cần label:**

```sql
SELECT
  model_version,
  count(*)                                                   AS n_requests,
  avg(fraud_score)                                           AS mean_score,
  percentile_cont(0.5)  WITHIN GROUP (ORDER BY fraud_score)  AS p50_score,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY fraud_score)  AS p95_score,
  avg(prediction::float)                                     AS alert_rate,
  avg(latency_ms)                                            AS avg_latency
FROM application.prediction_logs
WHERE created_at > now() - interval '24 hours'
GROUP BY model_version;
```

**Alert rate là guardrail quan trọng nhất**: nếu model mới flag 8% giao dịch trong khi
model cũ flag 2%, bạn đang chặn nhầm khách hàng hàng loạt — rollback ngay, không cần đợi label.

**True metrics — khi label về (`application.labels`, trễ vài ngày–vài tuần):**

```sql
SELECT
  p.model_version,
  sum((p.prediction = 1 AND l.label = 1)::int)::float
    / nullif(sum(p.prediction), 0)                        AS precision,
  sum((p.prediction = 1 AND l.label = 1)::int)::float
    / nullif(sum(l.label), 0)                             AS recall,
  sum((p.prediction = 1 AND l.label = 0)::int)            AS false_positives
FROM application.prediction_logs p
JOIN application.labels l ON l.transaction_id = p.transaction_id
WHERE p.created_at > now() - interval '30 days'
GROUP BY p.model_version;
```

---

## 6. Phân tích: `scripts/ab_analysis.py`

```python
"""So sánh 2 model version từ prediction_logs: proxy metrics ngay lập tức,
true metrics khi application.labels có ground truth."""
import os
import sys
import pandas as pd
import psycopg
from scipy import stats  # có sẵn qua scikit-learn

VERSION_A, VERSION_B = sys.argv[1], sys.argv[2]  # ví dụ: e546307b abc12345

conn = psycopg.connect(
    host=os.environ["POSTGRES_HOST"], port=os.environ["POSTGRES_PORT"],
    user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["POSTGRES_DB"],
)

# ---- Proxy metrics: không cần label ----
df = pd.read_sql("""
    SELECT model_version, fraud_score, prediction, latency_ms
    FROM application.prediction_logs
    WHERE created_at > now() - interval '7 days'
      AND model_version = ANY(%s)
""", conn, params=([VERSION_A, VERSION_B],))
a = df[df.model_version == VERSION_A]
b = df[df.model_version == VERSION_B]

print(df.groupby("model_version").agg(
    n=("prediction", "size"),
    alert_rate=("prediction", "mean"),
    mean_score=("fraud_score", "mean"),
    p95_score=("fraud_score", lambda s: s.quantile(0.95)),
    p99_latency=("latency_ms", lambda s: s.quantile(0.99)),
))

# Alert rate khác nhau là thật hay noise? (two-proportion z-test)
n_a, n_b = len(a), len(b)
p_pool = (a.prediction.sum() + b.prediction.sum()) / (n_a + n_b)
se = (p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b)) ** 0.5
z = (b.prediction.mean() - a.prediction.mean()) / se
print(f"alert-rate z={z:.2f} p={2 * stats.norm.sf(abs(z)):.4f}")

# Phân phối score có lệch không? (Kolmogorov-Smirnov)
ks, p = stats.ks_2samp(a.fraud_score, b.fraud_score)
print(f"score KS={ks:.3f} p={p:.4f}")

# ---- True metrics: join với labels (về trễ vài ngày) ----
truth = pd.read_sql("""
    SELECT p.model_version, p.prediction, l.label
    FROM application.prediction_logs p
    JOIN application.labels l ON l.transaction_id = p.transaction_id
    WHERE p.created_at > now() - interval '30 days'
      AND p.model_version = ANY(%s)
""", conn, params=([VERSION_A, VERSION_B],))
for v, g in truth.groupby("model_version"):
    tp = ((g.prediction == 1) & (g.label == 1)).sum()
    fp = ((g.prediction == 1) & (g.label == 0)).sum()
    fn = ((g.prediction == 0) & (g.label == 1)).sum()
    print(f"{v}: precision={tp / max(tp + fp, 1):.3f} "
          f"recall={tp / max(tp + fn, 1):.3f} n_fraud={tp + fn}")
```

Chạy định kỳ bằng cron/Airflow, hoặc đưa 2 query ở mục 5 vào Grafana
(Postgres datasource đã có) làm row "A/B experiment": request split theo model_version,
p99 latency theo revision, alert rate và mean fraud_score theo model_version.

`p < 0.05` → khác biệt có ý nghĩa thống kê, không phải noise.

---

## 7. Trade-off của canary split so với hash trong app

Mất duy nhất một thứ: **stickiness** — KServe split ngẫu nhiên theo *request* chứ không
theo *user*, một user gọi 2 lần có thể gặp 2 model. Với fraud scoring thì chấp nhận được:
user không nhìn thấy score, và về mặt thống kê so sánh 2 model vẫn valid (mẫu ngẫu nhiên).
Chỉ khi cần "một user luôn được đối xử nhất quán" (pricing/UI experiment) mới phải
quay lại cách hash user trong app.

Đổi lại:

- App không biết gì về experiment — đúng separation of concerns.
- Deploy/tăng %/promote/rollback đều là một lệnh helm, không rollout lại API.
- Bug env-version được fix vĩnh viễn — kể cả deploy thường không A/B, log luôn đúng version.

---

## 8. Bẫy cần biết khi phân tích

- **Sample size**: fraud rate thường ~0.1–1%, nên để so precision/recall cần *rất nhiều*
  giao dịch trong nhánh canary. 20% traffic vài ngày có thể chỉ có vài chục fraud case —
  chạy power analysis trước để biết cần chạy experiment bao lâu, đừng kết luận sớm.
- **Feedback loop đặc thù fraud**: giao dịch bị model chặn thì không bao giờ có ground
  truth (không biết nó có thật là fraud không) — chỉ quan sát được label trên tập *đã
  được approve*, làm precision/recall bị bias. Các team hoặc chấp nhận bias này, hoặc
  cho qua một tỷ lệ nhỏ giao dịch nghi ngờ để giữ tập unbiased.
- **Scale-to-zero làm bẩn số liệu latency**: revision canary cũng chịu autoscale
  annotations; nếu để nó scale về 0 thì cold start làm p99 của model mới xấu oan —
  giữ `min-scale: 1` trong lúc chạy experiment.
- **Đừng nhìn số thô**: "recall B cao hơn A 2 điểm" với sample nhỏ có thể là noise —
  luôn chốt promote/rollback bằng significance test (mục 6).
