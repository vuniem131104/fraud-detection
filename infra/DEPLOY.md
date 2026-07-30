# Deploy Runbook — Fraud Detection Platform (from zero → running)

Hướng dẫn deploy toàn bộ hệ thống **từ số 0**, đánh dấu rõ bước nào **bạn phải làm tay**
và bước nào **script tự chạy**.

- 🔧 **MANUAL** — bạn phải tự làm (không tự động hoá được / cần input của bạn)
- ⚙️ **AUTO** — script/terraform lo, bạn chỉ chạy 1 lệnh

**Thứ tự phụ thuộc** (không nhảy cóc được):

```
0 Prereqs ──▶ 1 Aiven Kafka ──▶ 2 Terraform (GCP infra) ──▶ 3 Seed secrets
   ──▶ 4 Build/push images ──▶ 5 DB init ──▶ 6 VM (Ansible: MLflow+Airflow)
   ──▶ 7 Feast backfill DAG (apply+materialize) ──▶ 8 Train model + set storageUri
   ──▶ 9 deploy.sh (cluster deps + apps + nodes) ──▶ 10 Redis bootstrap ──▶ 11 Verify
```

> Runbook cho **project mới tinh** — chạy tuần tự bước 0 → 11, mọi thứ tạo mới từ đầu,
> **không import/adopt** hạ tầng cũ.

---

## 0. Prerequisites 🔧

Cài tools trên máy điều khiển (control node):

```bash
# gcloud, kubectl, helm, terraform>=1.5, ansible, uv (python), docker, htpasswd, openssl
gcloud components install kubectl gke-gcloud-auth-plugin   # nếu chưa có
helm version && terraform version && ansible --version && uv --version
```

Đăng nhập GCP (2 loại credential — cần cả 2):

```bash
gcloud auth login                        # cho gcloud CLI
gcloud auth application-default login     # ADC: cho terraform + cloud-sql-proxy local
gcloud config set project project-57f7ef9a-6059-4068-ae7
```

Clone repo + cài deps Python:

```bash
git clone <repo> && cd fraud-detection
uv sync
```

---

## 1. Aiven Kafka (external) 🔧

Kafka **không** do Terraform quản (dịch vụ managed bên ngoài GCP).

1. Tạo Aiven Kafka service (hoặc dùng cái đã có).
2. Tạo topic **`predictions`**.
3. Vào **Aiven console → service → Connection Info → SSL**, tải 3 file về thư mục `certs/`:

```
certs/ca.pem
certs/service.cert
certs/service.key
```

> `certs/` đã nằm trong `.gitignore`. `deploy.sh` sẽ tạo secret `kafka-certs` từ 3 file này.
> Cập nhật `BOOTSTRAP_SERVERS` trong các configmap nếu endpoint Aiven khác.

---

## 2. Infra GCP — Terraform ⚙️ (input 🔧)

Tạo mọi thứ trong GCP: GKE, Cloud SQL, Memorystore, GCS, Artifact Registry, Secret
Manager (container rỗng), IAM/SA, VM.

**2a.** 🔧 Tạo bucket chứa Terraform state (một lần duy nhất):

```bash
gcloud storage buckets create gs://fraud-detection-tfstatess \
  --project project-57f7ef9a-6059-4068-ae7 --location us-central1 \
  --uniform-bucket-level-access
gcloud storage buckets update gs://fraud-detection-tfstatess --versioning
```

**2b.** 🔧 Điền mật khẩu DB (không commit):

```bash
cd infra/terraform/environments/prod
cp secrets.auto.tfvars.example secrets.auto.tfvars
# sửa password của user vuniem
```

**2c.** ⚙️ Init + apply:

```bash
terraform init -backend-config="bucket=fraud-detection-tfstatess"

# Project mới tinh: tạo mới toàn bộ, không import gì cả.
terraform plan     # tất cả phải là "+ create", không có lỗi
terraform apply
```

Lấy các output cần cho bước sau:

```bash
terraform output    # cloud_sql_connection_name, redis_host, buckets, artifact_registry_url, vm_internal_ip
```

---

## 3. Seed giá trị Secret Manager 🔧

Terraform chỉ tạo **container rỗng**. Giá trị thật (mật khẩu) bạn nạp tay — không bao
giờ nằm trong git/state. External Secrets Operator (cài ở bước 9) sẽ sync vào cluster.

```bash
P=project-57f7ef9a-6059-4068-ae7
printf '%s' 'vuniem'                 | gcloud secrets versions add fraud-pg-user            --data-file=- --project $P
printf '%s' '<DB_PASSWORD>'          | gcloud secrets versions add fraud-pg-password        --data-file=- --project $P
printf '%s' 'admin'                  | gcloud secrets versions add fraud-grafana-admin-user --data-file=- --project $P
printf '%s' '<GRAFANA_ADMIN_PW>'     | gcloud secrets versions add fraud-grafana-admin-password --data-file=- --project $P
htpasswd -nbB admin '<INGRESS_PW>'   | gcloud secrets versions add fraud-ingress-basic-auth --data-file=- --project $P

gcloud secrets list --project $P     # phải thấy 5 secret
```

> Chi tiết rotation: `docs/Centralize Secret Management.md`.

---

## 4. Build & push Docker images 🔧

3 image app đẩy lên Artifact Registry (chart tham chiếu tag `:latest`).

```bash
P=project-57f7ef9a-6059-4068-ae7
AR=us-central1-docker.pkg.dev/$P/fraud-detection
gcloud auth configure-docker us-central1-docker.pkg.dev

# build context = repo root
docker build -f infra/docker/Dockerfile.fraudDetection    -t $AR/fraud-detection:latest    .
docker build -f infra/docker/Dockerfile.predictionWriter  -t $AR/prediction-writer:latest  .
docker build -f infra/docker/Dockerfile.driftDetection    -t $AR/drift-detection-api:latest .
docker push $AR/fraud-detection:latest
docker push $AR/prediction-writer:latest
docker push $AR/drift-detection-api:latest
```

---

## 5. Khởi tạo Database 🔧

Tạo schema `application` + seed dữ liệu giả (cần cho training, feature pipeline,
prediction-writer). Chạy qua **cloud-sql-proxy** local.

```bash
# Terminal 1: mở proxy tới Cloud SQL (ADC + role cloudsql.client trên user của bạn)
cloud-sql-proxy --port 5432 project-57f7ef9a-6059-4068-ae7:us-central1:fraud-detection

# Terminal 2: .env trỏ POSTGRES_HOST=127.0.0.1, POSTGRES_PORT=5432
uv run python scripts/initial/generate_fake_data.py      # tạo 6 bảng application.* + seed ~300k
```

> Database `mlflow` và `airflow` được MLflow/Airflow tự migrate (bước 6). DB
> `feast-registry` do DAG `feature_backfill` khởi tạo (bước 7).

---

## 6. VM Airflow + MLflow — Ansible ⚙️ (input 🔧)

VM (`airflow-mlflow-fraud-detection`) do Terraform tạo (đang STOPPED — khởi động khi
cần). Ansible cấu hình + deploy stack docker-compose (Airflow + MLflow + cloud-sql-proxy).

**6a.** 🔧 Start VM + điền inventory & vault:

```bash
gcloud compute instances start airflow-mlflow-fraud-detection --zone us-central1-a

cd infra/ansible
# inventory/prod/hosts.yml: ansible_host = IP VM, ansible_user = OS-Login user
# group_vars/all.yml: redis_host = <terraform output redis_host>
cp inventory/prod/group_vars/vault.yml.example inventory/prod/group_vars/vault.yml
# điền password (vuniem, JWT, admin...) rồi:
ansible-vault encrypt inventory/prod/group_vars/vault.yml
```

**6b.** ⚙️ Deploy:

```bash
ansible-galaxy collection install -r requirements.yml
ansible-playbook playbooks/site.yml --ask-vault-pass
# verify: curl http://<vm>:8090/health (Airflow) ; http://<vm>:5000 (MLflow)
```

---

## 7. Feast bootstrap — DAG `feature_backfill` 🔧

Feast được bootstrap **hoàn toàn trong container Airflow** (không cần venv/feast trên
control node). DAG `feature_backfill` chạy `feast apply` (đăng ký feature definitions
vào DB `feast-registry`) rồi **full materialize** toàn bộ history vào online store (Redis).
Kết nối DB đi qua `cloud-sql-proxy` (compose service) — không dùng private IP.

1. Mở Airflow UI (`http://<vm>:8090`).
2. Bật & **Trigger DAG w/ config** cho `feature_backfill`, sửa `start_date`/`end_date`
   phủ khoảng dữ liệu (mặc định `2026-07-02` → `2026-07-16`).
3. Xong: bật DAG `feature_pipeline` (`@daily`) để materialize incremental hằng ngày.

> Muốn xoá sạch online store (làm lại từ đầu): trigger DAG `feature_teardown` —
> nghịch đảo của `feature_backfill` (`feast teardown`).

---

## 8. Train model + set storageUri 🔧

Serving (KServe) load model từ GCS. Cần train → log vào MLflow → lấy đường dẫn artifact.

1. Chạy notebook `scripts/training/model_training.ipynb` (log run vào MLflow ở bước 6).
2. Vào MLflow UI → run → tab Artifacts → copy path `gs://fraud-detection-modelss/mlflow-artifacts/<exp>/<run-id>/artifacts`.
3. 🔧 Sửa `infra/k8s/helm/serving/values.yaml` → `predictor.storageUri` = path đó.

> Thư mục artifact phải chứa `model.bst` (+ `feature_schema.json`).

---

## 9. Deploy cluster + apps — deploy.sh ⚙️

Một phát: enable API → cluster deps (cert-manager, Knative, **Istio đã right-size**,
KServe+runtime, KEDA, ingress-nginx, ESO) → secrets/kafka-certs → configmaps →
app charts (requests đã nhẹ) → **node layout 2×4vCPU+1×2vCPU** → verify.

```bash
cd <repo-root>
./infra/deploy/deploy.sh                 # chạy hết, idempotent
# xem trước không chạy:   DRY_RUN=1 ./infra/deploy/deploy.sh
# chạy 1 phase:           ./infra/deploy/deploy.sh apps        (hoặc: istio | nodes | verify)
```

**9a.** 🔧 **Chỉ khi cluster mới toanh** — ingress-nginx nhận LB IP mới, phải sửa host rồi apply lại:

```bash
kubectl get svc -n ingress-nginx ingress-nginx-controller   # copy EXTERNAL-IP
# sửa infra/k8s/helm/fraud_detection/values.yaml:
#   ingress.host: "fraud-detection-api.<NEW-IP>.sslip.io"
./infra/deploy/deploy.sh apps
```

> Project mới → cluster mới nhận LB IP mới, nên **phải làm 9a**.
> Điều kiện tiên quyết của bước này: đã seed GSM (3), có images (4), có kafka-certs (1),
> đã set storageUri (8).

---

## 10. Redis velocity bootstrap 🔧 (tuỳ chọn)

Nạp sẵn velocity state vào Redis từ Postgres để on-demand serving không cold-start:

```bash
# proxy đang mở + .env trỏ REDIS_HOST=<memorystore ip>, POSTGRES_HOST=127.0.0.1
uv run python scripts/bootstrap_velocity_state.py     # idempotent
```

---

## 11. Verify ⚙️/🔧

```bash
./infra/deploy/deploy.sh verify           # pods / inferenceservice / scaledobject / externalsecret / releases

# smoke test API (basic-auth):
IP=$(kubectl get svc -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
curl http://fraud-detection-api.$IP.sslip.io/health
curl -u admin:'<INGRESS_PW>' -X POST http://fraud-detection-api.$IP.sslip.io/predict \
  -H 'Content-Type: application/json' -d '{"transaction_id":"txn-001", ...}'
```

---

## ✅ Checklist các bước MANUAL (tóm tắt)

| # | Bước MANUAL 🔧 |
|---|---|
| 0 | Cài tools + `gcloud auth login` + ADC |
| 1 | Aiven Kafka: topic `predictions` + 3 cert → `certs/` |
| 2a | Tạo bucket TF state |
| 2b | `secrets.auto.tfvars` (DB password) |
| 3 | Seed 5 giá trị Secret Manager |
| 4 | Build & push 3 images |
| 5 | `generate_fake_data.py` (schema + seed) |
| 6a | Start VM + inventory + vault |
| 7 | Trigger DAG `feature_backfill` (apply + materialize) |
| 8 | Train model + set `serving` storageUri |
| 9a | Sửa ingress host (cluster mới → LB IP mới) |
| 10 | Redis velocity bootstrap (tuỳ chọn) |

**Bước AUTO ⚙️:** 2c `terraform apply`, 6b `ansible-playbook`, 9 `deploy.sh`, 11 `verify`.

---

## Tài liệu liên quan

| Chủ đề | File |
|---|---|
| Terraform (GCP infra) | [terraform/README.md](terraform/README.md) |
| Deploy script (cluster + apps) | [deploy/README.md](deploy/README.md) |
| Ansible (VM) | [ansible/README.md](ansible/README.md) |
| Secret Manager + ESO | [../docs/Centralize Secret Management.md](../docs/Centralize%20Secret%20Management.md) |
| KServe / KEDA / từng service | `docs/Deploy *.md`, `docs/Install *.md` |
