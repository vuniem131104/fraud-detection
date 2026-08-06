# Deploy Drift Detection API on GKE

This guide covers deploying the `drift-detection` REST API to GKE (namespace: `core`).
The service computes the **Population Stability Index (PSI)** on the `amount_usd` column,
comparing the **last 30 days** of transactions in Postgres against the **training baseline**
(`dataset/training_data.parquet`, baked into the image).

Unlike the Fraud Detection API, this service is **internal only** (ClusterIP, no Ingress) —
it is scraped/queried by Grafana and other in-cluster tools. It uses Cloud SQL Proxy for
database connectivity and Workload Identity for GCP authentication.

> **Prerequisites**
> - GKE cluster running with Workload Identity enabled (`--workload-pool`)
> - `drift-detection-api` Docker image built and pushed to Artifact Registry
> - GCP Service Account (`drift-detection-api-sa`) with `roles/artifactregistry.reader` and `roles/cloudsql.client`
> - Cloud SQL (PostgreSQL) instance with `application.transactions` populated

---

## Architecture Overview

```
In-cluster clients (Grafana, cron, ad-hoc)
    │  GET /detect
    ▼
Service: drift-detection-api (ClusterIP :80)   →  namespace: core
    │
    ▼
Pod: drift-detection-api (port 8001)
    │   ├── training baseline (amount_usd) baked in at /app/dataset/training_data.parquet
    │   └── queries last 30 days of amount_usd from Postgres
    │
    └── Sidecar: cloud-sql-proxy               ←── Cloud SQL IAM auth via Workload Identity
```

Endpoints:

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness probe (static `ok`) |
| GET | `/ready` | Readiness — Postgres reachable + baseline loaded |
| GET | `/detect?threshold=0.1` | Run PSI drift on `amount_usd` (last 30 days) vs. baseline |

---

## Step 1 — Build and Push the Image

The image is built from [`infra/docker/Dockerfile.driftDetection`](../infra/docker/Dockerfile.driftDetection).
It copies `src/database`, `src/drift_detection`, and bakes in the baseline parquet.

> The baseline lives under `dataset/`, which is excluded by `.dockerignore`; a `!dataset/training_data.parquet`
> exception re-includes just that file so the `COPY` succeeds. Make sure `dvc pull` has fetched the parquet before building.

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export IMAGE="us-central1-docker.pkg.dev/${PROJECT_ID}/fraud-detection/drift-detection-api:latest"

# From the repo root (build context = repo root)
docker build -f infra/docker/Dockerfile.driftDetection -t "$IMAGE" .
docker push "$IMAGE"
```

---

## Step 2 — Create GCP Service Account (if not done)

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export GSA_NAME="drift-detection-api-sa"

gcloud iam service-accounts create $GSA_NAME \
  --display-name="Drift Detection API Service Account"

# Pull images from Artifact Registry
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/artifactregistry.reader" \
  --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Connect to Cloud SQL
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/cloudsql.client" \
  --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

---

## Step 3 — Create Namespace (skip if it already exists)

```bash
kubectl create namespace core
```

---

## Step 4 — Bind Workload Identity

Allows the K8s SA (`drift-detection-api-sa` in namespace `core`) to authenticate as the GCP SA.

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export NAMESPACE="core"
export KSA_NAME="drift-detection-api-sa"
export GSA_NAME="drift-detection-api-sa"

gcloud iam service-accounts add-iam-policy-binding \
  ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]"
```

> **Note:** The K8s SA annotation (`iam.gke.io/gcp-service-account`) is added automatically by the
> Helm chart via `serviceAccount.gcpServiceAccount` in `values.yaml`.

---

## Step 5 — Apply ConfigMap and Secrets

### ConfigMap

```bash
kubectl apply -f infra/k8s/manifests/drift-detection-configmap.yaml
```

Key config values (see [`drift-detection-configmap.yaml`](../infra/k8s/manifests/drift-detection-configmap.yaml)):

| Key | Description |
|---|---|
| `POSTGRES_HOST` | `127.0.0.1` (Cloud SQL Proxy listens locally) |
| `BASELINE_DATA_PATH` | `/app/dataset/training_data.parquet` (baked into the image) |
| `DRIFT_DETECTION_API_WORKERS` | uvicorn worker count |

### App Secret

```bash
kubectl apply -f infra/k8s/manifests/drift-detection-secrets.yaml
```

Contains: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `CLOUD_SQL_INSTANCE`.

> Both manifests set `namespace: core` explicitly, so the deployment's `envFrom` resolves them.

---

## Step 6 — Configure `values.yaml`

Edit [`infra/k8s/helm/drift_detection/values.yaml`](../infra/k8s/helm/drift_detection/values.yaml):

```yaml
namespace: core

api:
  name: drift-detection-api
  replicaCount: 1
  image:
    repository: us-central1-docker.pkg.dev/<project>/fraud-detection/drift-detection-api
    tag: "latest"        # or a specific tag from CI/CD
  port: 8001

serviceAccount:
  name: drift-detection-api-sa
  gcpServiceAccount: "drift-detection-api-sa@<PROJECT_ID>.iam.gserviceaccount.com"

service:
  type: ClusterIP
  port: 80
  targetPort: http
```

---

## Step 7 — Deploy with Helm

`--rollback-on-failure` auto-rolls back the release if the upgrade fails (Helm 4 flag; on
Helm 3 use `--atomic`). `--cleanup-on-fail` removes any resources created during a failed upgrade.

```bash
helm upgrade --install drift-detection-api \
  ./infra/k8s/helm/drift_detection \
  --namespace core \
  --values ./infra/k8s/helm/drift_detection/values.yaml \
  --rollback-on-failure \
  --timeout 10m \
  --cleanup-on-fail
```

---

## Step 8 — Verify Deployment

```bash
# Expect 2/2 Running — app + cloud-sql-proxy
kubectl get pods -n core -l app=drift-detection-api

# App logs (should show "Baseline loaded" with row count on startup)
kubectl logs -n core deployment/drift-detection-api -c drift-detection-api

# Cloud SQL Proxy logs
kubectl logs -n core deployment/drift-detection-api -c cloud-sql-proxy
```

Expected pod status:
```
NAME                                  READY   STATUS    RESTARTS
drift-detection-api-xxxxxxxxxx-xxxxx  2/2     Running   0
```

---

## Step 9 — Test the API

The service is ClusterIP (internal), so port-forward for a quick check:

```bash
kubectl port-forward -n core svc/drift-detection-api 8001:80

# In another terminal:
curl http://localhost:8001/health
curl http://localhost:8001/ready
curl "http://localhost:8001/detect?threshold=0.1"
```

Example `/detect` response:
```json
{
  "column": "amount_usd",
  "drift_detected": false,
  "psi": 0.0163,
  "psi_label": "no_drift",
  "threshold": 0.1,
  "n_current": 12043,
  "current_mean": 58.91,
  "current_std": 41.22,
  "baseline_mean": 59.0,
  "baseline_std": 41.87
}
```

Internal URL for Grafana / other in-cluster clients:
```
http://drift-detection-api.core.svc.cluster.local/detect
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `/ready` 503 "Baseline not loaded" | Parquet missing from image | Rebuild image; ensure `dvc pull` ran and `.dockerignore` re-includes the parquet |
| `/detect` 422 "Only N rows found" | < 30 transactions in the last 30 days | Wait for more data, or lower the window (code) |
| `cloud-sql-proxy` crash | Workload Identity not bound | Re-run Step 4 |
| `ImagePullBackOff` | SA missing `artifactregistry.reader` | Grant the role (Step 2) |
| `/ready` 503 "Database unreachable" | Cloud SQL Proxy not ready / wrong `CLOUD_SQL_INSTANCE` | Check proxy logs + the secret value |

---

## Uninstall

```bash
helm uninstall drift-detection-api --namespace core
kubectl delete -f infra/k8s/manifests/drift-detection-configmap.yaml
kubectl delete -f infra/k8s/manifests/drift-detection-secrets.yaml
```

---

## Reference

| Resource | Path |
|---|---|
| Helm chart | `infra/k8s/helm/drift_detection/` |
| Dockerfile | `infra/docker/Dockerfile.driftDetection` |
| ConfigMap | `infra/k8s/manifests/drift-detection-configmap.yaml` |
| Secrets | `infra/k8s/manifests/drift-detection-secrets.yaml` |
| App code | `src/drift_detection/` |
| Fraud API deploy guide | `docs/Deploy Fraud Detection API.md` |
