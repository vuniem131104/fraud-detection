# Deploy Prediction Writer on GKE

This guide covers deploying the `prediction-writer` background worker to GKE (namespace: `core`).
The worker consumes scored-transaction messages from the Kafka **`predictions`** topic (Aiven,
SSL) and writes each message to two Postgres tables:

- `application.transactions` — the transaction details
- `application.prediction_logs` — the model's prediction output

Unlike the APIs, this is a **pure consumer**: no Service, no Ingress, no HTTP ports. It polls
Kafka in batches (`KAFKA_MAX_RECORDS`, default 100), dispatches each message, and commits
offsets **manually** only after a batch is fully written — auto-commit is disabled, so a crash
mid-batch means redelivery, not data loss.

Scaling is handled by **KEDA** on Kafka consumer lag, including **scale-to-zero**: with no lag
on the `predictions` topic, the Deployment sits at 0 replicas. This is normal.

> **Prerequisites**
> - GKE cluster with Workload Identity enabled (`--workload-pool`)
> - **KEDA installed** — the chart includes a `ScaledObject` and `TriggerAuthentication`, so
>   the install fails without the KEDA CRDs (see [`Install KEDA.md`](Install%20KEDA.md))
> - `prediction-writer` Docker image built and pushed to Artifact Registry
> - GCP Service Account (`prediction-writer-sa`) with `roles/artifactregistry.reader` and `roles/cloudsql.client`
> - Aiven Kafka service with the `predictions` topic + SSL certs
> - Cloud SQL (PostgreSQL) instance with the `application` schema

---

## Architecture Overview

```
Kafka (Aiven, SSL): topic "predictions"
    │  batched poll (≤100 msgs / 1s), consumer group: prediction-writer
    ▼
Pod: prediction-writer                          →  namespace: core
    │   ├── INSERT INTO application.transactions
    │   └── INSERT INTO application.prediction_logs
    │   └── manual offset commit after the batch
    │
    └── Sidecar: cloud-sql-proxy                ←── Cloud SQL IAM auth via Workload Identity
            ▲
KEDA ScaledObject ── watches consumer lag ──▶ scales Deployment 0 ↔ 2 replicas
    └── TriggerAuthentication: reads TLS certs from the kafka-certs secret
```

KEDA scaling parameters (see [`scaled-object.yaml`](../infra/k8s/helm/prediction_writer/templates/scaled-object.yaml)):

| Parameter | Value | Meaning |
|---|---|---|
| `minReplicaCount` | 0 | Scale to zero when the topic is drained |
| `maxReplicaCount` | 2 | Cap (matches topic partitioning; more pods than partitions sit idle) |
| `lagThreshold` | 3 | Target lag per replica |
| `activationLagThreshold` | 0 | Any lag > 0 wakes the worker from zero |
| `pollingInterval` / `cooldownPeriod` | 30s / 120s | Lag check frequency / wait before scaling back down |

---

## Step 1 — Build and Push the Image

The image is built from [`infra/docker/Dockerfile.predictionWriter`](../infra/docker/Dockerfile.predictionWriter).
It installs only `asyncpg`, `aiokafka`, and `structlog`, and copies `src/database` +
`src/workers/` (base worker + Postgres worker).

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export IMAGE="us-central1-docker.pkg.dev/${PROJECT_ID}/fraud-detection/prediction-writer:latest"

# From the repo root (build context = repo root)
docker build -f infra/docker/Dockerfile.predictionWriter -t "$IMAGE" .
docker push "$IMAGE"
```

---

## Step 2 — Create GCP Service Account (if not done)

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export GSA_NAME="prediction-writer-sa"

gcloud iam service-accounts create $GSA_NAME \
  --display-name="Prediction Writer Service Account"

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

Allows the K8s SA (`prediction-writer-sa` in namespace `core`) to authenticate as the GCP SA.
Wait ~30s after binding before deploying so the binding propagates.

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"
export NAMESPACE="core"
export KSA_NAME="prediction-writer-sa"
export GSA_NAME="prediction-writer-sa"

gcloud iam service-accounts add-iam-policy-binding \
  ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]"
```

> **Note:** The K8s SA annotation (`iam.gke.io/gcp-service-account`) is added automatically by
> the Helm chart via `serviceAccount.gcpServiceAccount` in `values.yaml`.

---

## Step 5 — Apply ConfigMap and Secrets

> Unlike the drift-detection manifests, these do **not** set `namespace:` in metadata — pass
> `-n core` explicitly.

### ConfigMap

```bash
kubectl apply -f infra/k8s/manifests/prediction-writer-configmap.yaml -n core
```

Key config values (see [`prediction-writer-configmap.yaml`](../infra/k8s/manifests/prediction-writer-configmap.yaml)):

| Key | Description |
|---|---|
| `PREDICTIONS_TOPIC` / `KAFKA_GROUP_ID` | Topic `predictions`, consumer group `prediction-writer` (must match the KEDA trigger) |
| `BOOTSTRAP_SERVERS` / `KAFKA_SECURITY_PROTOCOL` | Aiven Kafka endpoint, `SSL` |
| `KAFKA_MAX_RECORDS` / `KAFKA_TIMEOUT_MS` | Batch size (100) / poll timeout (1s) |
| `KAFKA_SSL_CAFILE` / `_CERTFILE` / `_KEYFILE` | Cert paths under `/app/certs` (mounted from the `kafka-certs` secret) |
| `POSTGRES_HOST` | `127.0.0.1` (Cloud SQL Proxy listens locally) |

### App Secret

```bash
kubectl apply -f infra/k8s/manifests/prediction-writer-secrets.yaml -n core
```

Contains: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `CLOUD_SQL_INSTANCE`.

### Kafka TLS Certs (shared with the Fraud Detection API — skip if already created)

Both the worker's volume mount **and** KEDA's `TriggerAuthentication` read this secret:

```bash
kubectl create secret generic kafka-certs \
  --namespace=core \
  --from-file=ca.pem=/path/to/ca.pem \
  --from-file=service.cert=/path/to/service.cert \
  --from-file=service.key=/path/to/service.key
```

> Certs are downloaded from the **Aiven console** → your Kafka service → Connection Info → SSL.

---

## Step 6 — Deploy with Helm

```bash
helm upgrade --install prediction-writer \
  ./infra/k8s/helm/prediction_writer \
  --namespace core \
  --values ./infra/k8s/helm/prediction_writer/values.yaml \
  --rollback-on-failure \
  --timeout 10m \
  --cleanup-on-fail
```

> `--rollback-on-failure` is the Helm 4 flag (on Helm 3 use `--atomic`).

---

## Step 7 — Verify Deployment

```bash
# KEDA resources
kubectl get scaledobject,triggerauthentication -n core

# Pods — 2/2 (worker + cloud-sql-proxy) when there is lag; 0 pods when the topic is drained
kubectl get pods -n core -l app=prediction-writer

# Worker logs (should show consumer started with topic/group, then batch writes)
kubectl logs -n core deployment/prediction-writer -c prediction-writer

# Cloud SQL Proxy logs
kubectl logs -n core deployment/prediction-writer -c cloud-sql-proxy
```

> **`No resources found` / 0 replicas is not an error** — with `minReplicaCount: 0`, KEDA
> scales the worker away when there is no consumer lag. It wakes up within ~30s
> (`pollingInterval`) of new messages arriving.

---

## Step 8 — End-to-End Test

Send a transaction through the Fraud Detection API (it publishes the scored result to the
`predictions` topic), then confirm the rows landed:

```bash
# Watch the worker wake up and process
kubectl get pods -n core -l app=prediction-writer -w

# Then verify in Postgres (e.g. via cloud-sql-proxy locally or the Grafana Postgres datasource)
# SELECT count(*) FROM application.transactions;
# SELECT count(*) FROM application.prediction_logs ORDER BY 1 DESC;
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Helm install fails: `no matches for kind "ScaledObject"` | KEDA not installed | Install KEDA first ([`Install KEDA.md`](Install%20KEDA.md)) |
| Pods stuck at 0 despite lag | KEDA can't auth to Kafka | `kubectl describe scaledobject prediction-writer -n core`; check `kafka-certs` keys (`ca.pem`, `service.cert`, `service.key`) |
| Pod stuck `0/2` / `CreateContainerConfigError` | `kafka-certs`, configmap, or secret missing in `core` | Re-run Step 5 with `-n core` |
| `cloud-sql-proxy` crash / `403` | Workload Identity not bound or not yet propagated | Re-run Step 4, wait ~30s, delete the pod |
| Kafka SSL errors in worker logs | Wrong/expired Aiven certs | Re-download certs and recreate the `kafka-certs` secret |
| Messages re-processed after a crash | Expected — offsets commit only after a full batch | Inserts should stay idempotent (conflict-safe) if exactly-once matters |
| Pods flapping 0↔1 | Trickle of messages + `activationLagThreshold: 0` | Raise `activationLagThreshold` or `cooldownPeriod` in `scaled-object.yaml` |
| `ImagePullBackOff` | SA missing `artifactregistry.reader` | Grant the role (Step 2) |

---

## Uninstall

```bash
helm uninstall prediction-writer --namespace core
kubectl delete -f infra/k8s/manifests/prediction-writer-configmap.yaml -n core
kubectl delete -f infra/k8s/manifests/prediction-writer-secrets.yaml -n core
# kafka-certs is shared with the Fraud Detection API — only delete if nothing else uses it
```

---

## Reference

| Resource | Path |
|---|---|
| Helm chart | `infra/k8s/helm/prediction_writer/` |
| Dockerfile | `infra/docker/Dockerfile.predictionWriter` |
| ConfigMap | `infra/k8s/manifests/prediction-writer-configmap.yaml` |
| Secrets | `infra/k8s/manifests/prediction-writer-secrets.yaml` |
| Worker code | `src/workers/postgres/postgres_worker.py` (+ `src/workers/base_worker.py`) |
| KEDA install guide | `docs/Install KEDA.md` |
| Fraud API deploy guide | `docs/Deploy Fraud Detection API.md` |
