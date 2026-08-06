# Deploy Fraud Detection API on GKE

This guide covers the full deployment of the `fraud-detection` REST API to GKE (namespace: `core`), using NGINX Ingress Controller for external access, Cloud SQL Proxy for database connectivity, and Workload Identity for GCP authentication.

> **Prerequisites**
> - GKE cluster running with Workload Identity enabled (`--workload-pool`)
> - Docker image pushed to Artifact Registry
> - GCP Service Account (`fraud-detection-sa`) created with `roles/artifactregistry.reader` and `roles/cloudsql.client`
> - Cloud SQL (PostgreSQL) instance exists
> - Aiven Kafka cluster with TLS certs available

---

## Architecture Overview

```
Internet
    │
    ▼ HTTP/HTTPS
NGINX Ingress Controller  (LoadBalancer, namespace: ingress-nginx)
    │  ✓ Basic Auth (htpasswd)
    │  ✓ Rate limiting (10 RPS)
    │  ✓ Max body size (50m)
    ▼
Service: fraud-detection (ClusterIP :80)  →  namespace: core
    │
    ▼
Pod: fraud-detection (port 1311)          ←── reads from Redis, calls KServe
    │
    └── Sidecar: cloud-sql-proxy          ←── Cloud SQL IAM auth via Workload Identity
```

---

## Step 1 — Install NGINX Ingress Controller

```bash
helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx \
  --namespace ingress-nginx --create-namespace \
  --set controller.service.type=LoadBalancer \
  --set controller.resources.requests.cpu=100m \
  --set controller.resources.requests.memory=128Mi \
  --set controller.resources.limits.cpu=300m \
  --set controller.resources.limits.memory=256Mi \
  --wait
```

Get the external IP:

```bash
kubectl get svc -n ingress-nginx ingress-nginx-controller
# EXTERNAL-IP → use this in values.yaml
```

---

## Step 2 — Create GCP Service Account (if not done)

```bash
export PROJECT_ID="your-gcp-project-id"
export GSA_NAME="fraud-detection-sa"

# Create SA
gcloud iam service-accounts create $GSA_NAME \
  --display-name="Fraud Detection API Service Account"

# Grant Artifact Registry read (to pull images)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/artifactregistry.reader" \
  --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant Cloud SQL client access
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/cloudsql.client" \
  --member="serviceAccount:${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
```

---

## Step 3 — Create Namespace

```bash
kubectl create namespace core
```

---

## Step 4 — Bind Workload Identity

Allows the K8s SA (`fraud-detection-sa` in namespace `core`) to authenticate as the GCP SA.

```bash
export PROJECT_ID="your-gcp-project-id"
export NAMESPACE="core"
export KSA_NAME="fraud-detection-sa"
export GSA_NAME="fraud-detection-sa"

gcloud iam service-accounts add-iam-policy-binding \
  ${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA_NAME}]"
```

> **Note:** The K8s SA annotation (`iam.gke.io/gcp-service-account`) is automatically added by the Helm chart via `serviceAccount.gcpServiceAccount` in `values.yaml`.

---

## Step 5 — Apply ConfigMap and Secrets

### ConfigMap

```bash
kubectl apply -f infra/k8s/manifests/fraud-detection-configmap.yaml -n core
```

Key config values (see [`fraud-detection-configmap.yaml`](../infra/k8s/manifests/fraud-detection-configmap.yaml)):

| Key | Description |
|---|---|
| `REDIS_HOST` | Redis cluster IP |
| `POSTGRES_HOST` | `127.0.0.1` (Cloud SQL Proxy listens locally) |
| `KSERVE_URL` | Internal KServe endpoint for predictions |
| `BOOTSTRAP_SERVERS` | Aiven Kafka broker address |

### App Secret

```bash
kubectl apply -f infra/k8s/manifests/fraud-detection-secrets.yaml -n core
```

Contains: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `CLOUD_SQL_INSTANCE`.

### Kafka TLS Certs

The API mounts Kafka TLS certs from a secret named `kafka-certs`:

```bash
kubectl create secret generic kafka-certs \
  --namespace=core \
  --from-file=ca.pem=/path/to/ca.pem \
  --from-file=service.cert=/path/to/service.cert \
  --from-file=service.key=/path/to/service.key
```

> Certs are downloaded from the **Aiven console** → your Kafka service → Connection Info → SSL.

### Basic Auth Secret (for NGINX)

```bash
# Generate htpasswd hash
HTPASSWD=$(openssl passwd -apr1 "your-password")

# Create secret
echo "admin:$HTPASSWD" | kubectl create secret generic basic-auth \
  --namespace=core \
  --from-file=auth=/dev/stdin
```

---

## Step 6 — Configure `values.yaml`

Edit [`infra/k8s/helm/fraud_detection/values.yaml`](../infra/k8s/helm/fraud_detection/values.yaml):

```yaml
api:
  name: fraud-detection
  replicaCount: 2
  image:
    repository: us-central1-docker.pkg.dev/<project>/<registry>/fraud-detection
    tag: "latest"        # or specific tag from CI/CD
  port: 1311

serviceAccount:
  name: fraud-detection-sa
  gcpServiceAccount: "fraud-detection-sa@<PROJECT_ID>.iam.gserviceaccount.com"

ingress:
  enabled: true
  host: "fraud-detection-api.<NGINX-EXTERNAL-IP>.sslip.io"  # ← use IP from Step 1
  ingressClassName: nginx
  maxBodySize: "50m"
  tls:
    enabled: false    # set to true + create TLS secret for production
  auth:
    enabled: true
    secretName: "basic-auth"    # ← must match secret from Step 5
    realm: "Fraud Detection API"
  rateLimit:
    rps: "10"         # requests per second per IP
    connections: "20" # concurrent connections per IP

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
  targetCPUUtilization: 70
  targetMemoryUtilization: 85
```

---

## Step 7 — Deploy with Helm

```bash
helm upgrade --install fraud-detection-api \
  ./infra/k8s/helm/fraud_detection \
  --namespace core \
  --values ./infra/k8s/helm/fraud_detection/values.yaml \
  --wait --timeout=3m
```

To upgrade after changes:

```bash
helm upgrade fraud-detection-api \
  ./infra/k8s/helm/fraud_detection \
  --namespace core \
  --values ./infra/k8s/helm/fraud_detection/values.yaml
```

---

## Step 8 — Verify Deployment

```bash
# Check pods (expect 2/2 Running — app + cloud-sql-proxy)
kubectl get pods -n core

# Check ingress
kubectl get ingress -n core

# Check HPA
kubectl get hpa -n core

# View app logs
kubectl logs -n core deployment/fraud-detection -c fraud-detection

# View Cloud SQL Proxy logs
kubectl logs -n core deployment/fraud-detection -c cloud-sql-proxy
```

Expected pod status:
```
NAME                               READY   STATUS    RESTARTS
fraud-detection-xxxxxxxxxx-xxxxx   2/2     Running   0
```

---

## Step 9 — Test the API

```bash
export API_URL="http://fraud-detection-api.<NGINX-EXTERNAL-IP>.sslip.io"

# Health check (no auth required)
curl $API_URL/health

# Ready check
curl $API_URL/ready

# Prediction (with basic auth)
curl -u admin:your-password \
  -X POST "$API_URL/predict" \
  -H "Content-Type: application/json" \
  -d '{"transaction_id": "txn-001", ...}'
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Pod stuck `0/2` | `kafka-certs` secret missing | Create secret (Step 5) |
| `cloud-sql-proxy` crash | Workload Identity not bound | Re-run Step 4 |
| `ImagePullBackOff` | SA missing `artifactregistry.reader` | Grant the role (Step 2) |
| `401 Unauthorized` | Wrong basic-auth credentials | Recreate `basic-auth` secret |
| `429 Too Many Requests` | Rate limit hit | Increase `rateLimit.rps` in values.yaml |
| Ingress not routing | Wrong `host` in values.yaml | Use exact NGINX external IP |

---

## Uninstall

```bash
helm uninstall fraud-detection-api --namespace core
kubectl delete namespace core
```

---

## Reference

| Resource | Path |
|---|---|
| Helm chart | `infra/k8s/helm/fraud_detection/` |
| ConfigMap | `infra/k8s/manifests/fraud-detection-configmap.yaml` |
| Secrets | `infra/k8s/manifests/fraud-detection-secrets.yaml` |
| KServe serving docs | `docs/Deploy Model Serving.md` |
