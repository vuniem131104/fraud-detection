# Deploy Monitoring Stack on GKE (Prometheus, Loki, Alloy, Grafana)

This guide covers deploying the full observability stack to GKE (namespace: `monitoring`):

| Component | Role | Chart |
|---|---|---|
| **Prometheus** | Metrics — scrapes the cluster and app endpoints, stores time series | Vendored upstream chart (`prometheus` 28.13.0, app v3.10.0) + kube-state-metrics + pushgateway |
| **Loki** | Logs backend — stores log chunks in GCS | Custom chart (StatefulSet, Loki 3.4.2) |
| **Alloy** | Log collector — DaemonSet tailing pod logs on every node, pushes to Loki | Custom chart (Alloy v1.7.1) |
| **Grafana** | Dashboards — queries Prometheus, Loki, and Cloud SQL Postgres | Vendored upstream chart (`grafana` 10.5.15, app v12.3.1) |

Alertmanager and node-exporter are **intentionally disabled** in the Prometheus values
(node-exporter needs `hostNetwork`/`hostPath`, which GKE Autopilot forbids).

> **Prerequisites**
> - GKE cluster with Workload Identity enabled (`--workload-pool`)
> - nginx Ingress controller installed (Grafana Ingress uses `ingressClassName: nginx`)
> - GCS buckets for Loki (see `storage.*` in [`loki/values.yaml`](../infra/k8s/helm/loki/values.yaml))
> - Cloud SQL (PostgreSQL) instance `fraud-detection` (Grafana's Postgres datasource)

---

## Architecture Overview

```
                        ┌──────────────────────── namespace: monitoring ───────────────────────┐
Pod logs on every node  │                                                                       │
/var/log/pods/**        │   DaemonSet: alloy ──push──▶  StatefulSet: loki (:3100) ──chunks──▶ GCS bucket
                        │                                      ▲                                │   (via Workload Identity: loki-sa)
                        │                                      │ logs (datasource)              │
Cluster + app metrics ──┼─▶ Deployment: prometheus-server ◀── Grafana ──▶ Cloud SQL Postgres    │
(kube-state-metrics,    │      (:80, PVC-backed)            (:80, Ingress)  (cloud-sql-proxy    │
 pushgateway, pods)     │                                                    sidecar via        │
                        │                                                    Workload Identity: │
                        └────────────────────────────────────────────────── grafana-sa) ────────┘
```

- **Alloy** discovers running pods via the Kubernetes API, tails `/var/log/pods/**`, labels
  each stream with `namespace`, `pod`, `container`, `app`, `cluster=fraud-detection`, and pushes
  to `http://loki.monitoring.svc.cluster.local:3100`.
- **Loki** keeps 7 days of logs (`retentionPeriod: 168h`) and writes chunks/ruler data to GCS.
- **Prometheus** uses the chart's default Kubernetes scrape configs (API servers, nodes,
  cAdvisor, service endpoints, pods) plus kube-state-metrics and pushgateway.
- **Grafana** is pre-provisioned with three datasources (Prometheus, Loki, Postgres) and
  dashboards (the custom `main-dashboard.json` plus community dashboards from grafana.com).
  Persistence is disabled — everything is provisioned from the chart, so pods are disposable.

Workload Identity is needed by two components only:

| K8s SA (in `monitoring`) | GCP SA | Why |
|---|---|---|
| `loki-sa` | `loki-sa@<PROJECT_ID>.iam.gserviceaccount.com` | Write chunks to GCS |
| `grafana-sa` | `grafana-sa@<PROJECT_ID>.iam.gserviceaccount.com` | cloud-sql-proxy sidecar → Cloud SQL |

`alloy-sa` needs **no** GCP access — it only talks to the Kubernetes API (RBAC is created by
the chart) and to Loki over HTTP.

---

## Step 1 — Create GCP Service Accounts (skip if they exist)

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"

gcloud iam service-accounts create grafana-sa \
  --project $PROJECT_ID \
  --display-name="Grafana (Cloud SQL access)"

gcloud iam service-accounts create loki-sa \
  --project $PROJECT_ID \
  --display-name="Loki (GCS chunk storage)"
```

### Grant roles

```bash
# Grafana: connect to Cloud SQL through the proxy
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/cloudsql.client" \
  --member="serviceAccount:grafana-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Grafana: log in as an IAM database user (required because the proxy runs with --auto-iam-authn)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --role="roles/cloudsql.instanceUser" \
  --member="serviceAccount:grafana-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Loki: read/write chunks in the GCS bucket (bucket name from loki/values.yaml storage.gcsBucket)
gcloud storage buckets add-iam-policy-binding gs://fraud-detection-chunkss \
  --member="serviceAccount:loki-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

> **IAM vs. password auth for Grafana's Postgres datasource:** the sidecar args in
> [`grafana/values.yaml`](../infra/k8s/helm/grafana/values.yaml) include `--auto-iam-authn`,
> which logs in as the IAM identity (`grafana-sa@<PROJECT_ID>.iam`) — not the `GF_PG_USER`
> from the secret. To use IAM auth end-to-end, create the IAM DB user and set `GF_PG_USER`
> accordingly:
>
> ```bash
> gcloud sql users create grafana-sa@${PROJECT_ID}.iam \
>   --instance=fraud-detection --type=cloud_iam_service_account
> ```
>
> To keep password auth instead, remove `--auto-iam-authn` from `extraContainers` and skip
> the `cloudsql.instanceUser` grant.

---

## Step 2 — Create Namespace

```bash
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
```

---

## Step 3 — Apply Grafana Secret

Grafana's pod reads `grafana-secrets` (`envFromSecret` in values) and the cloud-sql-proxy
sidecar reads `CLOUD_SQL_INSTANCE` from it, so it must exist in `monitoring` **before** the
Helm install. The manifest has no `namespace:` field — pass `-n monitoring` explicitly:

```bash
kubectl apply -f infra/k8s/manifests/grafana-secrets.yaml -n monitoring
```

Contains: `GRAFANA_PG_HOST`, `GF_PG_PORT`, `GF_PG_DB`, `GF_PG_USER`, `GF_PG_PASSWORD`,
`CLOUD_SQL_INSTANCE` — consumed by the Postgres datasource template and the proxy sidecar.

---

## Step 4 — Bind Workload Identity

Links the K8s SAs to the GCP SAs. The `iam.gke.io/gcp-service-account` annotations are added
by the charts (`serviceAccount.gcpServiceAccount` for Loki, `serviceAccount.annotations` for
Grafana) — only the IAM-side binding is manual:

```bash
export PROJECT_ID="project-57f7ef9a-6059-4068-ae7"

gcloud iam service-accounts add-iam-policy-binding \
  grafana-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[monitoring/grafana-sa]"

gcloud iam service-accounts add-iam-policy-binding \
  loki-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:${PROJECT_ID}.svc.id.goog[monitoring/loki-sa]"
```

> **Wait ~30 seconds** after binding before deploying — the binding takes a moment to
> propagate, and deploying too early makes cloud-sql-proxy / Loki fail with `403` until
> the pod is restarted.

---

## Step 5 — Deploy with Helm

Order matters: Loki before Alloy (Alloy pushes to it), Prometheus before Grafana (Grafana
queries it). `--rollback-on-failure` auto-rolls back a failed upgrade (Helm 4 flag; on
Helm 3 use `--atomic`).

```bash
helm upgrade --install loki infra/k8s/helm/loki \
  --namespace monitoring --rollback-on-failure

helm upgrade --install alloy infra/k8s/helm/alloy \
  --namespace monitoring --rollback-on-failure

helm upgrade --install prometheus infra/k8s/helm/prometheus \
  --namespace monitoring --rollback-on-failure --timeout 10m

helm upgrade --install grafana infra/k8s/helm/grafana \
  --namespace monitoring --rollback-on-failure --timeout 10m
```

> **Release names matter.** Grafana's provisioned datasources point at
> `prometheus-server.monitoring.svc.cluster.local` and `loki.monitoring.svc.cluster.local:3100`
> — these names are derived from the release names above. Renaming a release breaks the
> datasources. Prometheus's subchart dependencies are already vendored in
> `infra/k8s/helm/prometheus/charts/`, so no `helm dependency build` is needed.

---

## Step 6 — Verify Deployment

```bash
kubectl get pods -n monitoring
```

Expected (Alloy runs one pod **per node**; Grafana is 2/2 with the proxy sidecar):

```
NAME                                            READY   STATUS    RESTARTS
alloy-xxxxx                                     1/1     Running   0
grafana-xxxxxxxxxx-xxxxx                        2/2     Running   0
loki-0                                          1/1     Running   0
prometheus-server-xxxxxxxxxx-xxxxx              2/2     Running   0
prometheus-kube-state-metrics-xxxxxxxxxx-xxxxx  1/1     Running   0
prometheus-prometheus-pushgateway-xxxxx-xxxxx   1/1     Running   0
```

Spot-check the plumbing:

```bash
# Loki ready + GCS reachable (no permission errors in logs)
kubectl logs -n monitoring loki-0 | grep -i "error\|ready" | tail

# Alloy shipping logs (no push errors)
kubectl logs -n monitoring daemonset/alloy | grep -i error | tail

# Grafana's Cloud SQL proxy connected
kubectl logs -n monitoring deployment/grafana -c cloud-sql-proxy | tail
```

---

## Step 7 — Access Grafana

Via the Ingress (nginx, host from `ingress.hosts` in values):

```
http://grafana.34.56.166.63.sslip.io
```

Or port-forward:

```bash
kubectl port-forward -n monitoring svc/grafana 3000:80
# → http://localhost:3000
```

Log in with `adminUser` / `adminPassword` from
[`grafana/values.yaml`](../infra/k8s/helm/grafana/values.yaml). Then confirm:

1. **Connections → Data sources** — Prometheus, Loki, and PostgreSQL all "working".
2. **Explore → Loki** — query `{namespace="core"}` and see app logs flowing.
3. **Dashboards** — the provisioned `main-dashboard` plus the community dashboards
   (Prometheus stats, Kubernetes deployments, Loki logs app, etc.).

Prometheus itself is ClusterIP-only; to inspect it directly:

```bash
kubectl port-forward -n monitoring svc/prometheus-server 9090:80
# → http://localhost:9090/targets
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `cloud-sql-proxy` logs `403` / `Permission denied` | Workload Identity binding missing or not yet propagated | Re-run Step 4, wait ~30s, restart the pod (`kubectl rollout restart deployment/grafana -n monitoring`) |
| Loki logs `storage: permission denied` on GCS | `loki-sa` missing `storage.objectAdmin` on the bucket, or WI not bound | Re-run Steps 1 & 4; check the bucket name in `loki/values.yaml` matches the real bucket |
| Postgres datasource: `password authentication failed` | Proxy uses `--auto-iam-authn` but `GF_PG_USER` is a built-in Postgres user | See the IAM vs. password note in Step 1 |
| No logs in Grafana Explore | Alloy can't reach Loki, or Loki not ready | `kubectl logs daemonset/alloy -n monitoring`; verify `config.lokiEndpoint` in `alloy/values.yaml` |
| Prometheus datasource "connection refused" | Prometheus release renamed → service isn't `prometheus-server` | Reinstall with release name `prometheus`, or update the datasource URL in `grafana/values.yaml` |
| Grafana Ingress 404 / no address | nginx Ingress controller missing | Install ingress-nginx, or use port-forward |
| Alloy pod missing on a node | Taint not tolerated | Chart tolerates control-plane taints only; add tolerations in `daemonset.yaml` if you use custom taints |

---

## Uninstall

```bash
helm uninstall grafana prometheus alloy loki --namespace monitoring
kubectl delete secret grafana-secrets -n monitoring

# PVCs survive helm uninstall — remove them to fully clean up Prometheus data
kubectl delete pvc -n monitoring --all
```

---

## Reference

| Resource | Path |
|---|---|
| Loki chart (custom) | `infra/k8s/helm/loki/` |
| Alloy chart (custom) | `infra/k8s/helm/alloy/` |
| Prometheus chart (vendored) | `infra/k8s/helm/prometheus/` |
| Grafana chart (vendored) | `infra/k8s/helm/grafana/` |
| Grafana secret | `infra/k8s/manifests/grafana-secrets.yaml` |
| Custom dashboard JSON | `infra/k8s/helm/grafana/dashboards/main-dashboard.json` |
| Drift Detection deploy guide (Grafana queries it) | `docs/Deploy Drift Detection API.md` |
