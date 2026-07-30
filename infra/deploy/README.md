# One-shot platform deploy

`deploy.sh` takes a GKE cluster that already exists (with Workload Identity) all
the way to the full running system — GCP APIs → cluster dependencies → app and
monitoring workloads — in one idempotent run. No CI/CD required.

```bash
./infra/deploy/deploy.sh
```

Everything is pinned in [`config.env`](config.env) to the **live** install
(read from `gcloud` + `helm list` on 2026-07-14), so a from-scratch run
reproduces the current system exactly.

## Phases

Run in this order (or name specific ones: `./deploy.sh apps verify`):

| Phase | Does |
|---|---|
| `preflight` | Check `gcloud`/`kubectl`/`helm`, gcloud auth, set project |
| `apis` | `gcloud services enable` the 14 required APIs |
| `credentials` | `get-credentials` for the cluster |
| `workload_identity` | Assert the 7 KSA→GSA WI bindings (idempotent) |
| `cluster_deps` | cert-manager, Knative Serving, Istio/net-istio, KServe (+ LightGBM runtime), KEDA, ingress-nginx, External Secrets |
| `secrets` | ClusterSecretStore + the 6 ExternalSecrets + `kafka-certs` (from `certs/`) |
| `config` | The 3 ConfigMaps (into `core`) |
| `apps` | `serving`, `fraud-detection`, `prediction-writer`, `drift-detection-api`, `prometheus`, `loki`, `grafana`, `alloy` |
| `verify` | Print pods / InferenceService / ScaledObject / ExternalSecrets / releases |

```bash
./deploy.sh --list        # list phases
./deploy.sh --help        # usage + aliases (deps, creds, iam)
DRY_RUN=1 ./deploy.sh     # print every command, execute nothing
```

## Idempotency

Safe to re-run — it converges, never duplicates: `gcloud services enable` and
`add-iam-policy-binding` are no-ops when already set, `kubectl apply` and
`helm upgrade --install` reconcile in place.

## What it does NOT do (owned elsewhere)

- **Cluster, Cloud SQL, Redis, buckets, GCP service accounts + project roles** →
  Terraform ([`infra/terraform`](../terraform)) or the `gcloud` steps in `docs/`.
  This script only asserts the WI *bindings* so pods can authenticate; it assumes
  the GCP SAs already exist.
- **Secret values** → seeded/rotated in Secret Manager (see
  `docs/Centralize Secret Management.md`). ESO syncs them into the cluster.
- **Building/pushing images** → images are already in Artifact Registry
  (`…/fraud-detection/{fraud-detection,prediction-writer,drift-detection-api}:latest`).

## Prerequisites

- `gcloud` authenticated with rights on `project-57f7ef9a-6059-4068-ae7`;
  `kubectl` and `helm` installed.
- The GCP service accounts exist (Terraform / docs) so WI bindings resolve.
- Aiven Kafka TLS certs at `certs/{ca.pem,service.cert,service.key}` for
  `kafka-certs` (the fraud API and prediction-writer mount them). Without them
  that one secret is skipped with a warning.

## Right-sizing & node reconfiguration

The cluster was over-provisioned on **CPU** (requests ~60% of capacity, real use
~5%); memory was about right (~54% used). Two things address this:

**1. Leaner requests** (already baked into the Helm values / this script):

| Workload | CPU request before → after | Note |
|---|---|---|
| fraud-detection (×2) | 1000m → **200m** | biggest win (~1.6 vCPU freed) |
| drift-detection-api | 250m → **100m** | mem kept (loads baseline parquet) |
| prediction-writer | 250m → **100m** | + proxy 250m→128m |
| alloy (DaemonSet ×3) | 100m → **50m** | log shipper |
| istiod / ingress-gw | pinned 1 replica, 100m each | `optimize_istio` (in `cluster_deps`) |

Roll them out:
```bash
./deploy.sh istio            # right-size Istio only
helm upgrade fraud-detection    infra/k8s/helm/fraud_detection   -n core
helm upgrade drift-detection-api infra/k8s/helm/drift_detection  -n core
helm upgrade prediction-writer  infra/k8s/helm/prediction_writer -n core
helm upgrade alloy              infra/k8s/helm/alloy             -n monitoring
# …or just: ./deploy.sh apps   (re-applies all app charts with the new values)
```

**2. Node layout → 2× 4-vCPU + 1× 2-vCPU** (10 vCPU / 24 GB), which frees 2 vCPU
of regional quota for the airflow-mlflow VM. A GKE pool has one machine type, so
the target needs a `small-pool` — this is **folded into the deploy** as the
`nodes` phase (target set in `config.env` → `NODE_POOLS_SPEC`), so a full
`./deploy.sh` lands on it with no separate step:

- **Greenfield** (cluster created by Terraform with `gke_node_pools` = the 2-pool
  target): `nodes` sees the pools already correct → no-op.
- **Existing 3-node cluster**: `nodes` creates `small-pool` and shrinks
  `default-pool` 3→2, converging to target.

`nodes` runs **after** `apps` on purpose, so the leaner requests land first and a
drained node's pods reschedule cleanly. Run it alone with `./deploy.sh nodes`
(alias `reconfigure`). Set `NODES_ALLOW_SHRINK=0` to skip the draining shrink.

## Fresh-cluster caveat

On a brand-new cluster the **ingress LB gets a new IP**, so update
`infra/k8s/helm/fraud_detection/values.yaml` → `ingress.host`
(`fraud-detection-api.<NEW-IP>.sslip.io`) and re-run `./deploy.sh apps`. The
`cluster_deps` phase prints the assigned IP. On the existing cluster the IP
(`34.56.166.63`) is preserved across re-runs.
