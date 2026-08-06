# Centralize Secret Management with GCP Secret Manager + External Secrets Operator

This guide migrates every plaintext secret in this repo to **GCP Secret Manager (GSM)** synced
into the cluster by the **External Secrets Operator (ESO)**. After the migration, git contains
only *references* to secrets, never values; rotation happens in one place; and pods keep using
the exact same K8s Secrets they use today (same names, same keys — **no app/chart changes**).

## What is being fixed

| Secret (in cluster) | Namespace | Source file in git | Leaked values |
|---|---|---|---|
| `fraud-detection-secrets` | `core` | `infra/k8s/manifests/fraud-detection-secrets.yaml` | PG user/password, Feast registry URI (password embedded) |
| `prediction-writer-secrets` | `core` | `infra/k8s/manifests/prediction-writer-secrets.yaml` | PG user/password |
| `drift-detection-api-secrets` | `core` | `infra/k8s/manifests/drift-detection-secrets.yaml` | PG user/password |
| `grafana-secrets` | `monitoring` | `infra/k8s/manifests/grafana-secrets.yaml` | PG user/password (Grafana datasource/DB) |
| `basic-auth` | `core` | `infra/k8s/manifests/basic-auth-secret.yaml` | htpasswd hash (placeholder in git, real in cluster) |
| Grafana admin login | `monitoring` | `infra/k8s/helm/grafana/values.yaml` → `adminPassword:` | **admin password in plaintext** |

ConfigMaps (`*-configmap.yaml`) are **not** part of this migration — they hold non-sensitive
config and staying in git is correct (GitOps).

> **Prerequisites**
> - `gcloud` authenticated against project `project-57f7ef9a-6059-4068-ae7`
> - `kubectl` pointed at the GKE cluster (Workload Identity enabled)
> - Helm 4 (auto-rollback flag is `--rollback-on-failure`)

## Architecture after migration

```
                GCP Secret Manager (project: project-57f7ef9a-6059-4068-ae7)
                fraud-pg-user / fraud-pg-password / fraud-grafana-admin-* / fraud-ingress-basic-auth
                                        ▲
                                        │ AccessSecretVersion (Workload Identity:
                                        │ eso-secrets-reader@…iam.gserviceaccount.com)
        ┌───────────────────────────────┴──────────────────────────────┐
        │  namespace: external-secrets                                 │
        │  External Secrets Operator  ◀── ClusterSecretStore           │
        └───────────────┬──────────────────────────────┬───────────────┘
                        │ sync (refreshInterval: 1h)   │
   namespace: core      ▼                              ▼   namespace: monitoring
   ExternalSecret → Secret fraud-detection-secrets     ExternalSecret → Secret grafana-secrets
   ExternalSecret → Secret prediction-writer-secrets   ExternalSecret → Secret grafana-admin-credentials
   ExternalSecret → Secret drift-detection-api-secrets
   ExternalSecret → Secret basic-auth
                        │                              │
                        ▼ envFrom / secretKeyRef       ▼ admin.existingSecret / envFrom
                   app pods (unchanged)            Grafana (chart values, no plaintext)
```

**Migration strategy:** seed GSM with the *current* live values first (zero disruption), switch
every consumer over to ESO, verify, and only then rotate all credentials as the final step —
which doubles as proof that centralized rotation works.

---

## 1. Enable the Secret Manager API

```bash
gcloud services enable secretmanager.googleapis.com \
  --project project-57f7ef9a-6059-4068-ae7
```

## 2. Seed GSM from the live cluster

Read current values straight out of the cluster and pipe them into GSM — no plaintext is typed
or written to disk. Naming convention: `<app>-<key>` (org-wide, one flat project-level namespace).

```bash
PROJECT=project-57f7ef9a-6059-4068-ae7

# Shared Postgres credential (used by fraud-detection, prediction-writer,
# drift-detection, and Grafana's datasource — ONE copy in GSM)
kubectl get secret fraud-detection-secrets -n core -o jsonpath='{.data.POSTGRES_USER}' \
  | base64 -d | gcloud secrets create fraud-pg-user --project $PROJECT --data-file=-
kubectl get secret fraud-detection-secrets -n core -o jsonpath='{.data.POSTGRES_PASSWORD}' \
  | base64 -d | gcloud secrets create fraud-pg-password --project $PROJECT --data-file=-

# Grafana admin login (chart-generated secret "grafana" holds the current values)
kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-user}' \
  | base64 -d | gcloud secrets create fraud-grafana-admin-user --project $PROJECT --data-file=-
kubectl get secret grafana -n monitoring -o jsonpath='{.data.admin-password}' \
  | base64 -d | gcloud secrets create fraud-grafana-admin-password --project $PROJECT --data-file=-

# Ingress basic-auth (htpasswd line, format "user:hash")
kubectl get secret basic-auth -n core -o jsonpath='{.data.auth}' \
  | base64 -d | gcloud secrets create fraud-ingress-basic-auth --project $PROJECT --data-file=-
```

Verify: `gcloud secrets list --project $PROJECT` should show 5 secrets.

Non-sensitive values (`POSTGRES_DB`, `CLOUD_SQL_INSTANCE`, hosts, ports) do **not** go to GSM —
they stay as literals in the ExternalSecret templates below.

## 3. Create the GSA that ESO will use

```bash
PROJECT=project-57f7ef9a-6059-4068-ae7

gcloud iam service-accounts create eso-secrets-reader \
  --project $PROJECT --display-name "External Secrets Operator reader"

gcloud projects add-iam-policy-binding $PROJECT \
  --member "serviceAccount:eso-secrets-reader@${PROJECT}.iam.gserviceaccount.com" \
  --role roles/secretmanager.secretAccessor
```

## 4. Install External Secrets Operator

```bash
helm repo add external-secrets https://charts.external-secrets.io
helm repo update

helm install external-secrets external-secrets/external-secrets \
  -n external-secrets --create-namespace \
  --set serviceAccount.annotations."iam\.gke\.io/gcp-service-account"=eso-secrets-reader@${PROJECT}.iam.gserviceaccount.com \
  --rollback-on-failure
```

Bind Workload Identity (KSA `external-secrets` in ns `external-secrets` → the GSA):

```bash
gcloud iam service-accounts add-iam-policy-binding \
  eso-secrets-reader@${PROJECT}.iam.gserviceaccount.com \
  --project $PROJECT \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT}.svc.id.goog[external-secrets/external-secrets]"
```

> ⚠️ **Wait ~30 seconds** after the binding before creating any ExternalSecret — WI bindings
> propagate slowly and ESO will get `403 PermissionDenied` otherwise (same behavior seen with
> cloud-sql-proxy). If ESO pods started before the binding, restart them:
> `kubectl rollout restart deploy -n external-secrets`

## 5. Create the ClusterSecretStore

One cluster-scoped store, usable from every namespace. Save as
`infra/k8s/manifests/cluster-secret-store.yaml`:

```yaml
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: gcp-secret-manager
spec:
  provider:
    gcpsm:
      projectID: project-57f7ef9a-6059-4068-ae7
      # no auth block: ESO pod uses its Workload Identity (ADC)
```

```bash
kubectl apply -f infra/k8s/manifests/cluster-secret-store.yaml
kubectl get clustersecretstore gcp-secret-manager   # STATUS must be Valid, READY True
```

## 6. Replace the five Secret manifests with ExternalSecrets

Each ExternalSecret's `target.name` matches the old Secret's name and its `template` reproduces
**every key** the old Secret had — deployments consuming them via `envFrom`/`secretKeyRef` need
no change at all.

For each one: **delete the old hand-made Secret, then apply the ExternalSecret** (ESO refuses to
adopt a Secret it doesn't own). The gap is harmless — env vars were injected into running pods at
container start; nothing re-reads the Secret until the next pod restart, by which time ESO has
recreated it (sync is near-instant).

### 6.1 `fraud-detection-secrets` — replace file content of `infra/k8s/manifests/fraud-detection-secrets.yaml`

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: fraud-detection-secrets
  namespace: core
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: fraud-detection-secrets
    template:
      data:
        POSTGRES_USER: "{{ .pg_user }}"
        POSTGRES_PASSWORD: "{{ .pg_password }}"
        POSTGRES_DB: "fraud-detection"
        CLOUD_SQL_INSTANCE: "project-57f7ef9a-6059-4068-ae7:us-central1:fraud-detection"
        # password stored ONCE in GSM; connection string assembled at sync time.
        # urlquery percent-encodes reserved chars (e.g. "!" -> "%21")
        FEAST_REGISTRY_PATH: "postgresql+psycopg://{{ .pg_user }}:{{ .pg_password | urlquery }}@127.0.0.1:5432/feast-registry"
  data:
    - secretKey: pg_user
      remoteRef:
        key: fraud-pg-user
    - secretKey: pg_password
      remoteRef:
        key: fraud-pg-password
```

```bash
kubectl delete secret fraud-detection-secrets -n core
kubectl apply -f infra/k8s/manifests/fraud-detection-secrets.yaml
kubectl get externalsecret fraud-detection-secrets -n core   # READY True, STATUS SecretSynced
```

### 6.2 `prediction-writer-secrets` — replace `infra/k8s/manifests/prediction-writer-secrets.yaml`

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: prediction-writer-secrets
  namespace: core
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: prediction-writer-secrets
    template:
      data:
        POSTGRES_USER: "{{ .pg_user }}"
        POSTGRES_PASSWORD: "{{ .pg_password }}"
        POSTGRES_DB: "fraud-detection"
        CLOUD_SQL_INSTANCE: "project-57f7ef9a-6059-4068-ae7:us-central1:fraud-detection"
  data:
    - secretKey: pg_user
      remoteRef:
        key: fraud-pg-user
    - secretKey: pg_password
      remoteRef:
        key: fraud-pg-password
```

```bash
kubectl delete secret prediction-writer-secrets -n core
kubectl apply -f infra/k8s/manifests/prediction-writer-secrets.yaml
```

### 6.3 `drift-detection-api-secrets` — replace `infra/k8s/manifests/drift-detection-secrets.yaml`

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: drift-detection-api-secrets
  namespace: core
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: drift-detection-api-secrets
    template:
      data:
        POSTGRES_USER: "{{ .pg_user }}"
        POSTGRES_PASSWORD: "{{ .pg_password }}"
        POSTGRES_DB: "fraud-detection"
        CLOUD_SQL_INSTANCE: "project-57f7ef9a-6059-4068-ae7:us-central1:fraud-detection"
  data:
    - secretKey: pg_user
      remoteRef:
        key: fraud-pg-user
    - secretKey: pg_password
      remoteRef:
        key: fraud-pg-password
```

```bash
kubectl delete secret drift-detection-api-secrets -n core
kubectl apply -f infra/k8s/manifests/drift-detection-secrets.yaml
```

### 6.4 `grafana-secrets` — replace `infra/k8s/manifests/grafana-secrets.yaml`

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: grafana-secrets
  namespace: monitoring
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: grafana-secrets
    template:
      data:
        GRAFANA_PG_HOST: "127.0.0.1"
        GF_PG_PORT: "5432"
        GF_PG_DB: "fraud-detection"
        GF_PG_USER: "{{ .pg_user }}"
        GF_PG_PASSWORD: "{{ .pg_password }}"
        CLOUD_SQL_INSTANCE: "project-57f7ef9a-6059-4068-ae7:us-central1:fraud-detection"
  data:
    - secretKey: pg_user
      remoteRef:
        key: fraud-pg-user
    - secretKey: pg_password
      remoteRef:
        key: fraud-pg-password
```

```bash
kubectl delete secret grafana-secrets -n monitoring
kubectl apply -f infra/k8s/manifests/grafana-secrets.yaml
```

### 6.5 `basic-auth` — replace `infra/k8s/manifests/basic-auth-secret.yaml`

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: basic-auth
  namespace: core
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: basic-auth
  data:
    # htpasswd line "user:hash" — nginx ingress expects it under the "auth" key
    - secretKey: auth
      remoteRef:
        key: fraud-ingress-basic-auth
```

```bash
kubectl delete secret basic-auth -n core
kubectl apply -f infra/k8s/manifests/basic-auth-secret.yaml
```

### 6.6 Verify all five

```bash
kubectl get externalsecret -A          # all READY True
# spot-check that keys survived intact:
kubectl get secret fraud-detection-secrets -n core -o jsonpath='{.data}' | jq 'keys'
kubectl get secret grafana-secrets -n monitoring -o jsonpath='{.data}' | jq 'keys'
```

## 7. Fix the Grafana admin plaintext password

### 7.1 Sync admin credentials into a new Secret

Save as `infra/k8s/manifests/grafana-admin-external-secret.yaml`:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: grafana-admin-credentials
  namespace: monitoring
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: gcp-secret-manager
    kind: ClusterSecretStore
  target:
    name: grafana-admin-credentials
  data:
    - secretKey: admin-user
      remoteRef:
        key: fraud-grafana-admin-user
    - secretKey: admin-password
      remoteRef:
        key: fraud-grafana-admin-password
```

```bash
kubectl apply -f infra/k8s/manifests/grafana-admin-external-secret.yaml
```

### 7.2 Point the chart at it

In `infra/k8s/helm/grafana/values.yaml`, **delete** the plaintext lines and set
`admin.existingSecret` (around line 501):

```yaml
# Administrator credentials when not using an existing secret (see below)
adminUser: admin
adminPassword: ""            # ← plaintext removed; ignored once existingSecret is set

# Use an existing secret for the admin user.
admin:
  existingSecret: grafana-admin-credentials
  userKey: admin-user
  passwordKey: admin-password
```

```bash
helm upgrade grafana infra/k8s/helm/grafana -n monitoring --rollback-on-failure
```

> **Note:** Grafana only reads `GF_SECURITY_ADMIN_PASSWORD` on *first* startup — after that the
> admin password lives in Grafana's database (Cloud SQL here). Changing the Secret therefore
> changes what's mounted, **not** the actual login, until you reset it explicitly (done in step 8).

## 8. Rotate everything (the payoff)

Every value that was ever committed is in git history → treat it as compromised. Rotation is
now: change GSM + the backing system, and consumers pick it up.

### 8.1 Postgres password (do in a low-traffic window)

```bash
PROJECT=project-57f7ef9a-6059-4068-ae7
NEW_PG_PW=$(openssl rand -hex 20)   # hex = URL-safe, no encoding edge cases

# 1. change it in Cloud SQL and GSM back-to-back
printf '%s' "$NEW_PG_PW" | gcloud sql users set-password vuniem \
  --instance fraud-detection --project $PROJECT --password "$(cat -)"
printf '%s' "$NEW_PG_PW" | gcloud secrets versions add fraud-pg-password \
  --project $PROJECT --data-file=-
unset NEW_PG_PW

# 2. force ESO to re-sync now instead of waiting for refreshInterval
for es in fraud-detection-secrets prediction-writer-secrets drift-detection-api-secrets; do
  kubectl annotate externalsecret $es -n core force-sync=$(date +%s) --overwrite
done
kubectl annotate externalsecret grafana-secrets -n monitoring force-sync=$(date +%s) --overwrite

# 3. restart consumers (env vars only refresh on pod restart)
kubectl rollout restart deploy -n core        # fraud-detection, prediction-writer, drift-detection-api
kubectl rollout restart deploy grafana -n monitoring
```

Existing DB connections keep working after `set-password` (only *new* logins use the new
password), so the restart closes the loop; expect at most a brief blip between steps 1 and 3.

### 8.2 Grafana admin password

```bash
NEW_GF_PW=$(openssl rand -hex 20)
printf '%s' "$NEW_GF_PW" | gcloud secrets versions add fraud-grafana-admin-password \
  --project $PROJECT --data-file=-
kubectl annotate externalsecret grafana-admin-credentials -n monitoring force-sync=$(date +%s) --overwrite

# Grafana stores the admin password in its DB — reset it explicitly:
kubectl exec -n monitoring deploy/grafana -c grafana -- \
  grafana cli admin reset-admin-password "$NEW_GF_PW"
unset NEW_GF_PW
```

Log in at the Grafana ingress with the new password
(`gcloud secrets versions access latest --secret fraud-grafana-admin-password`).

### 8.3 Ingress basic-auth

```bash
htpasswd -nb admin "$(openssl rand -hex 16)" | gcloud secrets versions add \
  fraud-ingress-basic-auth --project $PROJECT --data-file=-
kubectl annotate externalsecret basic-auth -n core force-sync=$(date +%s) --overwrite
# nginx reads the Secret per-request via its controller cache — no restart needed
```

## 9. Keep it from happening again

1. **Commit the new manifests** (they contain references only — safe for git).
2. **Purge history (optional but recommended):** the old passwords remain in git history.
   Since everything is rotated they're dead credentials, but if the repo ever goes public run
   `git filter-repo --replace-text` (or BFG) to scrub them.
3. **Secret scanning:** add [gitleaks](https://github.com/gitleaks/gitleaks) as a pre-commit
   hook / CI step so no new plaintext lands in git.
4. **RBAC:** ESO-synced Secrets are still plain K8s Secrets in etcd — restrict `get`/`list`
   on Secrets in `core` and `monitoring` to admins and the service accounts that need them.

## Day-2: adding a new secret

```bash
# 1. put the value in GSM
printf '%s' "<value>" | gcloud secrets create fraud-<app>-<key> --project $PROJECT --data-file=-
# 2. add an ExternalSecret manifest in the app's namespace (copy any example from step 6)
# 3. reference the synced Secret from the deployment via envFrom/secretKeyRef as usual
```

Rotation is always: `gcloud secrets versions add …` → force-sync annotation → restart consumers.
