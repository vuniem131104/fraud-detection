Drift Detection API Helm Chart
==============================

This chart deploys the Drift Detection API (PSI drift on `amount_usd`, last 30 days
from Postgres vs. the training baseline in `dataset/training_data.parquet`).

The chart's `envFrom` expects a ConfigMap `drift-detection-api-configmap` and a Secret
`drift-detection-api-secrets` in the `core` namespace. Apply them first:

```sh
kubectl apply -f infra/k8s/manifests/drift-detection-configmap.yaml
kubectl apply -f infra/k8s/manifests/drift-detection-secrets.yaml
```

Install or upgrade (auto-rollback the release on a failed upgrade). `--rollback-on-failure`
is the Helm 4 flag; on Helm 3 use `--atomic` instead.

```sh
helm upgrade --install drift-detection-api ./infra/k8s/helm/drift_detection \
  -n core \
  --rollback-on-failure \
  --timeout 10m \
  --cleanup-on-fail
```

Render locally:

```sh
helm template drift-detection-api ./infra/k8s/helm/drift_detection
```
