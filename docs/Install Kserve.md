# Install Knative Serving
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.22.1/serving-crds.yaml
kubectl apply -f https://github.com/knative/serving/releases/download/knative-v1.22.1/serving-core.yaml

# Install Istio
kubectl apply -l knative.dev/crd-install=true -f https://github.com/knative-extensions/net-istio/releases/download/knative-v1.22.1/istio.yaml
kubectl apply -f https://github.com/knative-extensions/net-istio/releases/download/knative-v1.22.1/istio.yaml
kubectl apply -f https://github.com/knative-extensions/net-istio/releases/download/knative-v1.22.1/net-istio.yaml
kubectl patch configmap/config-network \
    --namespace knative-serving \
    --type merge \
    --patch '{"data":{"ingress-class":"istio.ingress.networking.knative.dev"}}'
kubectl --namespace istio-system get service istio-ingressgateway

# Install Cert Manager
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.2/cert-manager.yaml

# Install Kserve
kubectl create namespace kserve
helm install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.18.0
helm install kserve oci://ghcr.io/kserve/charts/kserve-resources --version v0.18.0

# Install serving runtime depend on model type
kubectl apply -f https://raw.githubusercontent.com/kserve/kserve/master/config/runtimes/kserve-mlserver.yaml

# Problems 
  - Insufficient cpus, memory when installing kserve, knative, istio
  - Not have ClusterServingRuntime kserve-mlserver, we need to replace placeholder in mlserver:replace → seldonio/mlserver:1.5.0
  - Pods use default service account to work with operations on GCP -> make sure to Turn on Workload Identity GKE by running:
  ```
  gcloud container clusters update fraud-detection \
  --zone us-central1-a \
  --workload-pool=$(gcloud config get-value project).svc.id.goog
  # check result: gcloud container clusters describe fraud-detection --zone us-central1-a --format="value(workloadIdentityConfig.workloadPool)"
  ```
  
