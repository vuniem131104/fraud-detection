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
helm install kserve-crd oci://ghcr.io/kserve/charts/kserve-crd --version v0.18.0 -n kserve
helm install kserve oci://ghcr.io/kserve/charts/kserve-resources --version v0.18.0 -n kserve

# Install serving runtime depend on model type
For lightgbm, please run the following command to install lightgbm serving runtime:
```bash
kubectl apply -f - <<EOF
apiVersion: serving.kserve.io/v1alpha1
kind: ClusterServingRuntime
metadata:
  name: kserve-mlserver
spec:
  protocolVersions:
    - v2
  supportedModelFormats:
    - name: lightgbm
      version: "4"
      autoSelect: true
      priority: 2
    - name: sklearn
      version: "1"
      autoSelect: true
      priority: 2
  containers:
    - name: kserve-container
      image: docker.io/seldonio/mlserver:1.6.1
      env:
        - name: MLSERVER_HTTP_PORT
          value: "8080"
        - name: MLSERVER_GRPC_PORT
          value: "9000"
        - name: MODELS_DIR
          value: /mnt/models
      resources:
        requests:
          cpu: "1"
          memory: 2Gi
EOF
```