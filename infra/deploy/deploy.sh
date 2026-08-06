#!/usr/bin/env bash
# =============================================================================
# One-shot deploy for the fraud-detection platform.
#
# Brings a GKE cluster from "exists + Workload Identity" to the full running
# system: GCP APIs, cluster dependencies (cert-manager, Knative, Istio, KServe,
# KEDA, ingress-nginx, External Secrets), then all app + monitoring workloads.
#
# Every step is idempotent (gcloud enable / add-iam-policy-binding, kubectl
# apply, helm upgrade --install), so re-running converges instead of duplicating.
# Versions/identifiers live in config.env, pinned to the live install.
#
#   ./infra/deploy/deploy.sh                 # run everything, in order
#   ./infra/deploy/deploy.sh apps verify     # only the named phases
#   ./infra/deploy/deploy.sh --list          # show phases
#   DRY_RUN=1 ./infra/deploy/deploy.sh        # print commands, run nothing
#
# Not in scope (owned by Terraform / the docs): creating the cluster, Cloud SQL,
# Redis, buckets, the GCP service accounts and their project-level roles.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"
# shellcheck source=config.env
source "$SCRIPT_DIR/config.env"

DRY_RUN="${DRY_RUN:-0}"

# ── logging ──────────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then B=$'\e[1;34m'; G=$'\e[1;32m'; Y=$'\e[1;33m'; R=$'\e[1;31m'; D=$'\e[2m'; N=$'\e[0m'; else B= G= Y= R= D= N=; fi
log()  { printf '%s\n' "${B}==>${N} $*"; }
ok()   { printf '%s\n' "${G} ok${N} $*"; }
warn() { printf '%s\n' "${Y} !!${N} $*" >&2; }
die()  { printf '%s\n' "${R}xxx${N} $*" >&2; exit 1; }
run()  { printf '%s\n' "${D}  \$ $*${N}"; [[ "$DRY_RUN" == "1" ]] || "$@"; }

# helm upgrade --install with common flags
helmi() { run helm upgrade --install "$@"; }

# apply a kubectl manifest, tolerating a missing file with a warning
kapply() { [[ -f "$1" ]] && run kubectl apply -f "$1" || warn "missing manifest: $1"; }

ensure_ns() { run kubectl get ns "$1" >/dev/null 2>&1 || run kubectl create ns "$1"; }

# =============================================================================
# Phases
# =============================================================================

phase_preflight() {
  log "Preflight"
  for t in gcloud kubectl helm; do command -v "$t" >/dev/null || die "missing required tool: $t"; done
  gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q . \
    || die "no active gcloud account — run: gcloud auth login"
  run gcloud config set project "$PROJECT_ID" >/dev/null
  ok "project=$PROJECT_ID cluster=$CLUSTER_NAME zone=$ZONE"
}

phase_apis() {
  log "Enable GCP APIs (${#REQUIRED_APIS[@]})"
  # One batch call — enabling already-enabled APIs is a no-op.
  run gcloud services enable "${REQUIRED_APIS[@]}" --project "$PROJECT_ID"
  ok "APIs enabled"
}

phase_credentials() {
  log "Fetch cluster credentials"
  run gcloud container clusters get-credentials "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID"
  ok "kubectl context -> $CLUSTER_NAME"
}

# Ensure the node pools match NODE_POOLS_SPEC (create missing, converge counts).
# Idempotent: on a cluster already at target this is a no-op; on the current
# 3-node cluster it adds small-pool + shrinks default-pool to land on 2×4 + 1×2.
phase_nodes() {
  log "Ensure node pools (target layout)"
  gcloud container clusters describe "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1 \
    || die "cluster $CLUSTER_NAME not found — create it first (Terraform: infra/terraform)"
  for spec in "${NODE_POOLS_SPEC[@]}"; do
    local name="${spec%%=*}" rest="${spec#*=}"
    local machine="${rest%%:*}" want="${rest##*:}"
    if gcloud container node-pools describe "$name" --cluster "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID" >/dev/null 2>&1; then
      local have; have="$(kubectl get nodes -l cloud.google.com/gke-nodepool="$name" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
      if [[ "$have" == "$want" ]]; then
        ok "pool $name: $have node(s) == target — no change"
      elif [[ "$want" -lt "$have" && "$NODES_ALLOW_SHRINK" != "1" ]]; then
        warn "pool $name has $have > target $want, but NODES_ALLOW_SHRINK=0 — not draining"
      else
        log "pool $name: resize $have -> $want (a shrink drains node(s))"
        run gcloud container clusters resize "$CLUSTER_NAME" --node-pool "$name" \
          --num-nodes "$want" --zone "$ZONE" --project "$PROJECT_ID" --quiet
      fi
    else
      log "pool $name: create ${want}× ${machine}"
      run gcloud container node-pools create "$name" \
        --cluster "$CLUSTER_NAME" --zone "$ZONE" --project "$PROJECT_ID" \
        --machine-type "$machine" --num-nodes "$want" --disk-size "$NODE_DISK_SIZE" \
        --workload-metadata=GKE_METADATA
    fi
  done
  ok "node pools ensured"
}

phase_workload_identity() {
  log "Assert Workload Identity bindings (KSA -> GSA)"
  local pool="${PROJECT_ID}.svc.id.goog"
  for b in "${WORKLOAD_IDENTITY_BINDINGS[@]}"; do
    local gsa="${b%%=*}" ksa="${b#*=}"
    run gcloud iam service-accounts add-iam-policy-binding \
      "${gsa}@${PROJECT_ID}.iam.gserviceaccount.com" \
      --project "$PROJECT_ID" \
      --role roles/iam.workloadIdentityUser \
      --member "serviceAccount:${pool}[${ksa}]" --quiet >/dev/null \
      || warn "WI binding failed for ${gsa} (does the GCP SA exist? it is created by Terraform/docs)"
  done
  ok "Workload Identity bindings asserted"
}

phase_cluster_deps() {
  log "Cluster dependencies"

  # cert-manager (KServe webhook certs depend on it)
  log "  cert-manager ${CERT_MANAGER_VERSION}"
  kapply "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
  run kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout=180s

  # Knative Serving
  log "  Knative Serving ${KNATIVE_VERSION}"
  kapply "https://github.com/knative/serving/releases/download/knative-${KNATIVE_VERSION}/serving-crds.yaml"
  kapply "https://github.com/knative/serving/releases/download/knative-${KNATIVE_VERSION}/serving-core.yaml"

  # Istio (net-istio) + point Knative at the Istio ingress class
  log "  Istio / net-istio ${KNATIVE_VERSION}"
  run kubectl apply -l knative.dev/crd-install=true -f "https://github.com/knative-extensions/net-istio/releases/download/knative-${KNATIVE_VERSION}/istio.yaml"
  kapply "https://github.com/knative-extensions/net-istio/releases/download/knative-${KNATIVE_VERSION}/istio.yaml"
  kapply "https://github.com/knative-extensions/net-istio/releases/download/knative-${KNATIVE_VERSION}/net-istio.yaml"
  run kubectl patch configmap/config-network -n knative-serving --type merge \
    -p '{"data":{"ingress-class":"istio.ingress.networking.knative.dev"}}'
  optimize_istio

  # KServe (CRDs then resources)
  log "  KServe ${KSERVE_VERSION}"
  helmi kserve-crd "oci://ghcr.io/kserve/charts/kserve-crd" --version "$KSERVE_VERSION" -n "$NS_KSERVE" --create-namespace --wait
  helmi kserve      "oci://ghcr.io/kserve/charts/kserve-resources" --version "$KSERVE_VERSION" -n "$NS_KSERVE" --wait
  run kubectl wait --for=condition=established --timeout=120s \
    crd/inferenceservices.serving.kserve.io crd/clusterservingruntimes.serving.kserve.io
  # LightGBM serving runtime the InferenceService selects (runtime: kserve-mlserver)
  kapply "$MANIFESTS_DIR/lightgbm-serving-runtime.yaml"

  # KEDA (prediction-writer's ScaledObject depends on the CRDs)
  log "  KEDA ${KEDA_VERSION}"
  helmi keda keda --repo "$KEDA_REPO" --version "$KEDA_VERSION" -n "$NS_KEDA" --create-namespace --wait

  # ingress-nginx
  log "  ingress-nginx ${INGRESS_NGINX_VERSION}"
  helmi ingress-nginx ingress-nginx --repo "$INGRESS_NGINX_REPO" --version "$INGRESS_NGINX_VERSION" \
    -n "$NS_INGRESS" --create-namespace \
    --set controller.service.type=LoadBalancer \
    --set controller.resources.requests.cpu=100m \
    --set controller.resources.requests.memory=128Mi \
    --set controller.resources.limits.cpu=300m \
    --set controller.resources.limits.memory=256Mi \
    --wait
  local lb
  lb="$(kubectl get svc -n "$NS_INGRESS" ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true)"
  [[ -n "$lb" ]] && ok "ingress LB IP: ${lb} (fraud_detection values host uses <ip>.sslip.io)"

  # External Secrets Operator (impersonates eso-secrets-reader via WI)
  log "  External Secrets Operator (chart ${ESO_CHART_VERSION:-latest})"
  local eso_ver=(); [[ -n "$ESO_CHART_VERSION" ]] && eso_ver=(--version "$ESO_CHART_VERSION")
  helmi external-secrets external-secrets --repo "$ESO_REPO" "${eso_ver[@]}" \
    -n "$NS_ESO" --create-namespace \
    --set "serviceAccount.annotations.iam\.gke\.io/gcp-service-account=eso-secrets-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
    --wait
  ok "cluster dependencies ready"
}

# Trim the default net-istio footprint: 1 lean ingress gateway (no HPA fan-out)
# + a small istiod. Safe to run standalone: `./deploy.sh optimize_istio`.
optimize_istio() {
  log "  optimize Istio footprint (gateway ${ISTIO_GATEWAY_REPLICAS} replica, lean requests)"
  run kubectl -n istio-system rollout status deploy/istio-ingressgateway --timeout=120s || warn "istio-ingressgateway not ready yet"
  # Gateway: pin replicas, remove its autoscaler so it can't fan out, shrink resources.
  run kubectl -n istio-system delete hpa istio-ingressgateway --ignore-not-found
  run kubectl -n istio-system scale deploy/istio-ingressgateway --replicas="$ISTIO_GATEWAY_REPLICAS"
  run kubectl -n istio-system set resources deploy/istio-ingressgateway \
    --requests="$ISTIO_GATEWAY_REQ" --limits="$ISTIO_GATEWAY_LIM"
  # istiod: small requests; cap the HPA so it stays at 1–2.
  run kubectl -n istio-system set resources deploy/istiod \
    --requests="$ISTIOD_REQ" --limits="$ISTIOD_LIM"
  run kubectl -n istio-system patch hpa istiod --type=merge \
    -p "{\"spec\":{\"minReplicas\":${ISTIOD_MIN_REPLICAS},\"maxReplicas\":${ISTIOD_MAX_REPLICAS}}}" 2>/dev/null \
    || warn "no istiod HPA to cap (ok)"
  ok "Istio right-sized"
}

phase_secrets() {
  log "Secret plumbing (ClusterSecretStore + ExternalSecrets + kafka-certs)"
  ensure_ns "$NS_CORE"; ensure_ns "$NS_MONITORING"
  kapply "$MANIFESTS_DIR/cluster-secret-store.yaml"
  run kubectl wait --for=condition=Ready --timeout=120s clustersecretstore/gcp-secret-manager || warn "ClusterSecretStore not Ready yet (WI can take ~30s to propagate)"
  for m in fraud-detection-secrets prediction-writer-secrets drift-detection-secrets \
           grafana-secrets grafana-admin-external-secret basic-auth-secret; do
    kapply "$MANIFESTS_DIR/${m}.yaml"
  done

  # Kafka (Aiven) TLS certs — not in Secret Manager; built from the certs/ dir.
  # Note: NOT wrapped in run() — routing a log line into the apply pipe would
  # corrupt the YAML on stdin.
  if [[ -f "$CERTS_DIR/ca.pem" && -f "$CERTS_DIR/service.cert" && -f "$CERTS_DIR/service.key" ]]; then
    printf '%s\n' "${D}  \$ kubectl create secret generic kafka-certs -n $NS_CORE --from-file=... | kubectl apply -f -${N}"
    if [[ "$DRY_RUN" != "1" ]]; then
      kubectl create secret generic kafka-certs -n "$NS_CORE" \
        --from-file=ca.pem="$CERTS_DIR/ca.pem" \
        --from-file=service.cert="$CERTS_DIR/service.cert" \
        --from-file=service.key="$CERTS_DIR/service.key" \
        --dry-run=client -o yaml | kubectl apply -f -
      ok "kafka-certs applied"
    fi
  else
    warn "certs not found in $CERTS_DIR — skipping kafka-certs (fraud-detection & prediction-writer need it)"
  fi
}

phase_config() {
  log "ConfigMaps"
  ensure_ns "$NS_CORE"
  # These two have no namespace in metadata — apply into core explicitly.
  run kubectl apply -n "$NS_CORE" -f "$MANIFESTS_DIR/fraud-detection-configmap.yaml"
  run kubectl apply -n "$NS_CORE" -f "$MANIFESTS_DIR/prediction-writer-configmap.yaml"
  # drift-detection-configmap already sets namespace: core
  kapply "$MANIFESTS_DIR/drift-detection-configmap.yaml"
  ok "configmaps applied"
}

phase_apps() {
  log "Application workloads"
  ensure_ns "$NS_CORE"; ensure_ns "$NS_SERVING"; ensure_ns "$NS_MONITORING"

  # Model serving (KServe InferenceService) — needs kserve + mlserver runtime + kserve-sa.
  helmi serving             "$HELM_DIR/serving"           -n "$NS_SERVING"    --create-namespace
  # Core services (share namespace core).
  helmi fraud-detection     "$HELM_DIR/fraud_detection"   -n "$NS_CORE"
  helmi prediction-writer   "$HELM_DIR/prediction_writer" -n "$NS_CORE"
  helmi drift-detection-api "$HELM_DIR/drift_detection"   -n "$NS_CORE"

  # Monitoring stack (local vendored charts).
  helmi prometheus "$HELM_DIR/prometheus" -n "$NS_MONITORING" --create-namespace
  helmi loki       "$HELM_DIR/loki"       -n "$NS_MONITORING"
  helmi grafana    "$HELM_DIR/grafana"    -n "$NS_MONITORING"
  helmi alloy      "$HELM_DIR/alloy"      -n "$NS_MONITORING"
  ok "app releases applied"
}

# Standalone entrypoint for the Istio right-sizing (also runs inside cluster_deps).
phase_optimize_istio() { optimize_istio; }

phase_verify() {
  log "Verify"
  run kubectl get pods -n "$NS_CORE" -o wide || true
  run kubectl get inferenceservice -n "$NS_SERVING" || true
  run kubectl get scaledobject -n "$NS_CORE" || true
  run kubectl get externalsecret -A || true
  run helm list -A || true
  ok "done — see status above"
}

# =============================================================================
# Runner
# =============================================================================
# `nodes` runs AFTER `apps` on purpose: the leaner requests are applied first, so
# shrinking a pool (draining a node) reschedules already-small pods cleanly. On a
# cluster already at the target layout the phase is a no-op wherever it sits.
declare -a ALL_PHASES=(preflight apis credentials workload_identity cluster_deps secrets config apps nodes verify)

usage() {
  cat <<EOF
Usage: ${0##*/} [phase ...]

Phases (default: all, in this order):
  preflight           check tools + gcloud auth, set project
  apis                enable required GCP APIs
  credentials         gcloud get-credentials for the cluster
  nodes               ensure node pools = target layout (2x 4-vCPU + 1x 2-vCPU); alias: reconfigure
  workload_identity   assert KSA->GSA Workload Identity bindings
  cluster_deps        cert-manager, Knative, Istio, KServe(+runtime), KEDA, ingress-nginx, ESO
  optimize_istio      right-size istiod + ingress gateway (alias: istio; runs inside cluster_deps too)
  secrets             ClusterSecretStore + ExternalSecrets + kafka-certs
  config              ConfigMaps
  apps                serving, fraud-detection, prediction-writer, drift-detection, monitoring
  verify              print cluster status

Env: DRY_RUN=1 prints commands without running. Overrides in config.env.
Aliases: deps=cluster_deps, iam/wi=workload_identity, creds=credentials
EOF
}

main() {
  local -a phases=()
  if [[ $# -eq 0 ]]; then
    phases=("${ALL_PHASES[@]}")
  else
    for a in "$@"; do
      case "$a" in
        -h|--help) usage; exit 0 ;;
        --list) printf '%s\n' "${ALL_PHASES[@]}"; exit 0 ;;
        all) phases=("${ALL_PHASES[@]}") ;;
        deps) phases+=(cluster_deps) ;;
        creds) phases+=(credentials) ;;
        iam|wi) phases+=(workload_identity) ;;
        istio) phases+=(credentials optimize_istio) ;;
        reconfigure) phases+=(credentials nodes) ;;
        preflight|apis|credentials|nodes|workload_identity|cluster_deps|optimize_istio|secrets|config|apps|verify) phases+=("$a") ;;
        *) die "unknown phase: $a (see --help)" ;;
      esac
    done
  fi
  # preflight always runs first for context/auth unless it's the only ask
  [[ " ${phases[*]} " == *" preflight "* ]] || phases=(preflight "${phases[@]}")

  [[ "$DRY_RUN" == "1" ]] && warn "DRY_RUN=1 — no commands will execute"
  for p in "${phases[@]}"; do "phase_$p"; done
  log "All requested phases complete."
}

main "$@"
