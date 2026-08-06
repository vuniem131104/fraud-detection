# Terraform — GCP infrastructure for the fraud-detection platform

Provisions everything the platform runs on in GCP. Cluster add-ons (KServe,
KEDA, ESO, ingress-nginx, monitoring) stay in Helm — see `docs/`. Kafka is Aiven
(managed, external) and is not represented here.

## Layout

Service-per-module, composed by one root per environment — the
[small→large layout](https://ajdevopssolutions.medium.com/best-practices-for-terraform-directory-layout-from-small-to-large-projects-4d4ff41cf93c):
reusable `modules/`, thin `environments/<env>/` roots that wire them.

The live platform runs on the project's **default auto-mode VPC**, so `prod`
references it via data sources (`data.tf`) and does **not** use `modules/network`
— that module is kept for a greenfield/dedicated-VPC build only.

```
infra/terraform/
├── modules/
│   ├── network/            (greenfield only) VPC, subnet, Cloud NAT, PSA, firewall
│   ├── gke/                VPC-native cluster (zonal or regional), node pools, Workload Identity
│   ├── cloud_sql/          Postgres instance (private IP), databases, users
│   ├── memorystore/        Redis (online feature store)
│   ├── gcs/                Buckets (MLflow artifacts + DVC)
│   ├── artifact_registry/  Docker repo for app images
│   ├── secret_manager/     Secret containers backing External Secrets Operator
│   ├── iam/                Service accounts, project roles, Workload Identity bindings
│   └── compute_vm/         Airflow/MLflow VM (the Ansible target)
└── environments/
    └── prod/               Root: wires all modules, tfvars, backend
```

## What maps to what (reverse-engineered from the repo)

| Resource | Live identifier | Source in repo |
|---|---|---|
| Project | `project-57f7ef9a-6059-4068-ae7` | docs, `.env` |
| Region | `us-central1` | chart image paths, docs |
| VPC | `default` (auto-mode) — referenced, not managed | live GCP |
| GKE | `fraud-detection`, **zonal** us-central1-a, node pool `default-pool` (fixed 3× e2-custom-4-8192, default compute SA) | live GCP |
| Cloud SQL | `…:us-central1:fraud-detection` **POSTGRES_18**, tier db-custom-8-32768, 250GB, public+private IP (DBs: fraud-detection, feast-registry, mlflow, airflow; user `vuniem`) | manifests, compose, live GCP |
| Memorystore | `fraud-detection`, **DIRECT_PEERING**, 10.102.22.64/29 → host 10.102.22.67 | configmap, live GCP |
| GCS buckets | `fraud-detection-modelss`, `-dvc`, `-chunkss`, `-rulerss` (US) | serving/loki values |
| Artifact Registry | `fraud-detection` (docker) | `fraud_detection/values.yaml` image |
| Secret Manager | `fraud-pg-user`, `fraud-pg-password`, `fraud-grafana-admin-*`, `fraud-ingress-basic-auth` | Centralize Secret Management doc |
| GSAs | `fraud-detection-sa`, `prediction-writer-sa`, `drift-detection-api-sa`, `kserve-sa`, `eso-secrets-reader`, `grafana-sa`, `loki-sa` | chart values, live GCP |
| VM SA | `virtualmachine-sa` (created by `module.iam`, attached to the VM via ADC) | live GCP |
| Kafka | Aiven `…aivencloud.com:17550` | configmaps — **external, not managed here** |

> The **app/cluster layer** (Helm charts, KServe/KEDA/ESO, monitoring) is deployed
> by [`infra/deploy/deploy.sh`](../deploy/), not Terraform. Terraform owns the GCP
> resources above; the deploy script owns everything inside the cluster.

## Prerequisites

- Terraform ≥ 1.5, `gcloud` authenticated on the project.
- APIs enabled: `compute`, `container`, `sqladmin`, `redis`, `servicenetworking`,
  `artifactregistry`, `secretmanager`, `storage`, `iam`.
- A GCS bucket for remote state (created out of band — see `backend.tf`).

## First run: greenfield build (create everything)

Written for a brand-new project — every resource is created from scratch. Nothing
is imported.

```bash
cd environments/prod

# 1. State bucket (once), then init pointing at it
terraform init -backend-config="bucket=fraud-detection-tfstatess"

# 2. Put the DB password in secrets.auto.tfvars (copy secrets.auto.tfvars.example).

# 3. Plan — everything should be "+ create", no errors
terraform plan

# 4. Apply
terraform apply
```

> The `default` VPC/subnet are read via data sources (a new project ships with
> them); a PSA range is created on the default VPC for the Cloud SQL private IP,
> and GKE auto-creates its Pod/Service secondary ranges. IAM members (project
> roles, Workload Identity, loki bucket IAM) are additive.
>
> If a resource already exists (e.g. you kept a bucket from a previous run),
> `apply` fails with `409 already exists` on that one resource — `terraform import`
> just that address, then re-apply.

## Day-2

```bash
terraform fmt -recursive      # format
terraform validate            # after init
terraform plan && terraform apply
```

Passwords/secret values are never in Terraform: DB user password comes from
`secrets.auto.tfvars` (git-ignored) / `TF_VAR_sql_users`, and Secret Manager
**values** are seeded and rotated with `gcloud` (see
`docs/Centralize Secret Management.md`) — this code only manages the containers.

## Outputs

`terraform output` surfaces the values the rest of the stack needs:
`cloud_sql_connection_name`, `redis_host`, `artifact_registry_url`,
`model_bucket`, `service_account_emails`, `vm_external_ip`. Feed the VM IP and
`redis_host` into the Ansible inventory/group_vars.
