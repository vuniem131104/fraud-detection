# infra/

> **Deploy toàn bộ hệ thống từ số 0:** làm theo [DEPLOY.md](DEPLOY.md) — runbook
> step-by-step, đánh dấu rõ bước nào MANUAL 🔧 và bước nào AUTO ⚙️.

Infrastructure for the fraud-detection platform, in the order it's applied:

| Dir | Tool | What it does |
|---|---|---|
| [`terraform/`](terraform/) | Terraform | GCP resources: GKE, Cloud SQL, Memorystore, GCS, Artifact Registry, Secret Manager, IAM, and the Airflow/MLflow VM |
| [`deploy/`](deploy/) | Bash | **One-shot** cluster deploy: enable APIs → cluster deps (KServe/KEDA/ESO/ingress/monitoring) → all app workloads |
| [`ansible/`](ansible/) | Ansible | Configures the VM and deploys the Airflow + MLflow + Cloud SQL Proxy compose stack |
| [`k8s/`](k8s/) | Helm + manifests | In-cluster service charts + manifests (consumed by `deploy/`) |
| [`docker/`](docker/) | Docker | Service image builds + local compose |

Flow: **Terraform** stands up the cluster and cloud services → **`deploy/deploy.sh`**
installs cluster dependencies and all GKE workloads in one run → **Ansible** brings
up the Airflow/MLflow VM stack. Kafka is Aiven (managed, external) and lives
outside all of these.

```bash
# typical order
terraform -chdir=terraform/environments/prod apply   # (or ./import.sh first, on the live env)
./deploy/deploy.sh                                    # cluster deps + all app pods, one shot
ansible-playbook -i ansible/inventory/prod ansible/playbooks/site.yml --ask-vault-pass
```

Terraform outputs feed the later stages: `redis_host` and `vm_external_ip` →
Ansible inventory/group_vars; `cloud_sql_connection_name`, `artifact_registry_url`
and the service-account emails → the Helm values / manifests already in `k8s/`.
