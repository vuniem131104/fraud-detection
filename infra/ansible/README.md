# Ansible — configure & deploy the Airflow/MLflow VM

Configures the Compute Engine VM created by Terraform (`module.compute_vm`) and
deploys the **Airflow + MLflow + Cloud SQL Proxy** docker-compose stack from
[`airflow/`](../../airflow) onto it. The in-cluster services are deployed with
Helm (see `docs/`), not Ansible.

## Roles

| Role | Responsibility |
|---|---|
| `common` | Base packages, timezone (`Asia/Ho_Chi_Minh`), swap file |
| `docker` | Docker Engine + Compose plugin (official apt repo), docker group |
| `gcp` | Google Cloud CLI + Artifact Registry docker auth via the VM's attached SA (no key files) |
| `airflow_stack` | Copy the `airflow/` build context, render `.env`, `docker compose up -d`, health-check |

```
infra/ansible/
├── ansible.cfg
├── requirements.yml            # community.docker, community.general, ansible.posix
├── inventory/prod/
│   ├── hosts.yml               # the VM (external IP or IAP tunnel)
│   └── group_vars/
│       ├── all.yml             # non-secret config (project, DB, redis, bucket…)
│       └── vault.yml.example   # secrets template → ansible-vault encrypt as vault.yml
├── playbooks/site.yml
└── roles/{common,docker,gcp,airflow_stack}/
```

## Prerequisites

- Ansible ≥ 2.15 on the control node; `ansible-galaxy collection install -r requirements.yml`.
- The VM provisioned by Terraform, with its service account attached
  (`airflow-mlflow-virtualmachine-sa`). Cloud SQL Proxy and MLflow authenticate through the
  metadata server — **no key files on the box**.
- Cloud SQL reachable from the VM by private IP (Terraform sets up the VPC/PSA).

## Configure

1. **Inventory** — set `ansible_host` (VM IP from
   `terraform output -raw vm_external_ip`) and `ansible_user` (your OS-Login
   user) in `inventory/prod/hosts.yml`. For a VM with no external IP, use the
   commented IAP `ProxyCommand`.
2. **Non-secrets** — `inventory/prod/group_vars/all.yml`. Set `redis_host` to
   `terraform output -raw redis_host`; the rest already match the repo.
3. **Secrets** — create the encrypted vault:
   ```bash
   cd inventory/prod/group_vars
   cp vault.yml.example vault.yml
   # edit values (DB user = vuniem, its current password, JWT secret, admin login)
   ansible-vault encrypt vault.yml
   ```

## Deploy

```bash
cd infra/ansible
ansible-galaxy collection install -r requirements.yml
ansible-playbook playbooks/site.yml --ask-vault-pass
```

Re-runs are idempotent; changes to the build context, code, or `.env` trigger a
`docker compose up` that restarts only the affected services. Target one role
with tags, e.g. `--tags deploy` (just re-deploy the stack) or `--tags docker`.

## Verify

```bash
ssh <user>@<vm-ip>
cd /opt/airflow-stack && docker compose ps
curl -f http://localhost:8090/health     # Airflow API
curl -f http://localhost:5000/           # MLflow UI
```

The Airflow UI (`8090`) and MLflow (`5000`) are only reachable from the CIDRs in
`vm_app_allowed_source_ranges` (Terraform firewall) — keep that locked to your
office/VPN.

## Notes

- DB passwords are inserted into `.env` raw; keep them URL-safe (the rotation
  guide uses `openssl rand -hex`), matching how `airflow/docker-compose.yml`
  assembles the MLflow/Feast URIs.
- `.env` is rendered with `no_log` and `mode 0600`; the real `vault.yml` is
  git-ignored.
