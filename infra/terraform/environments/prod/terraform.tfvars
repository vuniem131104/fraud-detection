# Live values read from GCP on 2026-07-14 (gcloud describe). These match the
# running system so `terraform plan` after import should be clean. Reconcile the
# few settings marked CHECK against `gcloud ... describe` if a plan shows drift.

project_id = "project-57f7ef9a-6059-4068-ae7"
region     = "us-central1"
zone       = "us-central1-a"

# ── GKE (ZONAL, on the default VPC) ──────────────────────────────────────────
# Pod/Service secondary range names left unset => GKE auto-creates them on the
# default subnet (a fresh project has none named).
cluster_name = "fraud-detection"
# Target layout: 2× 4-vCPU + 1× 2-vCPU = 10 vCPU / 24 GB (frees 2 vCPU of regional
# quota for the airflow-mlflow VM). A GKE node pool has a single machine type, so
# the heterogeneous "Node 3 = 2 vCPU" needs a second pool.
gke_node_pools = {
  "default-pool" = {
    machine_type = "e2-custom-4-8192"
    autoscaling  = false
    node_count   = 2
    disk_size_gb = 75
  }
  "small-pool" = {
    machine_type = "e2-custom-2-8192"
    autoscaling  = false
    node_count   = 1
    disk_size_gb = 75
  }
}

# ── Cloud SQL (POSTGRES_18, public+private IP) ───────────────────────────────
sql_instance_name    = "fraud-detection"
sql_database_version = "POSTGRES_18"
sql_tier             = "db-custom-8-32768"
sql_disk_size_gb     = 250
sql_ipv4_enabled     = true
sql_databases        = ["fraud-detection", "feast-registry", "mlflow", "airflow"]
sql_users = {}

# ── Memorystore Redis (DIRECT_PEERING) ───────────────────────────────────────
redis_instance_name     = "fraud-detection"
redis_memory_size_gb    = 1
redis_connect_mode      = "DIRECT_PEERING"
redis_reserved_ip_range = "10.102.22.64/29" # -> host 10.102.22.67

# ── GCS (4 buckets, US multi-region) ─────────────────────────────────────────
bucket_location = "US"
buckets = {
  # UBLA must be true: the org enforces constraints/storage.uniformBucketLevelAccess
  # (creating a bucket with UBLA=false returns 412 conditionNotMet).
  "fraud-detection-modelss" = { versioning = false, uniform_bucket_level_access = true }
  "fraud-detection-dvc"     = { versioning = false, uniform_bucket_level_access = true }
  "fraud-detection-chunkss" = { versioning = false, uniform_bucket_level_access = true }
  "fraud-detection-rulerss" = { versioning = false, uniform_bucket_level_access = true }
}
loki_buckets = ["fraud-detection-chunkss", "fraud-detection-rulerss"]

# ── Artifact Registry ────────────────────────────────────────────────────────
artifact_registry_repo = "fraud-detection"

# ── Airflow/MLflow VM (internal-only, currently STOPPED) ─────────────────────
vm_name                  = "airflow-mlflow-fraud-detection"
vm_machine_type          = "e2-standard-2"
# vm_service_account_email removed: virtualmachine-sa is now created and wired by
# module.iam (see main.tf service_accounts).
vm_assign_external_ip        = false
vm_app_allowed_source_ranges = []
