# Production environment for the fraud-detection platform.
#
# Fresh-project build: written as if provisioning a brand-new GCP project. Every
# resource is created from scratch; the only thing we reference rather than
# create is the auto-mode `default` VPC that a new project already ships with
# (see data.tf). Existing live resources are used only as a reference for the
# values below, not imported.
#
# Topology facts baked in here:
#   - Default auto-mode VPC (data source); its pre-existing PSA range serves Cloud SQL
#   - GKE auto-creates its Pod/Service secondary ranges on the default subnet
#   - ZONAL GKE cluster in us-central1-a, node pool uses the DEFAULT compute SA
#   - Cloud SQL POSTGRES_18, public+private IP, private IP on the default VPC
#   - Memorystore Redis, DIRECT_PEERING, reserved range 10.102.22.64/29
#   - 4 GCS buckets (models, dvc, loki chunks, loki ruler)
#   - Kafka is Aiven (managed, external) — not represented here

locals {
  workload_pool = "${var.project_id}.svc.id.goog"

  # Every GCP service account on the platform and the K8s SAs ("<ns>/<ksa>")
  # allowed to impersonate it via Workload Identity. Project roles mirror the
  # LIVE `getIamPolicy` output. (GKE nodes use the default compute SA, so there
  # is no custom node SA here.)
  service_accounts = {
    "fraud-detection-sa" = {
      display_name              = "fraud-detection-sa"
      project_roles             = ["roles/artifactregistry.reader", "roles/cloudsql.client", "roles/cloudsql.instanceUser"]
      workload_identity_members = ["core/fraud-detection-sa"]
    }
    "prediction-writer-sa" = {
      display_name              = "Prediction Writer Service Account"
      project_roles             = ["roles/artifactregistry.reader", "roles/cloudsql.client"]
      workload_identity_members = ["core/prediction-writer-sa"]
    }
    "drift-detection-api-sa" = {
      display_name              = "Drift Detection API Service Account"
      project_roles             = ["roles/artifactregistry.reader", "roles/cloudsql.client"]
      workload_identity_members = ["core/drift-detection-api-sa"]
    }
    "kserve-sa" = {
      display_name              = "kserve-sa"
      project_roles             = ["roles/storage.admin", "roles/storage.objectViewer"]
      workload_identity_members = ["serving/kserve-sa"]
    }
    "eso-secrets-reader" = {
      display_name              = "External Secrets Operator reader"
      project_roles             = ["roles/secretmanager.secretAccessor"]
      workload_identity_members = ["external-secrets/external-secrets"]
    }
    "grafana-sa" = {
      display_name              = "Grafana (Cloud SQL access)"
      project_roles             = ["roles/cloudsql.client", "roles/cloudsql.instanceUser"]
      workload_identity_members = ["monitoring/grafana-sa"]
    }
    # Loki reads/writes its GCS buckets; access is granted at the BUCKET level
    # (see module.gcs bucket_iam), so it has no project-level roles.
    "loki-sa" = {
      display_name              = "Loki (GCS chunk storage)"
      project_roles             = []
      workload_identity_members = ["monitoring/loki-sa"]
    }
    # Attached directly to the Airflow/MLflow VM (ADC, no Workload Identity):
    # cloud-sql-proxy -> cloudsql.client, MLflow/DVC -> GCS, image pulls -> AR.
    "virtualmachine-sa" = {
      display_name              = "Airflow/MLflow VM SA"
      project_roles             = ["roles/cloudsql.client", "roles/storage.objectAdmin", "roles/artifactregistry.reader"]
      workload_identity_members = []
    }
  }

  secret_ids = [
    "fraud-pg-user",
    "fraud-pg-password",
    "fraud-grafana-admin-user",
    "fraud-grafana-admin-password",
    "fraud-ingress-basic-auth",
  ]
}

module "iam" {
  source = "../../modules/iam"

  project_id       = var.project_id
  workload_pool    = local.workload_pool
  service_accounts = local.service_accounts
}

module "gke" {
  source = "../../modules/gke"

  project_id   = var.project_id
  location     = var.zone # ZONAL cluster
  cluster_name = var.cluster_name
  network      = data.google_compute_network.default.self_link
  subnetwork   = data.google_compute_subnetwork.default.self_link
  # Range names empty => GKE auto-creates the Pod/Service secondary ranges on the
  # default subnet (a fresh project has none named). Set names once you manage
  # the subnet's secondary ranges yourself.
  pods_range_name     = var.gke_pods_range_name
  services_range_name = var.gke_services_range_name
  node_pools          = var.gke_node_pools
  # node_service_account "" => default compute SA
  node_service_account = ""
}

# NOTE: the default VPC already has a Service Networking (PSA) connection and an
# allocated range, so we do NOT create one here (only one PSA connection is
# allowed per network). Cloud SQL's private IP consumes the existing range.

module "cloud_sql" {
  source = "../../modules/cloud_sql"

  project_id       = var.project_id
  region           = var.region
  instance_name    = var.sql_instance_name
  database_version = var.sql_database_version
  tier             = var.sql_tier
  disk_size_gb     = var.sql_disk_size_gb
  ipv4_enabled     = var.sql_ipv4_enabled
  private_network  = data.google_compute_network.default.self_link
  databases        = var.sql_databases
  users            = var.sql_users
  # Private IP uses the PSA range that already exists on the default VPC.
}

module "memorystore" {
  source = "../../modules/memorystore"

  project_id         = var.project_id
  region             = var.region
  name               = var.redis_instance_name
  memory_size_gb     = var.redis_memory_size_gb
  connect_mode       = var.redis_connect_mode
  reserved_ip_range  = var.redis_reserved_ip_range
  authorized_network = data.google_compute_network.default.self_link
}

module "gcs" {
  source = "../../modules/gcs"

  project_id = var.project_id
  location   = var.bucket_location
  buckets    = var.buckets
}

# Loki's GCS access, granted at the bucket level (loki-sa has no project roles).
resource "google_storage_bucket_iam_member" "loki" {
  for_each = toset(var.loki_buckets)

  bucket = each.value
  role   = "roles/storage.objectAdmin"
  member = module.iam.members["loki-sa"]

  depends_on = [module.gcs]
}

module "artifact_registry" {
  source = "../../modules/artifact_registry"

  project_id    = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repo
}

module "secret_manager" {
  source = "../../modules/secret_manager"

  project_id       = var.project_id
  secrets          = { for id in local.secret_ids : id => {} }
  accessor_members = []
}

module "compute_vm" {
  source = "../../modules/compute_vm"

  project_id            = var.project_id
  zone                  = var.zone
  name                  = var.vm_name
  machine_type          = var.vm_machine_type
  network               = data.google_compute_network.default.self_link
  subnetwork            = data.google_compute_subnetwork.default.self_link
  service_account_email = module.iam.emails["virtualmachine-sa"]
  # pd-standard (HDD) boot disk: the region's SSD_TOTAL_GB quota (250) is already
  # ~fully used by the GKE nodes, so keep the VM off SSD.
  boot_disk_type        = "pd-standard"
  assign_external_ip    = var.vm_assign_external_ip
  allowed_source_ranges = var.vm_app_allowed_source_ranges
}
