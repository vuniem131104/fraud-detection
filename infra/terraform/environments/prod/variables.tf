# ── Core project / location ─────────────────────────────────────────────────
variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  description = "Primary region."
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "Zone for the ZONAL GKE cluster and the Airflow/MLflow VM."
  default     = "us-central1-a"
}

# ── GKE ─────────────────────────────────────────────────────────────────────
variable "cluster_name" {
  type        = string
  description = "GKE cluster name."
  default     = "fraud-detection"
}

variable "gke_pods_range_name" {
  type        = string
  description = "Secondary range name for Pods on the default subnet. Empty => GKE creates the range itself (default for a fresh project)."
  default     = ""
}

variable "gke_services_range_name" {
  type        = string
  description = "Secondary range name for Services on the default subnet. Empty => GKE creates the range itself (default for a fresh project)."
  default     = ""
}

variable "gke_node_pools" {
  type = map(object({
    machine_type   = string
    autoscaling    = optional(bool, true)
    node_count     = optional(number, 1)
    min_node_count = optional(number, 1)
    max_node_count = optional(number, 3)
    disk_size_gb   = optional(number, 100)
    disk_type      = optional(string, "pd-balanced")
    spot           = optional(bool, false)
    labels         = optional(map(string), {})
  }))
  description = "GKE node pools."
}

# ── Cloud SQL ───────────────────────────────────────────────────────────────
variable "sql_instance_name" {
  type        = string
  description = "Cloud SQL instance name (last segment of the connection name)."
  default     = "fraud-detection"
}

variable "sql_database_version" {
  type        = string
  description = "Postgres engine version (live: POSTGRES_18)."
  default     = "POSTGRES_18"
}

variable "sql_tier" {
  type        = string
  description = "Cloud SQL machine tier (live: db-custom-8-32768)."
  default     = "db-custom-8-32768"
}

variable "sql_disk_size_gb" {
  type        = number
  description = "Data disk size (live: 250)."
  default     = 250
}

variable "sql_ipv4_enabled" {
  type        = bool
  description = "Public IPv4 in addition to private IP (live: true)."
  default     = true
}

variable "sql_databases" {
  type        = list(string)
  description = "Databases on the instance."
  default     = ["fraud-detection", "feast-registry", "mlflow", "airflow"]
}

variable "sql_users" {
  type        = map(object({ password = string }))
  description = "SQL users. Provide the password via a non-committed *.auto.tfvars / TF_VAR_ (live user: vuniem)."
  default     = {}
  sensitive   = true
}

# ── Memorystore ─────────────────────────────────────────────────────────────
variable "redis_instance_name" {
  type        = string
  description = "Memorystore instance name (live: fraud-detection)."
  default     = "fraud-detection"
}

variable "redis_memory_size_gb" {
  type        = number
  description = "Memorystore capacity (live: 1)."
  default     = 1
}

variable "redis_connect_mode" {
  type        = string
  description = "DIRECT_PEERING or PRIVATE_SERVICE_ACCESS. Live: DIRECT_PEERING. Changing it forces a replace."
  default     = "DIRECT_PEERING"
}

variable "redis_reserved_ip_range" {
  type        = string
  description = "Reserved CIDR for the instance (live: 10.102.22.64/29 -> host 10.102.22.67)."
  default     = "10.102.22.64/29"
}

# ── GCS ─────────────────────────────────────────────────────────────────────
variable "bucket_location" {
  type        = string
  description = "Bucket location (live buckets are US multi-region)."
  default     = "US"
}

variable "buckets" {
  description = "GCS buckets keyed by name."
  type = map(object({
    versioning                  = optional(bool, true)
    force_destroy               = optional(bool, false)
    uniform_bucket_level_access = optional(bool, true)
    lifecycle_age_days          = optional(number, 0)
  }))
}

variable "loki_buckets" {
  type        = list(string)
  description = "Buckets Loki writes to (granted to loki-sa at the bucket level)."
  default     = ["fraud-detection-chunkss", "fraud-detection-rulerss"]
}

# ── Artifact Registry ───────────────────────────────────────────────────────
variable "artifact_registry_repo" {
  type        = string
  description = "Docker Artifact Registry repository ID."
  default     = "fraud-detection"
}

# ── Airflow/MLflow VM ───────────────────────────────────────────────────────
variable "vm_name" {
  type        = string
  description = "VM instance name (live: airflow-mlflow-fraud-detection)."
  default     = "airflow-mlflow-fraud-detection"
}

variable "vm_machine_type" {
  type        = string
  description = "VM machine type (live: e2-standard-2)."
  default     = "e2-standard-2"
}

variable "vm_assign_external_ip" {
  type        = bool
  description = "Give the VM an external IP (live: false — internal only)."
  default     = false
}

variable "vm_app_allowed_source_ranges" {
  type        = list(string)
  description = "CIDRs allowed to reach Airflow (8090) / MLflow (5000). Empty = closed."
  default     = []
}
