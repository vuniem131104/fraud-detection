variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  description = "Region for the Cloud SQL instance."
}

variable "instance_name" {
  type        = string
  description = "Cloud SQL instance name (the last segment of the connection name)."
}

variable "database_version" {
  type        = string
  description = "Postgres engine version."
  default     = "POSTGRES_16"
}

variable "tier" {
  type        = string
  description = "Machine tier (e.g. db-custom-2-7680, db-g1-small). Match the live instance to avoid a replace."
  default     = "db-custom-2-7680"
}

variable "edition" {
  type        = string
  description = "Cloud SQL edition. ENTERPRISE accepts db-custom-* tiers; ENTERPRISE_PLUS requires db-perf-optimized-N-* tiers."
  default     = "ENTERPRISE"
}

variable "disk_size_gb" {
  type        = number
  description = "Data disk size in GB."
  default     = 20
}

variable "availability_type" {
  type        = string
  description = "ZONAL or REGIONAL (REGIONAL = HA)."
  default     = "ZONAL"
}

variable "private_network" {
  type        = string
  description = "VPC self link the instance gets its private IP on."
}

variable "ipv4_enabled" {
  type        = bool
  description = "Whether the instance also has a public IPv4. The live instance has BOTH public and private IP (true)."
  default     = false
}

variable "ssl_mode" {
  type        = string
  description = "SSL enforcement mode. Live = ENCRYPTED_ONLY."
  default     = "ENCRYPTED_ONLY"
}

variable "databases" {
  type        = list(string)
  description = "Databases to create on the instance."
  default     = ["fraud-detection", "feast-registry", "mlflow", "airflow"]
}

variable "users" {
  description = "SQL users. Passwords are passed in from the caller (sourced from Secret Manager / tfvars, never committed)."
  type = map(object({
    password = string
  }))
  default = {}
}

variable "deletion_protection" {
  type        = bool
  description = "Block terraform from deleting the instance."
  default     = true
}
