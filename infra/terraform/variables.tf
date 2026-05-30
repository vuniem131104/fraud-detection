variable "project_id" {
  description = "Google Cloud project ID that will own the Cloud SQL instance."
  type        = string
}

variable "region" {
  description = "Google Cloud region for the Cloud SQL instance."
  type        = string
  default     = "us-central1"
}

variable "enable_required_apis" {
  description = "Whether Terraform should enable required Google Cloud APIs."
  type        = bool
  default     = true
}

variable "model_storage_bucket_name" {
  description = "Cloud Storage bucket name for model artifacts. Bucket names are globally unique."
  type        = string
  default     = "fraud-detection"
}

variable "sql_instance_name" {
  description = "Cloud SQL instance name."
  type        = string
}

variable "database_version" {
  description = "Cloud SQL PostgreSQL database version, for example POSTGRES_18."
  type        = string
  default     = "POSTGRES_18"

  validation {
    condition     = can(regex("^POSTGRES_[0-9]+$", var.database_version))
    error_message = "database_version must look like POSTGRES_18."
  }
}

variable "edition" {
  description = "Cloud SQL edition."
  type        = string
  default     = "ENTERPRISE"

  validation {
    condition     = contains(["ENTERPRISE", "ENTERPRISE_PLUS"], var.edition)
    error_message = "edition must be ENTERPRISE or ENTERPRISE_PLUS."
  }
}

variable "tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-f1-micro"
}

variable "storage_size_gb" {
  description = "Initial storage size in GB."
  type        = number
  default     = 10

  validation {
    condition     = var.storage_size_gb >= 10
    error_message = "storage_size_gb must be at least 10."
  }
}

variable "disk_type" {
  description = "Cloud SQL disk type."
  type        = string
  default     = "PD_SSD"

  validation {
    condition     = contains(["PD_SSD", "PD_HDD"], var.disk_type)
    error_message = "disk_type must be PD_SSD or PD_HDD."
  }
}

variable "disk_autoresize" {
  description = "Whether Cloud SQL can automatically increase disk size."
  type        = bool
  default     = true
}

variable "availability_type" {
  description = "Cloud SQL availability type."
  type        = string
  default     = "ZONAL"

  validation {
    condition     = contains(["ZONAL", "REGIONAL"], var.availability_type)
    error_message = "availability_type must be ZONAL or REGIONAL."
  }
}

variable "backup_enabled" {
  description = "Whether automated backups are enabled."
  type        = bool
  default     = true
}

variable "point_in_time_recovery_enabled" {
  description = "Whether point-in-time recovery is enabled."
  type        = bool
  default     = true
}

variable "ipv4_enabled" {
  description = "Whether the instance exposes a public IPv4 address."
  type        = bool
  default     = true
}

variable "authorized_networks" {
  description = "Public CIDR blocks allowed to connect directly to the Cloud SQL public IP. Leave empty when using the Cloud SQL Auth Proxy."
  type = list(object({
    name  = string
    value = string
  }))
  default = []
}

variable "maintenance_day" {
  description = "Maintenance window day, from 1 for Monday to 7 for Sunday."
  type        = number
  default     = 7

  validation {
    condition     = var.maintenance_day >= 1 && var.maintenance_day <= 7
    error_message = "maintenance_day must be between 1 and 7."
  }
}

variable "maintenance_hour" {
  description = "Maintenance window hour in UTC."
  type        = number
  default     = 3

  validation {
    condition     = var.maintenance_hour >= 0 && var.maintenance_hour <= 23
    error_message = "maintenance_hour must be between 0 and 23."
  }
}

variable "deletion_protection" {
  description = "Whether Cloud SQL deletion protection is enabled."
  type        = bool
  default     = true
}

variable "postgres_user" {
  description = "Application PostgreSQL user to create."
  type        = string
}

variable "postgres_password" {
  description = "Password for the postgres root user and application user."
  type        = string
  sensitive   = true
}

variable "main_db" {
  description = "Main application database name."
  type        = string
  default     = "fraud_detection"
}
