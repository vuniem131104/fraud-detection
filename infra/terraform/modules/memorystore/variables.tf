variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  description = "Region for the Memorystore instance."
}

variable "name" {
  type        = string
  description = "Memorystore Redis instance name."
}

variable "tier" {
  type        = string
  description = "BASIC (single node) or STANDARD_HA (replicated)."
  default     = "BASIC"
}

variable "memory_size_gb" {
  type        = number
  description = "Capacity in GB."
  default     = 1
}

variable "redis_version" {
  type        = string
  description = "Redis engine version."
  default     = "REDIS_7_2"
}

variable "authorized_network" {
  type        = string
  description = "VPC self link the instance is reachable from (GKE pods + Airflow VM)."
}

variable "connect_mode" {
  type        = string
  description = "DIRECT_PEERING or PRIVATE_SERVICE_ACCESS."
  default     = "PRIVATE_SERVICE_ACCESS"
}

variable "reserved_ip_range" {
  type        = string
  description = "Optional reserved range name (PSA) or CIDR. Empty lets GCP pick."
  default     = ""
}
