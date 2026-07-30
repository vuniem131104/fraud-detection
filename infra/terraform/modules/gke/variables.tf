variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "location" {
  type        = string
  description = "Cluster location: a zone (e.g. us-central1-a) for a zonal cluster, or a region for a regional one. The live cluster is ZONAL (us-central1-a)."
}

variable "cluster_name" {
  type        = string
  description = "GKE cluster name."
}

variable "network" {
  type        = string
  description = "VPC network self link / name the cluster attaches to."
}

variable "subnetwork" {
  type        = string
  description = "Subnet self link / name for the cluster nodes."
}

variable "pods_range_name" {
  type        = string
  description = "Secondary range name for Pods (VPC-native / alias IPs). Empty => GKE creates and manages the range itself."
  default     = ""
}

variable "services_range_name" {
  type        = string
  description = "Secondary range name for Services. Empty => GKE-managed services range (the live cluster uses a managed range, no named secondary)."
  default     = ""
}

variable "release_channel" {
  type        = string
  description = "GKE release channel (RAPID | REGULAR | STABLE)."
  default     = "REGULAR"
}

variable "node_pools" {
  description = "Map of node pools. Set autoscaling=false + node_count for a fixed-size pool (the live default-pool is fixed at 3, no autoscaling)."
  type = map(object({
    machine_type   = string
    autoscaling    = optional(bool, true)
    node_count     = optional(number, 1) # used when autoscaling=false
    min_node_count = optional(number, 1) # used when autoscaling=true
    max_node_count = optional(number, 3) # used when autoscaling=true
    disk_size_gb   = optional(number, 100)
    disk_type      = optional(string, "pd-balanced")
    spot           = optional(bool, false)
    labels         = optional(map(string), {})
  }))
}

variable "node_service_account" {
  type        = string
  description = "Email of the GCP service account attached to the nodes. Empty string uses the default compute SA (not recommended)."
  default     = ""
}

variable "deletion_protection" {
  type        = bool
  description = "Prevent accidental cluster deletion."
  default     = true
}
