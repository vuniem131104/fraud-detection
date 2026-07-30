variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  description = "Region for the subnet, router and NAT."
}

variable "network_name" {
  type        = string
  description = "Name of the VPC network."
}

variable "subnet_name" {
  type        = string
  description = "Name of the primary subnet that hosts the GKE nodes and the Airflow/MLflow VM."
}

variable "subnet_cidr" {
  type        = string
  description = "Primary CIDR range for the subnet (node IPs)."
  default     = "10.10.0.0/20"
}

variable "pods_range_name" {
  type        = string
  description = "Name of the secondary range used for GKE Pods."
  default     = "gke-pods"
}

variable "pods_cidr" {
  type        = string
  description = "Secondary CIDR range for GKE Pods."
  default     = "10.20.0.0/14"
}

variable "services_range_name" {
  type        = string
  description = "Name of the secondary range used for GKE Services."
  default     = "gke-services"
}

variable "services_cidr" {
  type        = string
  description = "Secondary CIDR range for GKE Services."
  default     = "10.24.0.0/20"
}

variable "psa_prefix_length" {
  type        = number
  description = "Prefix length for the Private Service Access range (Cloud SQL + Memorystore private IPs)."
  default     = 16
}

variable "master_ipv4_cidr" {
  type        = string
  description = "Reserved /28 for the GKE control plane (only used when the cluster is private)."
  default     = "172.16.0.0/28"
}
