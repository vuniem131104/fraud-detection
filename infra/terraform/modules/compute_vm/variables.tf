variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "zone" {
  type        = string
  description = "Zone for the VM (e.g. us-central1-a)."
}

variable "name" {
  type        = string
  description = "Instance name."
}

variable "machine_type" {
  type        = string
  description = "Machine type."
  default     = "e2-standard-2"
}

variable "image" {
  type        = string
  description = "Boot image family or full image path."
  default     = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
}

variable "boot_disk_size_gb" {
  type        = number
  description = "Boot disk size."
  default     = 50
}

variable "boot_disk_type" {
  type        = string
  description = "Boot disk type: pd-standard (HDD), pd-balanced, or pd-ssd. pd-balanced/pd-ssd count against the region's SSD_TOTAL_GB quota."
  default     = "pd-balanced"
}

variable "network" {
  type        = string
  description = "VPC self link / name."
}

variable "subnetwork" {
  type        = string
  description = "Subnet self link / name."
}

variable "service_account_email" {
  type        = string
  description = "Service account attached to the VM (cloud-sql-proxy + GCS access via ADC)."
}

variable "assign_external_ip" {
  type        = bool
  description = "Attach an ephemeral external IP. Prefer false + IAP for SSH; true is convenient for reaching the Airflow/MLflow UIs directly."
  default     = true
}

variable "network_tags" {
  type        = list(string)
  description = "Network tags used by the app-port firewall rule."
  default     = ["airflow-mlflow"]
}

variable "app_ports" {
  type        = list(string)
  description = "TCP ports to open to allowed_source_ranges (Airflow UI, MLflow)."
  default     = ["8090", "5000"]
}

variable "allowed_source_ranges" {
  type        = list(string)
  description = "Source CIDRs allowed to reach app_ports. Lock this down — default is deny-all."
  default     = []
}

variable "ssh_user" {
  type        = string
  description = "Optional OS-login/metadata SSH user (for the Ansible key). Empty = rely on project-wide keys / OS Login."
  default     = ""
}

variable "ssh_public_key" {
  type        = string
  description = "Optional SSH public key material for ssh_user."
  default     = ""
}

variable "deletion_protection" {
  type        = bool
  description = "Guard against accidental deletion."
  default     = false
}
