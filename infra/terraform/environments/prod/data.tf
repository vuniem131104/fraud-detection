# A fresh GCP project ships with an auto-mode `default` VPC (subnet per region,
# internet egress, basic firewall). We build ON it rather than creating our own:
# the cluster, Cloud SQL private IP, Memorystore and the VM all attach to
# `default`. It is referenced (data source), never managed — modules/network is
# kept in the repo only for a dedicated-VPC build.
data "google_compute_network" "default" {
  name    = "default"
  project = var.project_id
}

data "google_compute_subnetwork" "default" {
  name    = "default"
  project = var.project_id
  region  = var.region
}
