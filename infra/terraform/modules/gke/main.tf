# Regional, VPC-native GKE cluster with Workload Identity.
#
# Workload Identity is the backbone of this platform: every in-cluster service
# (fraud API, prediction-writer, drift API, KServe, External Secrets Operator)
# impersonates a GCP service account through the `PROJECT.svc.id.goog` pool
# instead of mounting a key file. The default node pool is removed so all nodes
# come from the managed pools declared below.

resource "google_container_cluster" "this" {
  name     = var.cluster_name
  project  = var.project_id
  location = var.location

  network    = var.network
  subnetwork = var.subnetwork

  # Manage node pools separately.
  remove_default_node_pool = true
  initial_node_count       = 1

  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    # Empty => GKE creates/manages the range itself (no named secondary on the
    # subnet). Set a name to bind to a secondary range you manage yourself.
    cluster_secondary_range_name  = var.pods_range_name != "" ? var.pods_range_name : null
    services_secondary_range_name = var.services_range_name != "" ? var.services_range_name : null
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  release_channel {
    channel = var.release_channel
  }

  deletion_protection = var.deletion_protection

  # Node pools own the lifecycle of node config; ignore drift on the throwaway
  # bootstrap pool that remove_default_node_pool deletes on first apply.
  lifecycle {
    ignore_changes = [initial_node_count]
  }
}

resource "google_container_node_pool" "pools" {
  for_each = var.node_pools

  name     = each.key
  project  = var.project_id
  location = var.location
  cluster  = google_container_cluster.this.name

  # Fixed-size pool when autoscaling is off; otherwise autoscale between bounds.
  node_count = each.value.autoscaling ? null : each.value.node_count

  dynamic "autoscaling" {
    for_each = each.value.autoscaling ? [1] : []
    content {
      min_node_count = each.value.min_node_count
      max_node_count = each.value.max_node_count
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = each.value.machine_type
    disk_size_gb = each.value.disk_size_gb
    disk_type    = each.value.disk_type
    spot         = each.value.spot
    labels       = each.value.labels

    service_account = var.node_service_account != "" ? var.node_service_account : null
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    # Required for Workload Identity on the node.
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  lifecycle {
    ignore_changes = [node_config[0].labels]
  }
}
