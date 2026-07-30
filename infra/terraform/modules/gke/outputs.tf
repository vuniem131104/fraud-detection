output "cluster_name" {
  value       = google_container_cluster.this.name
  description = "GKE cluster name."
}

output "cluster_id" {
  value       = google_container_cluster.this.id
  description = "GKE cluster ID."
}

output "endpoint" {
  value       = google_container_cluster.this.endpoint
  description = "Cluster control-plane endpoint."
  sensitive   = true
}

output "workload_pool" {
  value       = "${var.project_id}.svc.id.goog"
  description = "Workload Identity pool used for KSA -> GSA bindings."
}

output "ca_certificate" {
  value       = google_container_cluster.this.master_auth[0].cluster_ca_certificate
  description = "Base64 cluster CA cert."
  sensitive   = true
}
