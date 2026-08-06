output "network_id" {
  value       = google_compute_network.this.id
  description = "Fully-qualified VPC network ID."
}

output "network_name" {
  value       = google_compute_network.this.name
  description = "VPC network name."
}

output "network_self_link" {
  value       = google_compute_network.this.self_link
  description = "VPC network self link (for the private services connection)."
}

output "subnet_id" {
  value       = google_compute_subnetwork.this.id
  description = "Primary subnet ID."
}

output "subnet_self_link" {
  value       = google_compute_subnetwork.this.self_link
  description = "Primary subnet self link."
}

output "pods_range_name" {
  value       = var.pods_range_name
  description = "Secondary range name for GKE Pods."
}

output "services_range_name" {
  value       = var.services_range_name
  description = "Secondary range name for GKE Services."
}

output "psa_connection" {
  value       = google_service_networking_connection.psa.id
  description = "Private Service Access connection (Cloud SQL/Memorystore depend on this)."
}
