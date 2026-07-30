output "name" {
  value       = google_compute_instance.this.name
  description = "Instance name."
}

output "internal_ip" {
  value       = google_compute_instance.this.network_interface[0].network_ip
  description = "Internal IP (use for the Ansible inventory when connecting over IAP/VPC)."
}

output "external_ip" {
  value       = var.assign_external_ip ? google_compute_instance.this.network_interface[0].access_config[0].nat_ip : null
  description = "External IP if assigned, else null."
}

output "self_link" {
  value       = google_compute_instance.this.self_link
  description = "Instance self link."
}
