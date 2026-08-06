output "repository_id" {
  value       = google_artifact_registry_repository.this.repository_id
  description = "Repository ID."
}

output "registry_host" {
  value       = "${var.location}-docker.pkg.dev"
  description = "Docker registry host."
}

output "repository_url" {
  value       = "${var.location}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.this.repository_id}"
  description = "Base path for pushing/pulling images."
}
