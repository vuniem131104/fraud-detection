output "gke_cluster_name" {
  value       = module.gke.cluster_name
  description = "GKE cluster name (for `gcloud container clusters get-credentials`)."
}

output "workload_pool" {
  value       = module.gke.workload_pool
  description = "Workload Identity pool."
}

output "cloud_sql_connection_name" {
  value       = module.cloud_sql.connection_name
  description = "CLOUDSQL_CONNECTION_NAME for the Cloud SQL Proxy (charts + VM .env)."
}

output "cloud_sql_private_ip" {
  value       = module.cloud_sql.private_ip_address
  description = "Cloud SQL private IP."
}

output "redis_host" {
  value       = module.memorystore.host
  description = "Memorystore host — set as REDIS_HOST in the configmap and the VM .env."
}

output "redis_port" {
  value       = module.memorystore.port
  description = "Memorystore port."
}

output "buckets" {
  value       = module.gcs.bucket_names
  description = "All GCS buckets (models, dvc, loki chunks, loki ruler)."
}

output "artifact_registry_url" {
  value       = module.artifact_registry.repository_url
  description = "Base image path for the app images."
}

output "service_account_emails" {
  value       = module.iam.emails
  description = "All platform service account emails."
}

output "vm_external_ip" {
  value       = module.compute_vm.external_ip
  description = "Airflow/MLflow VM external IP (if assigned) — put in the Ansible inventory."
}

output "vm_internal_ip" {
  value       = module.compute_vm.internal_ip
  description = "Airflow/MLflow VM internal IP."
}
