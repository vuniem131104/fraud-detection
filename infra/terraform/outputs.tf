output "instance_name" {
  description = "Cloud SQL instance name."
  value       = google_sql_database_instance.postgres.name
}

output "connection_name" {
  description = "Cloud SQL connection name, used by the Cloud SQL Auth Proxy."
  value       = google_sql_database_instance.postgres.connection_name
}

output "public_ip_address" {
  description = "Cloud SQL public IPv4 address, if enabled."
  value       = google_sql_database_instance.postgres.public_ip_address
}

output "postgres_user" {
  description = "Application PostgreSQL user."
  value       = google_sql_user.app.name
}

output "main_database" {
  description = "Main application database name."
  value       = google_sql_database.main.name
}

output "model_storage_bucket_name" {
  description = "Cloud Storage bucket name for model artifacts."
  value       = google_storage_bucket.model_storage.name
}

output "model_storage_bucket_url" {
  description = "Cloud Storage bucket URL for model artifacts."
  value       = google_storage_bucket.model_storage.url
}
