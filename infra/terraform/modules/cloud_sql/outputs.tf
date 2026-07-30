output "instance_name" {
  value       = google_sql_database_instance.this.name
  description = "Cloud SQL instance name."
}

output "connection_name" {
  value       = google_sql_database_instance.this.connection_name
  description = "CLOUDSQL_CONNECTION_NAME (project:region:instance) used by the proxy."
}

output "private_ip_address" {
  value       = google_sql_database_instance.this.private_ip_address
  description = "Private IP of the instance."
}

output "self_link" {
  value       = google_sql_database_instance.this.self_link
  description = "Instance self link."
}
