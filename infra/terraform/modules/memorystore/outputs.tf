output "host" {
  value       = google_redis_instance.this.host
  description = "Redis host IP — set as REDIS_HOST in the app configmap and the VM .env."
}

output "port" {
  value       = google_redis_instance.this.port
  description = "Redis port."
}

output "id" {
  value       = google_redis_instance.this.id
  description = "Instance ID."
}
