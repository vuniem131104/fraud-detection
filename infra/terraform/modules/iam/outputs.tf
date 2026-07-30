output "emails" {
  value       = { for k, sa in google_service_account.this : k => sa.email }
  description = "Map of service account key -> email."
}

output "members" {
  value       = { for k, sa in google_service_account.this : k => "serviceAccount:${sa.email}" }
  description = "Map of service account key -> IAM member string."
}
