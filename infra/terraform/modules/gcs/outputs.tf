output "bucket_names" {
  value       = [for b in google_storage_bucket.this : b.name]
  description = "Names of the created buckets."
}

output "bucket_urls" {
  value       = { for k, b in google_storage_bucket.this : k => b.url }
  description = "gs:// URLs keyed by bucket name."
}
