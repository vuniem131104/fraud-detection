# Memorystore for Redis — the online feature store. Both the GKE fraud API (read
# path) and the Airflow feature pipeline on the VM (Feast materialization / write
# path) hit this same instance by its VPC-internal IP, which is why it is managed
# Memorystore on the VPC rather than an in-cluster Redis.

resource "google_redis_instance" "this" {
  name           = var.name
  project        = var.project_id
  region         = var.region
  tier           = var.tier
  memory_size_gb = var.memory_size_gb
  redis_version  = var.redis_version

  authorized_network = var.authorized_network
  connect_mode       = var.connect_mode
  reserved_ip_range  = var.reserved_ip_range != "" ? var.reserved_ip_range : null
  # With PRIVATE_SERVICE_ACCESS the caller enforces ordering vs the PSA range
  # with `depends_on = [module.network]` on this module block.
}
