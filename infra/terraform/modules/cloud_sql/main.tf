# Cloud SQL for PostgreSQL — private IP only, reached through the Cloud SQL
# Proxy (sidecar in-cluster, compose service on the VM). One instance backs the
# app schema, the Feast SQL registry, the MLflow backend store and the Airflow
# metadata DB (one database each).

resource "google_sql_database_instance" "this" {
  name                = var.instance_name
  project             = var.project_id
  region              = var.region
  database_version    = var.database_version
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.tier
    edition           = var.edition
    availability_type = var.availability_type
    disk_size         = var.disk_size_gb
    disk_autoresize   = true

    ip_configuration {
      ipv4_enabled    = var.ipv4_enabled
      private_network = var.private_network
      ssl_mode        = var.ssl_mode
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "17:00" # 00:00 Asia/Ho_Chi_Minh
    }
  }
  # Ordering vs the PSA peering range (the private IP needs it) is enforced by
  # the caller with `depends_on = [module.network]` on this module block.
}

resource "google_sql_database" "databases" {
  for_each = toset(var.databases)

  name     = each.value
  project  = var.project_id
  instance = google_sql_database_instance.this.name
}

resource "google_sql_user" "users" {
  # Usernames (map keys) become resource instance addresses, so they must not be
  # sensitive. The caller marks sql_users sensitive to protect the passwords;
  # unwrap here for for_each. The password itself stays redacted because the
  # google_sql_user.password attribute is sensitive in the provider schema.
  for_each = nonsensitive(var.users)

  name     = each.key
  project  = var.project_id
  instance = google_sql_database_instance.this.name
  password = each.value.password
}
