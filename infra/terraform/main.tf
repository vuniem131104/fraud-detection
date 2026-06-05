resource "google_project_service" "sqladmin" {
  count = var.enable_required_apis ? 1 : 0

  project            = var.project_id
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  count = var.enable_required_apis ? 1 : 0

  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

resource "google_storage_bucket" "model_storage" {
  name                        = var.model_storage_bucket_name
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  labels = {
    purpose = "model-storage"
  }

  depends_on = [google_project_service.storage]
}

resource "google_sql_database_instance" "postgres" {
  name             = var.sql_instance_name
  project          = var.project_id
  region           = var.region
  database_version = var.database_version
  root_password    = var.postgres_password

  deletion_protection = var.deletion_protection

  settings {
    edition           = var.edition
    tier              = var.tier
    disk_size         = var.storage_size_gb
    disk_type         = var.disk_type
    disk_autoresize   = var.disk_autoresize
    availability_type = var.availability_type

    backup_configuration {
      enabled                        = var.backup_enabled
      point_in_time_recovery_enabled = var.point_in_time_recovery_enabled
    }

    ip_configuration {
      ipv4_enabled = var.ipv4_enabled

      dynamic "authorized_networks" {
        for_each = var.authorized_networks

        content {
          name  = authorized_networks.value.name
          value = authorized_networks.value.value
        }
      }
    }

    maintenance_window {
      day          = var.maintenance_day
      hour         = var.maintenance_hour
      update_track = "stable"
    }
  }

  depends_on = [google_project_service.sqladmin]
}

resource "google_sql_user" "app" {
  name     = var.postgres_user
  project  = var.project_id
  instance = google_sql_database_instance.postgres.name
  password = var.postgres_password
}

resource "google_sql_database" "main" {
  name     = var.main_db
  project  = var.project_id
  instance = google_sql_database_instance.postgres.name
}

resource "google_service_account" "fraud_detection_kserve" {
  account_id   = "kserve"
  display_name = "KServe"
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "fraud_detection_kserve_model_storage_reader" {
  bucket = var.model_storage_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.fraud_detection_kserve.email}"
}

resource "google_service_account_iam_member" "fraud_detection_kserve_workload_identity" {
  service_account_id = google_service_account.fraud_detection_kserve.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[model/kserve-sa]"
}
