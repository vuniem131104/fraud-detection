# GCS buckets. The model bucket (fraud-detection-modelss) holds MLflow artifacts
# (gs://.../mlflow-artifacts, read by KServe's storage-initializer) and the
# DVC-tracked training data.

resource "google_storage_bucket" "this" {
  for_each = var.buckets

  name                        = each.key
  project                     = var.project_id
  location                    = var.location
  force_destroy               = each.value.force_destroy
  uniform_bucket_level_access = each.value.uniform_bucket_level_access

  versioning {
    enabled = each.value.versioning
  }

  dynamic "lifecycle_rule" {
    for_each = each.value.lifecycle_age_days > 0 ? [1] : []
    content {
      action {
        type = "Delete"
      }
      condition {
        age = each.value.lifecycle_age_days
      }
    }
  }
}
