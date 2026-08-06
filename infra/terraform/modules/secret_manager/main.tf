# Secret Manager containers consumed by the External Secrets Operator
# (ClusterSecretStore gcp-secret-manager). Values are seeded/rotated outside
# Terraform on purpose — see docs/Centralize Secret Management.md.

resource "google_secret_manager_secret" "this" {
  for_each = var.secrets

  project   = var.project_id
  secret_id = each.key

  replication {
    auto {}
  }
}

# Optional first version — only for greenfield bootstrap. Later versions are
# added with `gcloud secrets versions add` and are ignored here so Terraform
# never overwrites a rotated secret.
resource "google_secret_manager_secret_version" "initial" {
  for_each = {
    for k, v in var.secrets : k => v if v.create_initial_version
  }

  secret      = google_secret_manager_secret.this[each.key].id
  secret_data = each.value.initial_value

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# Grant read access to the consumers (ESO, VM SA) on each secret.
resource "google_secret_manager_secret_iam_member" "accessors" {
  for_each = {
    for pair in setproduct(keys(var.secrets), var.accessor_members) :
    "${pair[0]}::${pair[1]}" => {
      secret_id = pair[0]
      member    = pair[1]
    }
  }

  project   = var.project_id
  secret_id = google_secret_manager_secret.this[each.value.secret_id].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = each.value.member
}
