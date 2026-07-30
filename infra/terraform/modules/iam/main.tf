# Service accounts + project role bindings + Workload Identity bindings.
#
# Each in-cluster workload runs as a K8s SA that impersonates one of these GCP
# SAs via Workload Identity (no key files). The Helm charts add the matching
# `iam.gke.io/gcp-service-account` annotation on the KSA side; this module owns
# the GCP side: the SA, its project roles, and the workloadIdentityUser binding.

locals {
  # Flatten {sa => [roles]} into one binding per (sa, role) pair.
  sa_role_pairs = merge([
    for sa_key, sa in var.service_accounts : {
      for role in sa.project_roles :
      "${sa_key}::${role}" => { sa = sa_key, role = role }
    }
  ]...)

  # Flatten {sa => ["ns/ksa"]} into one binding per (sa, ksa) pair.
  wi_pairs = merge([
    for sa_key, sa in var.service_accounts : {
      for ksa in sa.workload_identity_members :
      "${sa_key}::${ksa}" => { sa = sa_key, ksa = ksa }
    }
  ]...)
}

resource "google_service_account" "this" {
  for_each = var.service_accounts

  project      = var.project_id
  account_id   = each.key
  display_name = each.value.display_name
}

resource "google_project_iam_member" "roles" {
  for_each = local.sa_role_pairs

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.this[each.value.sa].email}"
}

resource "google_service_account_iam_member" "workload_identity" {
  for_each = local.wi_pairs

  service_account_id = google_service_account.this[each.value.sa].name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.workload_pool}[${each.value.ksa}]"
}
