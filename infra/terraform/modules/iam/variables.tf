variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "workload_pool" {
  type        = string
  description = "Workload Identity pool, i.e. \"<project_id>.svc.id.goog\"."
}

variable "service_accounts" {
  description = <<-EOT
    Service accounts to create. For each:
      - project_roles: roles bound at the project level to this SA.
      - workload_identity_members: K8s SAs allowed to impersonate this GSA,
        each written as "<namespace>/<ksa_name>". The module expands them to
        serviceAccount:<pool>[<namespace>/<ksa_name>] on roles/iam.workloadIdentityUser.
  EOT
  type = map(object({
    display_name               = string
    project_roles              = optional(list(string), [])
    workload_identity_members  = optional(list(string), [])
  }))
}
