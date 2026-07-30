variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "secrets" {
  description = <<-EOT
    Secrets to create, keyed by secret ID. Values are intentionally NOT managed
    here — they are seeded out-of-band (see docs/Centralize Secret Management.md)
    and rotated with `gcloud secrets versions add`, so nothing sensitive lands in
    state or git. Set `create_initial_version` only for a brand-new project where
    you want Terraform to write a first version from `initial_value`.
  EOT
  type = map(object({
    create_initial_version = optional(bool, false)
    initial_value          = optional(string, "")
  }))
}

variable "accessor_members" {
  description = "IAM members granted roles/secretmanager.secretAccessor on every secret (e.g. the ESO and VM service accounts)."
  type        = list(string)
  default     = []
}
