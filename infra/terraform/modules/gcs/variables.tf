variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "location" {
  type        = string
  description = "Bucket location (region or multi-region)."
}

variable "buckets" {
  description = "Buckets to create, keyed by bucket name."
  type = map(object({
    versioning                  = optional(bool, true)
    force_destroy               = optional(bool, false)
    uniform_bucket_level_access = optional(bool, true)
    lifecycle_age_days          = optional(number, 0) # 0 = no age-based deletion
  }))
}
