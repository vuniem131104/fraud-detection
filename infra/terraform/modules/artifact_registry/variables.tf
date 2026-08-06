variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "location" {
  type        = string
  description = "Artifact Registry location (e.g. us-central1)."
}

variable "repository_id" {
  type        = string
  description = "Repository ID (the segment after the location in the image path)."
}

variable "description" {
  type        = string
  description = "Human-readable repository description."
  default     = "Docker images for the fraud-detection platform."
}

variable "format" {
  type        = string
  description = "Repository format."
  default     = "DOCKER"
}
