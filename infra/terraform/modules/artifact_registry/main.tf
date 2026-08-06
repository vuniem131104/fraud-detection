# Docker repository for the app images. Full path used by the charts:
#   <location>-docker.pkg.dev/<project>/<repository_id>/<image>
# e.g. us-central1-docker.pkg.dev/<project>/fraud-detection/fraud-detection

resource "google_artifact_registry_repository" "this" {
  project       = var.project_id
  location      = var.location
  repository_id = var.repository_id
  description   = var.description
  format        = var.format
}
