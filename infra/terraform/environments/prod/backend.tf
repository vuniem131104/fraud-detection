# Remote state in GCS. Create the state bucket ONCE, out of band, before the
# first `terraform init` (it cannot be managed by the state it would hold):
#
#   gcloud storage buckets create gs://fraud-detection-tfstatess \
#     --project project-57f7ef9a-6059-4068-ae7 --location us-central1 \
#     --uniform-bucket-level-access
#   gcloud storage buckets update gs://fraud-detection-tfstatess --versioning
#
# Then set the bucket via `-backend-config` at init time so it is not hardcoded:
#   terraform init -backend-config="bucket=fraud-detection-tfstatess"
terraform {
  backend "gcs" {
    prefix = "fraud-detection/prod"
    # bucket supplied via: terraform init -backend-config="bucket=fraud-detection-tfstatess"
  }
}
