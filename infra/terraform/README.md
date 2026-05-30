# Fraud Detection Terraform

This Terraform creates the Cloud SQL resources from the equivalent `gcloud sql`
commands:

- PostgreSQL Cloud SQL instance
- application PostgreSQL user
- main application database
- Cloud Storage bucket for model storage

## Usage

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your `project_id`, `sql_instance_name`,
`postgres_user`, `main_db`, and `model_storage_bucket_name`. Do not commit the
real `postgres_password`.

Run:

```bash
terraform init
terraform plan -var='postgres_password=YOUR_PASSWORD'
terraform apply -var='postgres_password=YOUR_PASSWORD'
```

Useful outputs:

```bash
terraform output connection_name
terraform output public_ip_address
terraform output model_storage_bucket_url
```

For local development, prefer the Cloud SQL Auth Proxy:

```bash
cloud-sql-proxy "$(terraform output -raw connection_name)" --port 5433
```

Then point the app at:

```bash
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5433
POSTGRES_USER=<postgres_user>
POSTGRES_PASSWORD=<postgres_password>
POSTGRES_DB=<main_db>
```
