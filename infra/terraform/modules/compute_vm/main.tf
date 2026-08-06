# Compute Engine VM that runs the Airflow + MLflow + Cloud SQL Proxy docker
# compose stack (see airflow/docker-compose.yml). Ansible configures and deploys
# onto it. The attached service account gives cloud-sql-proxy and MLflow their
# GCS/Cloud SQL access through ADC — no key files on the box.

resource "google_compute_instance" "this" {
  name         = var.name
  project      = var.project_id
  zone         = var.zone
  machine_type = var.machine_type

  tags = var.network_tags

  boot_disk {
    initialize_params {
      image = var.image
      size  = var.boot_disk_size_gb
      type  = var.boot_disk_type
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork

    dynamic "access_config" {
      for_each = var.assign_external_ip ? [1] : []
      content {} # ephemeral external IP
    }
  }

  service_account {
    email  = var.service_account_email
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  metadata = merge(
    { enable-oslogin = "TRUE" },
    var.ssh_user != "" && var.ssh_public_key != "" ? {
      "ssh-keys" = "${var.ssh_user}:${var.ssh_public_key}"
    } : {}
  )

  # OS Login is enabled above; the ssh-keys fallback is only used when ssh_user
  # is set. Ansible reaches the box over IAP or the external IP.
  allow_stopping_for_update = true
  deletion_protection       = var.deletion_protection

  lifecycle {
    ignore_changes = [boot_disk[0].initialize_params[0].image]
  }
}

# App-port firewall. Only created when at least one source range is provided —
# otherwise the ports stay closed (SSH still flows via the VPC IAP rule).
resource "google_compute_firewall" "app_ports" {
  count = length(var.allowed_source_ranges) > 0 ? 1 : 0

  name    = "${var.name}-allow-app-ports"
  project = var.project_id
  network = var.network

  allow {
    protocol = "tcp"
    ports    = var.app_ports
  }

  source_ranges = var.allowed_source_ranges
  target_tags   = var.network_tags
}
