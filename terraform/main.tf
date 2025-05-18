terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_bigquery_dataset" "dagster_data" {
  dataset_id                 = "dagster_data"
  location                   = "australia-southeast1"
  friendly_name              = "Dagster Dataset"
  description                = "Dataset for Dagster pipelines"
  delete_contents_on_destroy = true
}


resource "google_service_account" "dagster_sa" {
  account_id   = "dagster-service-account"
  display_name = "Service Account for Dagster BigQuery access"
}

resource "google_project_iam_member" "dagster_bigquery_access" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

resource "google_project_iam_member" "dagster_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

resource "google_project_iam_member" "dagster_cloudfunctions_invoker" {
  project = var.project_id
  role    = "roles/cloudfunctions.invoker"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

resource "google_project_iam_member" "dagster_service_account_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}

resource "google_project_iam_member" "dagster_viewer" {
  project = var.project_id
  role    = "roles/iam.serviceAccountViewer"
  member  = "serviceAccount:${google_service_account.dagster_sa.email}"
}
