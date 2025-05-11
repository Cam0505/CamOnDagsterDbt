# CamOnDagsterDbt

A containerized data engineering project combining [Dagster](https://dagster.io/), [dbt](https://www.getdbt.com/), and [DLT](https://docs.dltHub.com/) to orchestrate, transform, and manage modern data workflows. It uses [DuckDB](https://duckdb.org/) as the local data warehouse backend.

## 🧱 Project Structure

```CamOnDagsterDbt/
├── cam_on_dagster_dbt/ # Dagster jobs and assets
├── dbt/ # dbt models and configs
│ ├── models/
│ ├── macros/
│ ├── dbt_project.yml
│ └── profiles.yml # Local dbt profile (excluded from git)
├── .devcontainer/ # Dev container setup
├── .github/workflows/ # GitHub Actions CI
├── docker-compose.yml
├── requirements.txt
├── workspace.yaml
├── dagster.yaml
└── README.md```

## ⚙️ Stack

- **Dagster** – Orchestration and job management
- **dbt** – SQL-based transformation layer
- **DLT** – Data ingestion from APIs to DuckDB
- **DuckDB** – Lightweight, local OLAP database
- **Docker** – Containerized local environment
- **GitHub Actions** – CI pipeline to validate dbt builds

---

This repo includes a basic GitHub Actions CI pipeline that:

Installs dbt

Runs dbt build using a temporary DuckDB path  

Fails if any models or dependencies are missing

⚠️ Note: Since the DuckDB file isn't stored in the repo, and no source tables are seeded in CI, only isolated models that don’t rely on source data will pass (Dim_Date).


🔐 Secrets
CI uses the following GitHub secret:

Secret Name	Purpose
DBT_DUCKDB_PATH	Path to DuckDB file in CI workflow


📝 Notes
The .duckdb database file is ignored from git (.gitignore)

profiles.yml is stored inside dbt/ and not committed for security

This repo is meant for local development and experimentation