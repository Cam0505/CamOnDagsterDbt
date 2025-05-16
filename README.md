# CamOnDagsterDbt

A containerized data engineering project combining [Dagster](https://dagster.io/), [dbt](https://www.getdbt.com/), and [DLT](https://docs.dltHub.com/) to orchestrate, transform, and manage modern data workflows. It uses [DuckDB](https://duckdb.org/) as the local data warehouse backend with plans to migrate to GCP [BigQuery](https://cloud.google.com/bigquery?hl=en) via [Terraform](https://developer.hashicorp.com/terraform) in the future.

## 🧱 Project Structure

<details>

<summary><strong>📁 (click to expand)</strong></summary>

```text
CamOnDagsterDbt/
├── cam_on_dagster_dbt/           # Dagster jobs, assets, schedules, sensors, and definitions
│   ├── assets/                   # All asset definitions grouped by data source
│   ├── jobs/                     # Dagster job definitions
│   ├── schedules.py              # Dagster schedules
│   ├── sensors.py                # Dagster sensors
│   ├── definitions.py            # Central Dagster Definitions object
│   └── __init__.py               # Package initializer
├── dbt/                          # dbt models and configs
│   ├── models/                   # dbt models
│   ├── macros/                   # Custom macros
│   ├── dbt_project.yml           # dbt project configuration
│   └── profiles.yml              # dbt profile (excluded from git)
├── terraform/                    # Infrastructure-as-code for GCP (planned)
│   ├── main.tf                   # GCP resource definitions
│   ├── outputs.tf                # Outputs from Terraform resources
│   ├── variables.tf              # Input variables
│   └── terraform.tfvars          # Environment-specific values
├── .devcontainer/                # Dev container setup
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── devcontainer.json
├── .github/workflows/            # GitHub Actions CI workflows
│   └── ci.yml
├── docker-compose.yml            # Main Docker Compose file
├── requirements.txt              # Python dependencies
├── workspace.yaml                # Dagster workspace configuration
├── dagster.yaml                  # Dagster project configuration
└── README.md                     # Project documentation
```

</details>


### ⚙️ Stack

- 🔄 Dagster for workflow orchestration and asset management

- 📊 dbt for data modeling and transformations

- ⚙️ Data Load Tool (DLT) for incremental pipeline automation

- 🦆 DuckDB for local analytics database with SQL support (duck stays — it’s fun!)

- ✔️ Great Expectations for data testing and validation

- 🛠️ Terraform (planned) for managing cloud infrastructure as code

- ☁️ Google Cloud Platform (planned), with BigQuery for scalable data warehousing

### Features

- Multi-source data ingestion pipelines: Google Sheets, TheCocktailDB, OpenLibrary, Rick and Morty API  
- Modular asset definitions and jobs for maintainability  
- Configured schedules and sensors to automate pipeline runs  
- Centralized definitions to easily manage asset and job dependencies  
- Integration of Great Expectations for data quality validation  
- Environment configurations prepared for local and containerized execution  

### Terraform and Cloud Migration Preparation

While the current project runs primarily on DuckDB and local orchestration tools, the codebase and infrastructure are designed with future cloud migration in mind. The project structure already includes:

- Terraform configuration files (planned or partially implemented) to manage infrastructure as code for Google Cloud Platform (GCP).  
- Access controls and service account management set up for seamless authentication with GCP services.  
- BigQuery dataset preparation, with the dataset schema and permissions configured and ready for integration.  

This preparation ensures a smooth transition from local DuckDB to a scalable, cloud-native data warehouse using BigQuery, enabling advanced analytics and enterprise-grade data operations. When fully implemented, Terraform will provision and manage GCP resources automatically, aligning infrastructure deployment with the project’s version-controlled codebase.

### Next Steps

- Finalize Terraform scripts for automated GCP resource deployment  
- Transition pipelines to run on BigQuery for scalable cloud analytics  
- Enhance data quality checks with Great Expectations integration  
- Expand pipeline coverage with additional APIs and datasets

