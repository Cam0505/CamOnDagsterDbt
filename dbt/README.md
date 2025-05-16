
# dbt for CamOnDagsterDbt

This folder contains the **dbt project** powering the transformation layer of the CamOnDagsterDbt pipeline. The project supports data from sources like OpenLibrary, TheCocktailDB, and Rick & Morty API, transformed and modeled using dbt + DLT and orchestrated by Dagster.

---

## 🔧 Project Structure

- `models/`: SQL models organized by source and domain.
- `models/source/`: Source definitions
- `models/base/`: Base Table from source systems
- `models/staging/`: Staging Table from base tables
- `models/common/`: Analytical models for consumption.
- `snapshots/`: snapshots for slowly changing dimensions.
- `macros/`: Custom Jinja macros to extend dbt functionality.
- `target/`: dbt compiled files (auto-generated, not committed).
- `dbt_project.yml`: dbt project configuration.

---

## 🚀 Key Features

- **Sources**:
  - `rick_and_morty`: Characters, episodes, and locations.
  - `openlibrary`: Book, author, and subject metadata.
  - `Beverages`: Beverages, ingredient, and glass.
  - `GSheets`: Google Sheets stock data.
  - `Meals`: Meals, ingredient, and country.
  - `geo`: City and country location metadata.
  - `uv`, `weather`: Environmental data like UV index and weather stats.

- **Transformations**:
  - Each source has a corresponding staging layer with column renames, typing, and documentation.
  - Core models expose clean, ready-for-analysis tables.
  - Metadata columns (`_dlt_id`, `_dlt_load_id`) are preserved for data lineage.

- **Tests & Documentation**:
  - Built-in dbt tests: `unique`, `not_null`, `relationships`, `accepted values`.
  - Dbt Packages: dbt_expectations, dbt_utils and audit_helper
  - Custom schema tests where needed.
  - All columns are documented using `description` or `{{ doc(...) }}` blocks.
  - Source constraints and relationships are tested.

---

## GitHub Pages Deployment
The project uses GitHub Pages to serve dbt docs from the target/ folder.

The docs are built in CI and committed to main.

Live site: https://cam0505.github.io/CamOnDagsterDbt/
Still a work in progress

