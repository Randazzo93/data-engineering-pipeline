# NPI Data Engineering Pipeline

## Overview
This repository contains a data engineering pipeline for processing and analyzing data from the NPI (National Provider Identifier) Registry. The pipeline is structured into multiple layers (Bronze, Silver, and Gold) to enable efficient data transformation and storage.

## Repository Structure

```plaintext
DATA-ENGINEERING-PIPELINE
├── data_pipeline/
│   ├── 0_bronze/
│   │   ├── nppes_extract_json_columns.csv  # Raw extracted data
│   ├── 1_silver/
│   │   ├── nppes_parsed.sql  # SQL scripts for data parsing and transformation
│   ├── 2_gold/
│   │   ├── dimensions/
│   │   │   ├── city_dimension.sql  # SQL script for city dimension table
│   │   │   ├── state_dimension_flattened.csv  # Flattened state dimension data
│   │   │   ├── state_dimension.sql  # SQL script for state dimension table
│   │   │   ├── taxonomy_code_dimension.sql  # SQL script for taxonomy codes
│   │   ├── facts/
│   │   │   ├── state_npi_facts.csv  # Processed NPI facts data
│   │   │   ├── state_npi_facts.sql  # SQL script for fact table
│   │   ├── status/
│   │   │   ├── insert_state_npi_daily_status.sql  # SQL script for daily status
│   │   │   ├── state_npi_daily_status.csv  # Processed daily status data
├── dimensionality.png  # Visualization of dimensional model
├── discovery.ipynb  # Jupyter notebook for data exploration
├── ERD.png  # Entity Relationship Diagram (ERD)
├── npi_extract.ipynb  # Jupyter notebook for data extraction
├── state_city.yml  # YAML configuration for state-city mappings
├── tableau_days_since_last_etl_run_aging_bins.png  # Tableau visualization
├── tableau_npi_lead_list_engagement_chart.png  # Tableau visualization
├── tableau_npi_lead_list_engagement_trends.png  # Tableau visualization
├── tableau_npi_lead_list_engagement_summary.png  # Tableau visualization
├── .gitignore  # Ignore unnecessary files
├── poetry.lock  # Dependencies lock file
├── pyproject.toml  # Python project configuration
├── README.md  # Documentation
```

## Data Pipeline Stages

### Bronze Layer
- Raw data is extracted from the NPI Registry in JSON format.
- Stored in CSV format (`nppes_extract_json_columns.csv`).

### Silver Layer
- Data is parsed and cleaned using SQL transformations (`nppes_parsed.sql`).

### Gold Layer
- Data is further transformed into dimensional and fact tables.
- **Dimension tables:**
  - City, State, and Taxonomy Code (`city_dimension.sql`, `state_dimension.sql`, etc.).
- **Fact tables:**
  - `state_npi_facts.sql`: Contains aggregated NPI data.
  - `state_npi_daily_status.csv`: Tracks daily updates to NPI data.

## Visualization & Analysis
- Jupyter notebooks (`discovery.ipynb`, `npi_extract.ipynb`) for exploratory data analysis.
- Tableau visualizations (`tableau_npi_lead_list_engagement_chart.png`, etc.).
- ERD (`ERD.png`) showing database schema relationships.

## Dependencies
- **Python** (configured via `pyproject.toml` and `poetry.lock`)
- **SQL** for data transformation
- **Jupyter Notebooks** for data exploration
- **Tableau** for visualization

## Getting Started

```sh
# Clone the repository
git clone <repository-url>
cd DATA-ENGINEERING-PIPELINE

# Install dependencies
poetry install

# Run data extraction
jupyter notebook npi_extract.ipynb
```

4. Process data through SQL transformations.
5. Load the final dataset into Tableau for visualization.

## Contributors
- Thomas Randazzo

