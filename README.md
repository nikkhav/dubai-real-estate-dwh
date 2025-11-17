# Dubai Real Estate Data Warehouse

## Overview

This project is a **data engineering pet project** that builds an end-to-end pipeline for real estate transaction data in Dubai. The Department of Land in Dubai provides an open API with daily data on property sales and rentals. The goal is to **automate data ingestion, transformation, and analytics** to enable insights into the real estate market and experiment with machine learning models.

## Architecture

The project uses a modern **data platform stack**:

* **PostgreSQL** — central database, structured into three layers:
   * **stg** (staging): raw JSON payloads, stored as-is.
   * **dds** (data mart): cleaned and transformed tables.
   * **cdm** (consumer data mart): curated analytics-ready views.

* **Airflow** — orchestrates daily ingestion of new data from the API and schema initialization.
* **dbt** — manages SQL transformations from staging to DDS and CDM.
* **Metabase** — BI and visualization layer with interactive dashboards.
* **Docker Compose** — runs the entire environment (Postgres, Airflow, dbt, Metabase) locally.

## Data Flow

1. **Ingestion**: Airflow fetches transactions daily from the Dubai API and stores them in the stg.raw_deals table.
2. **Transformation**: dbt transforms staging data into DDS and CDM layers (cleans fields, builds dimensions and fact tables).
3. **Analytics**: Metabase connects to CDM schemas to visualize trends (e.g., sales volume, top areas, property types).
4. **ML (planned)**: prototype a regression model to predict property prices based on location, size, and type.

## Repository Structure

```
.
├── dags/                 # Airflow DAGs (ingestion, schema init)
│   ├── stg/              # Staging layer DAGs
│   ├── dds/              # Data mart layer DAGs
│   ├── cdm/              # Consumer data mart layer DAGs
│   └── lib/              # Shared utilities
├── dbt/                  # dbt project (models, profiles.yml)
├── docker-compose.yml    # Infrastructure definition
├── README.md             # Project description
└── .gitignore            # Ignore dbt logs, Airflow logs, compiled artifacts
```

## Getting Started

1. Clone the repository.
2. Start the environment:

```bash
docker compose build airflow
```

```bash
docker-compose up -d
```

3. Access services:
   * Airflow UI → http://localhost:8080
     * Login: `admin`
     * Password: get it by running:
       ```bash
       docker exec -it airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
       ```
   * Metabase → http://localhost:3000
   * Postgres → localhost:15432 (user: jovyan, password: jovyan, db: dwh)

## Roadmap

* ✅ Staging schema (stg.raw_deals) with raw payloads.
* 🚧 dbt transformations for DDS and CDM layers.
* 🚧 Build dashboards in Metabase (sales volume, price per m², top projects).
* 🚧 ML prototype for price prediction.

## Motivation

The real estate market in Dubai is one of the most dynamic in the world. With open data available, this project demonstrates how to **build a modern analytics pipeline from scratch** — useful for both learning and practical exploration of data engineering.