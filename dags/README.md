# DHAP-34 — Dataset Catalog Pipeline

## Overview
This project ingests the dataset catalog CSV, validates it against a schema, cleans it, and loads it into a PostgreSQL table using an Airflow DAG. Everything runs locally via Docker Compose.

## Prerequisites
- Docker & Docker Compose installed
- Git access to the repository

## How to Run

1. Clone this repository and navigate to this folder.
2. Copy `.env.example` to `.env` and fill in real values:
cp .env.example .env
3. Start the environment:
docker compose up
4. Open the Airflow UI at [http://localhost:8080](http://localhost:8080)
   - Username: `admin`
   - Password: `admin`
5. Find the DAG named `load_dataset_catalog_to_postgres` and trigger it manually.
6. Verify the data loaded into PostgreSQL by connecting to the `dataset_catalog` database and running:
```sql
   SELECT * FROM dataset_catalog LIMIT 10;
```

## Troubleshooting

- **Port 8080 already in use**: Stop any other service using that port, or change the port mapping in `docker-compose.yaml`.
- **DAG not showing up in UI**: Make sure the `dags/` folder is correctly mounted in `docker-compose.yaml`, and check for Python syntax errors in the DAG file.
- **Database connection errors**: Verify the values in `.env` match what's expected, and that the `postgres` service is running (`docker ps`).