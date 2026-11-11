# Airflow ETL: Frankfurter API -> MongoDB (Astro CLI)

DAG `currency_exchange_etl` with 3 tasks (extract, transform, load) using named XComs
(`raw_exchange_rates`, `transformed_exchange_rates`). MongoDB + Mongo Express run as
extra Docker services in `docker-compose.override.yml` (shared `airflow` network,
`mongo_data` persistent volume).

## Run
    astro dev start

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Mongo Express | http://localhost:8081 | admin / admin123 |

Unpause and trigger `currency_exchange_etl`, then open Mongo Express -> `currency_db` -> `exchange_rates`.
