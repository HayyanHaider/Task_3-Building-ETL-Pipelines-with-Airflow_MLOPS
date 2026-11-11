"""Daily currency exchange rates (Frankfurter API) -> MongoDB.

Three tasks: extract -> transform -> load, passing data via named XComs.
"""
from datetime import datetime, timezone
import logging
import os

import requests
from pymongo import MongoClient, UpdateOne

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task

log = logging.getLogger(__name__)

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGETS = "USD,GBP,JPY"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rates -> MongoDB",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    # ---------- TASK 1: EXTRACT ----------
    @task(task_id="extract_currency_exchange_rates")
    def extract(ti=None):
        params = {"base": BASE_CURRENCY, "quotes": TARGETS}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        raw = response.json()
        log.info("Raw API response: %s", raw)
        ti.xcom_push(key="raw_exchange_rates", value=raw)  # named XCom

    # ---------- TASK 2: TRANSFORM ----------
    @task(task_id="transform_exchange_rate_data")
    def transform(ti=None):
        raw = ti.xcom_pull(
            task_ids="extract_currency_exchange_rates", key="raw_exchange_rates"
        )
        if not raw:
            raise ValueError("No data received from extract task")

        retrieved_at = (
            datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        )
        records = []

        if isinstance(raw, list):
            # v2: [{"date": "...", "base": "EUR", "quote": "USD", "rate": 1.15}, ...]
            for item in raw:
                records.append({
                    "base_currency": item["base"],
                    "target_currency": item["quote"],
                    "exchange_rate": float(item["rate"]),
                    "rate_date": item["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })
        else:
            # legacy: {"base": "EUR", "date": "...", "rates": {"USD": 1.15}}
            for currency, rate in raw["rates"].items():
                records.append({
                    "base_currency": raw["base"],
                    "target_currency": currency,
                    "exchange_rate": float(rate),
                    "rate_date": raw["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })

        log.info("Transformed %d records: %s", len(records), records)
        ti.xcom_push(key="transformed_exchange_rates", value=records)  # named XCom

    # ---------- TASK 3: LOAD ----------
    @task(task_id="load_data_into_mongodb")
    def load(ti=None):
        records = ti.xcom_pull(
            task_ids="transform_exchange_rate_data", key="transformed_exchange_rates"
        )
        if not records:
            raise ValueError("No transformed data to load")

        client = MongoClient(os.environ["MONGO_URI"])
        collection = client[os.environ.get("MONGO_DB", "currency_db")][
            os.environ.get("MONGO_COLLECTION", "exchange_rates")
        ]
        # Upsert: re-running for the same day does not create duplicates
        ops = [
            UpdateOne(
                {
                    "base_currency": r["base_currency"],
                    "target_currency": r["target_currency"],
                    "rate_date": r["rate_date"],
                },
                {"$set": r},
                upsert=True,
            )
            for r in records
        ]
        result = collection.bulk_write(ops)
        log.info("Upserted: %d, Modified: %d", result.upserted_count, result.modified_count)
        client.close()

    extract() >> transform() >> load()


currency_exchange_etl()
