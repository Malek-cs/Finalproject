"""Pull the customer dimension from the Waseet API into the warehouse.

    python ingest_customers.py

Run this once before the first scan load, because `pipeline.py` checks
customer_id against this table.
"""

import os
import sys
import time
import pandas as pd
import requests
from sqlalchemy import create_engine, text

API_KEY = "waseet-demo-key"
DB_URL = os.environ.get(
    "WASEET_DB_URL", "postgresql+psycopg2://de:de@localhost:5442/waseet"
)

engine = create_engine(DB_URL)


def get_with_retry(url, headers, params, attempts=4):
    """One GET, with a timeout, and a doubling wait on 429 and 5xx.

    Retry transient errors (429, 5xx) and raise immediately on permanent errors (401, 404).
    """
    backoff = 0.5
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429 or response.status_code >= 500:
                if attempt == attempts:
                    response.raise_for_status()
                time.sleep(backoff)
                backoff *= 2
            else:
                # Permanent error: 401, 403, 404, etc. - fail immediately
                response.raise_for_status()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == attempts:
                raise
            time.sleep(backoff)
            backoff *= 2


def fetch_all_customers(base_url):
    """Page until has_more is False. Return a list of dicts.

    Drive the loop on what the API tells you (has_more), not on a hardcoded page count.
    """
    url = f"{base_url}/customers"
    headers = {"X-API-Key": API_KEY}
    page = 1
    all_customers = []

    while True:
        params = {"page": page}
        data = get_with_retry(url, headers=headers, params=params)
        
        # Unpack the envelope
        customers = data.get("customers", [])
        all_customers.extend(customers)

        if not data.get("has_more", False):
            break
        page += 1

    return all_customers


def load_customers(records):
    """Land them in the customers table.

    Idempotent: staging via a temporary table and upserting into customers so running
    multiple times never fails and never duplicates rows.
    """
    if not records:
        return

    df = pd.DataFrame(records)

    with engine.begin() as conn:
        # Create a staging table matching the payload
        conn.execute(
            text(
                """
                CREATE TEMP TABLE staging_customers (
                    customer_id   INTEGER,
                    customer_name TEXT,
                    segment       TEXT,
                    city          TEXT,
                    signup_date   DATE,
                    credit_limit  NUMERIC(12, 2)
                ) ON COMMIT DROP;
                """
            )
        )

        df.to_sql("staging_customers", conn, if_exists="append", index=False)

        # Idempotent upsert
        conn.execute(
            text(
                """
                INSERT INTO customers (customer_id, customer_name, segment, city, signup_date, credit_limit)
                SELECT customer_id, customer_name, segment, city, signup_date, credit_limit
                FROM staging_customers
                ON CONFLICT (customer_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    segment       = EXCLUDED.segment,
                    city          = EXCLUDED.city,
                    signup_date   = EXCLUDED.signup_date,
                    credit_limit  = EXCLUDED.credit_limit;
                """
            )
        )


if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../api")))
    from waseet_api import start_api

    base_url = start_api()
    print("API on", base_url)

    records = fetch_all_customers(base_url)
    print("fetched", len(records), "customers")

    load_customers(records)
    print("loaded")