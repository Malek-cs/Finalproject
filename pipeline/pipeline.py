"""Waseet Logistics daily scan pipeline.

    python pipeline.py --date 2026-05-04

The shape is given so that the DAG in ../dags has something with a known
interface to call. The bodies are yours.

Nothing here is a puzzle. Every function below has a worked equivalent in
Lecture 13's `pipeline.py`; the difference is the data, and the data is the
point. Read a scan file before you write a line of this.
"""

import argparse
import logging
import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

DEFAULT_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
DATA_DIR = os.environ.get("WASEET_DATA_DIR", DEFAULT_DATA_DIR)
DB_URL = os.environ.get(
    "WASEET_DB_URL", "postgresql+psycopg2://de:de@localhost:5442/waseet"
)

# Contract: the 9 expected columns in every incoming daily scan file
REQUIRED = [
    "scan_id",
    "parcel_id",
    "customer_id",
    "scanned_at",
    "hub_id",
    "courier_id",
    "scan_type",
    "weight_kg",
    "service_code",
]

os.makedirs("logs", exist_ok=True)
os.makedirs("quarantine", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/pipeline.log"), logging.StreamHandler()],
)
log = logging.getLogger("waseet")

engine = create_engine(DB_URL)


def extract(scan_date):
    """Read the file for one day, strictly as strings."""
    filename = f"scans_{scan_date}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Missing file for date {scan_date}: {filepath}")

    log.info("extracting %s", filepath)
    return pd.read_csv(filepath, dtype=str)


def check_contract(df):
    """Fail the run if the file is not the shape we agreed."""
    missing = [col for col in REQUIRED if col not in df.columns]
    if missing:
        raise ValueError(
            f"Contract violation: missing required column(s) {missing}. Schema drift detected."
        )
    log.info("contract check passed: all %d required columns present", len(REQUIRED))


def parse_timestamps(series):
    """Two formats arrive. Return one datetime column.

    Roughly 3% use DD/MM/YYYY HH:MM instead of ISO format YYYY-MM-DD HH:MM:SS.
    """
    fmt1 = pd.to_datetime(series, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    fmt2 = pd.to_datetime(series, format="%d/%m/%Y %H:%M", errors="coerce")
    return fmt1.fillna(fmt2)


def transform(raw, scan_date, hubs, couriers, customers, services):
    """Clean, repair, reject with reasons, and deduplicate."""
    if len(raw) == 0:
        return pd.DataFrame(), pd.DataFrame(), 0

    df = raw.copy()

    # 1. Deduplicate identical scan events (same event sent twice)
    dup_mask = df.duplicated(subset=["scan_id"], keep="first")
    rows_deduplicated = int(dup_mask.sum())
    working = df[~dup_mask].copy()

    # Prepare lookup sets for fast O(1) membership checks
    valid_hubs = set(hubs["hub_id"].astype(str).str.strip())
    valid_couriers = set(couriers["courier_id"].astype(str).str.strip())
    valid_customers = set(customers["customer_id"].astype(str).str.strip())
    valid_services = set(services["service_code"].astype(str).str.strip())

    # 2. Repair scan_type (mixed-case to uppercase)
    working["scan_type"] = working["scan_type"].astype(str).str.strip().str.upper()

    # 3. Repair & parse timestamps
    parsed_timestamps = parse_timestamps(working["scanned_at"])

    # 4. Repair & validate weight_kg
    raw_weights = (
        working["weight_kg"].fillna("").astype(str).str.strip().str.replace(",", ".", regex=False)
    )
    numeric_weights = pd.to_numeric(raw_weights.replace("", np.nan), errors="coerce")

    # Evaluate validation rules and assign reject reasons
    reasons = []
    for row, ts, wt, raw_wt in zip(
        working.itertuples(index=False), parsed_timestamps, numeric_weights, raw_weights
    ):
        reason = None

        # Check unparseable timestamp
        if pd.isna(ts):
            reason = f"Unparseable scanned_at: '{row.scanned_at}'"

        # Check customer foreign key
        cust_id = str(row.customer_id).strip() if pd.notna(row.customer_id) else ""
        if not reason and (not cust_id or cust_id not in valid_customers):
            reason = f"Invalid customer_id: '{cust_id}' not in warehouse"

        # Check hub foreign key (e.g. hub_id 99)
        hub_id = str(row.hub_id).strip() if pd.notna(row.hub_id) else ""
        if not reason and (not hub_id or hub_id not in valid_hubs):
            reason = f"Invalid hub_id: '{hub_id}' not in hubs dimension"

        # Check courier (blank courier_id is valid unassigned pickup, non-blank must exist in couriers)
        cour_id = str(row.courier_id).strip() if pd.notna(row.courier_id) else ""
        if not reason and cour_id and cour_id not in valid_couriers:
            reason = f"Invalid courier_id: '{cour_id}' not in couriers dimension"

        # Check service level
        srv_code = str(row.service_code).strip() if pd.notna(row.service_code) else ""
        if not reason and srv_code and srv_code not in valid_services:
            reason = f"Invalid service_code: '{srv_code}' not in service_levels"

        # Check impossible weights (<= 0 or > 100 kg) or non-numeric if present
        if not reason and raw_wt != "":
            if pd.isna(wt):
                reason = f"Non-numeric weight_kg: '{row.weight_kg}'"
            elif wt <= 0 or wt > 100:
                reason = f"Impossible weight_kg: {wt}"

        reasons.append(reason)

    working["reject_reason"] = reasons
    working["scanned_at_clean"] = parsed_timestamps
    working["weight_kg_clean"] = numeric_weights

    rejects_mask = working["reject_reason"].notna()
    good = working[~rejects_mask].copy()
    rejects = working[rejects_mask].copy()

    # Save quarantined records with reason
    if len(rejects) > 0:
        quarantine_path = f"quarantine/rejects_{scan_date}.csv"
        rejects.to_csv(quarantine_path, index=False)
        log.warning("quarantined %d rows to %s", len(rejects), quarantine_path)

    return good, rejects, rows_deduplicated


def load(good, scan_date):
    """Write one day idempotently using strict half-open date boundaries."""
    rows_loaded = len(good)
    next_day = (pd.to_datetime(scan_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    with engine.begin() as conn:
        # Idempotency: remove previous runs for this day window
        conn.execute(
            text(
                "DELETE FROM parcel_scans WHERE scanned_at >= :start_ts AND scanned_at < :next_day_ts"
            ),
            {"start_ts": f"{scan_date} 00:00:00", "next_day_ts": f"{next_day} 00:00:00"},
        )

        if rows_loaded > 0:
            records = []
            for r in good.itertuples(index=False):
                cour_id = (
                    int(r.courier_id)
                    if pd.notna(r.courier_id) and str(r.courier_id).strip() != ""
                    else None
                )
                srv_code = (
                    str(r.service_code).strip()
                    if pd.notna(r.service_code) and str(r.service_code).strip() != ""
                    else None
                )
                wt_val = float(r.weight_kg_clean) if pd.notna(r.weight_kg_clean) else None

                records.append(
                    {
                        "scan_id": r.scan_id,
                        "parcel_id": r.parcel_id,
                        "customer_id": int(r.customer_id),
                        "scanned_at": r.scanned_at_clean,
                        "hub_id": int(r.hub_id),
                        "courier_id": cour_id,
                        "scan_type": r.scan_type,
                        "weight_kg": wt_val,
                        "service_code": srv_code,
                    }
                )

            conn.execute(
                text(
                    """
                    INSERT INTO parcel_scans (
                        scan_id, parcel_id, customer_id, scanned_at,
                        hub_id, courier_id, scan_type, weight_kg, service_code
                    ) VALUES (
                        :scan_id, :parcel_id, :customer_id, :scanned_at,
                        :hub_id, :courier_id, :scan_type, :weight_kg, :service_code
                    )
                    """
                ),
                records,
            )

    log.info("loaded %d rows into parcel_scans for %s", rows_loaded, scan_date)


def write_load_log(scan_date, source_file, read_cnt, loaded_cnt, rejected_cnt, dedup_cnt, status, err_msg=None):
    """Write run summary metrics to load_log."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO load_log (
                    run_date, source_file, rows_read, rows_loaded,
                    rows_rejected, rows_deduplicated, status, error_message
                ) VALUES (
                    :run_date, :source_file, :rows_read, :rows_loaded,
                    :rows_rejected, :rows_deduplicated, :status, :error_message
                )
                """
            ),
            {
                "run_date": scan_date,
                "source_file": source_file,
                "rows_read": read_cnt,
                "rows_loaded": loaded_cnt,
                "rows_rejected": rejected_cnt,
                "rows_deduplicated": dedup_cnt,
                "status": status,
                "error_message": str(err_msg) if err_msg else None,
            },
        )


def main(scan_date):
    log.info("run starting for %s", scan_date)
    source_file = f"scans_{scan_date}.csv"
    rows_read = 0
    rows_loaded = 0
    rows_rejected = 0
    rows_deduplicated = 0

    try:
        raw = extract(scan_date)
        rows_read = len(raw)

        # Empty file check (e.g. 15 May)
        if rows_read == 0:
            log.info("empty file for %s (0 rows), skipping load", scan_date)
            write_load_log(scan_date, source_file, 0, 0, 0, 0, "SKIPPED", "Header only / empty file")
            log.info("run finished for %s", scan_date)
            return 0

        check_contract(raw)

        hubs = pd.read_csv(os.path.join(DATA_DIR, "hubs.csv"))
        couriers = pd.read_csv(os.path.join(DATA_DIR, "couriers.csv"))
        services = pd.read_csv(os.path.join(DATA_DIR, "service_levels.csv"))
        customers = pd.read_sql_query("SELECT customer_id FROM customers", engine)

        good, rejects, rows_deduplicated = transform(
            raw, scan_date, hubs, couriers, customers, services
        )
        rows_loaded = len(good)
        rows_rejected = len(rejects)

        # Arithmetic reconciliation contract
        assert (
            rows_read == rows_loaded + rows_rejected + rows_deduplicated
        ), f"Arithmetic balance failed: {rows_read} != {rows_loaded} + {rows_rejected} + {rows_deduplicated}"

        load(good, scan_date)

        write_load_log(
            scan_date, source_file, rows_read, rows_loaded, rows_rejected, rows_deduplicated, "SUCCESS"
        )
        log.info("run finished for %s", scan_date)
        return 0

    except Exception as problem:
        log.error("run FAILED for %s: %s", scan_date, problem)
        write_load_log(
            scan_date, source_file, rows_read, rows_loaded, rows_rejected, rows_deduplicated, "FAILED", problem
        )
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    sys.exit(main(parser.parse_args().date))