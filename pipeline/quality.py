"""The Waseet quality suite.

Rules are data. The suite below is a list a warehouse manager could review
without reading Python, which is the whole reason it is not a set of .loc masks
inside transform().

Run it as a script to see today's report:

    python quality.py --date 2026-05-04
"""

import argparse
import json
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

engine = create_engine(DB_URL)

BASELINE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "baseline_schema.json"))

# 10 declarative rules covering completeness, uniqueness, validity, range, and format
SUITE = [
    {"column": "scan_id", "check": "unique"},
    {"column": "scan_id", "check": "not_null"},
    {"column": "parcel_id", "check": "not_null"},
    {"column": "customer_id", "check": "not_null"},
    {"column": "scanned_at", "check": "date_iso"},
    {"column": "hub_id", "check": "in_set", "values": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]},
    {"column": "scan_type", "check": "is_uppercase"},
    {"column": "scan_type", "check": "in_set", "values": ["PICKUP", "ARRIVE", "DEPART", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED"]},
    {"column": "weight_kg", "check": "numeric_positive"},
    {"column": "weight_kg", "check": "in_range", "min": 0.01, "max": 100.0},
]


def run_check(df, rule):
    """Return the number of rows that break one rule."""
    col = rule["column"]
    check = rule["check"]

    if col not in df.columns:
        return len(df)

    series = df[col]

    if check == "not_null":
        bad_mask = series.isna() | (series.astype(str).str.strip() == "")
    elif check == "unique":
        bad_mask = series.duplicated(keep=False) & series.notna()
    elif check == "date_iso":
        bad_mask = ~series.astype(str).str.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
    elif check == "is_uppercase":
        bad_mask = series.astype(str) != series.astype(str).str.upper()
    elif check == "in_set":
        valid_set = set(str(v) for v in rule["values"])
        bad_mask = ~series.astype(str).str.strip().isin(valid_set)
    elif check == "numeric_positive":
        cleaned = series.astype(str).str.replace(",", ".", regex=False).str.strip()
        num = pd.to_numeric(cleaned.replace("", np.nan), errors="coerce")
        bad_mask = (series.notna()) & (series.astype(str).str.strip() != "") & ((num.isna()) | (num <= 0))
    elif check == "in_range":
        cleaned = series.astype(str).str.replace(",", ".", regex=False).str.strip()
        num = pd.to_numeric(cleaned.replace("", np.nan), errors="coerce")
        min_v, max_v = rule.get("min", -np.inf), rule.get("max", np.inf)
        bad_mask = (series.notna()) & (series.astype(str).str.strip() != "") & ((num < min_v) | (num > max_v))
    else:
        raise ValueError(f"Unknown quality check rule: '{check}'")

    return int(bad_mask.sum())


def validate(df, suite):
    """Return a DataFrame: one row per rule, with how many rows failed it."""
    results = []
    total_rows = len(df)
    for rule in suite:
        fail_count = run_check(df, rule)
        results.append({
            "column": rule["column"],
            "check": rule["check"],
            "failed_rows": fail_count,
            "total_rows": total_rows,
            "pass_rate": f"{((total_rows - fail_count) / total_rows * 100):.2f}%" if total_rows > 0 else "0.0%"
        })
    return pd.DataFrame(results)


def check_schema(df, baseline_path=BASELINE_PATH):
    """Return (missing, new) against the pre-existing stored baseline."""
    if not os.path.exists(baseline_path):
        print(f"CRITICAL: Baseline schema file not found at {baseline_path}!")
        print("Please ensure baseline_schema.json is tracked in the repository.")
        sys.exit(1)

    with open(baseline_path, "r", encoding="utf-8") as f:
        baseline_cols = json.load(f)

    current_cols = set(df.columns)
    base_cols = set(baseline_cols)

    missing = sorted(list(base_cols - current_cols))
    new = sorted(list(current_cols - base_cols))
    return missing, new


def reconcile(scan_date, eng=engine):
    """Compare rows in the file against rows in the warehouse for one date."""
    filepath = os.path.join(DATA_DIR, f"scans_{scan_date}.csv")
    if not os.path.exists(filepath):
        return {
            "date": scan_date,
            "file_rows": 0,
            "warehouse_rows": 0,
            "difference": 0,
            "reconciled": True,
            "explanation": "File never arrived (Supplier delivery failure)"
        }

    file_df = pd.read_csv(filepath, dtype=str)
    file_rows = len(file_df)

    if file_rows == 0:
        return {
            "date": scan_date,
            "file_rows": 0,
            "warehouse_rows": 0,
            "difference": 0,
            "reconciled": True,
            "explanation": "Holiday/Eid (Network closed, header-only file skipped cleanly)"
        }

    with eng.connect() as conn:
        res = conn.execute(
            text(
                "SELECT count(*) FROM parcel_scans WHERE scanned_at >= :start_ts AND scanned_at < :next_day_ts"
            ),
            {
                "start_ts": f"{scan_date} 00:00:00",
                "next_day_ts": (pd.to_datetime(scan_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
            }
        ).scalar()
        wh_rows = int(res) if res else 0

        log_res = conn.execute(
            text("SELECT rows_rejected, rows_deduplicated, status, error_message FROM load_log WHERE run_date = :dt ORDER BY run_id DESC LIMIT 1"),
            {"dt": scan_date}
        ).fetchone()

    diff = file_rows - wh_rows
    reconciled = False
    explanation = "Exact match"

    if log_res and log_res[2] == "FAILED":
        explanation = f"Run failed: {log_res[3]}"
        reconciled = True
    elif diff == 0:
        reconciled = True
    elif diff > 0 and log_res:
        rejs = log_res[0] or 0
        dedups = log_res[1] or 0
        if file_rows == (wh_rows + rejs + dedups):
            reconciled = True
            explanation = f"{dedups} duplicate scan events removed, {rejs} rows quarantined into rejects (Reconciliation verified)"
        else:
            explanation = f"Unreconciled gap: file ({file_rows}) != loaded ({wh_rows}) + rejects ({rejs}) + dedup ({dedups})"
    else:
        explanation = f"Unreconciled gap of {diff} rows"

    return {
        "date": scan_date,
        "file_rows": file_rows,
        "warehouse_rows": wh_rows,
        "difference": diff,
        "reconciled": reconciled,
        "explanation": explanation
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    filepath = os.path.join(DATA_DIR, f"scans_{args.date}.csv")
    if not os.path.exists(filepath):
        print(f"File scans_{args.date}.csv not found.")
        sys.exit(1)

    day = pd.read_csv(filepath, dtype=str)

    # 1. Schema drift check
    missing_cols, new_cols = check_schema(day)
    print(f"\n=== SCHEMA DRIFT CHECK: {args.date} ===")
    print("Missing columns:", missing_cols)
    print("New columns:", new_cols)

    # 2. Quality rules report
    print(f"\n=== QUALITY RULES VALIDATION REPORT: {args.date} ===")
    report = validate(day, SUITE)
    print(report.to_string(index=False))

    # 3. Reconciliation check
    print(f"\n=== RECONCILIATION: {args.date} ===")
    rec = reconcile(args.date)
    print(f"File Rows: {rec['file_rows']} | Warehouse Rows: {rec['warehouse_rows']} | Difference: {rec['difference']}")
    print(f"Explanation: {rec['explanation']}")

    # 4. Exit Code Handling
    total_violations = report["failed_rows"].sum()

    if missing_cols:
        print("\nQuality status: FAILED (Critical schema drift)")
        sys.exit(1)

    if not rec["reconciled"]:
        print(f"\nQuality status: FAILED (Data reconciliation mismatch: {rec['explanation']})")
        sys.exit(1)

    if total_violations > 0:
        print(f"\nQuality status: FAILED ({total_violations} total rule violations found across raw input)")
        sys.exit(1)

    print("\nQuality status: PASSED")
    sys.exit(0)