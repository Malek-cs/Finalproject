# Waseet Logistics Capstone: Production Data Platform

An end-to-end data engineering pipeline processing daily logistics parcel scans, executing declarative quality audits, orchestrating via Apache Airflow, and performing distributed historical aggregations in PySpark.

---

## 1. Stack Architecture & System Design

[ Inbound CSVs / REST API ]
              │
              ▼
      ┌───────────────┐
      │  pipeline.py  │ ◄─── Fail-fast Contract Check & Idempotent Load
      └───────┬───────┘
              │
      ┌───────┴───────┐
      ▼               ▼
[ PostgreSQL ]  [ Quarantine ]
 (parcel_scans)   (Rejects CSV)
      │
      ▼
┌───────────────┐
│  quality.py   │ ◄─── 10 Declarative Rules & Arithmetic Reconciliation
└───────────────┘
      ▲
      │ (Orchestrated by Airflow DAG: waseet_daily)
      │
┌───────────────┐
│history_job.py │ ◄─── Historical Analytics & Broadcast Hash Joins
└───────┬───────┘
        ▼
 [ Parquet Lake ]

* Relational Store (PostgreSQL 16): Houses dimensions, daily transactional parcel_scans, and the load_log audit ledger. Enforces event uniqueness via a composite constraint on (parcel_id, scan_type, scanned_at).
* Pipeline Engine (pipeline/pipeline.py): Ingests raw inputs as strings, executes fail-fast contract verification, repairs mixed timestamps/weights, routes non-compliant records to CSV quarantine, and loads clean data idempotently.
* Quality & Audit (pipeline/quality.py): Runs 10 declarative rules across Completeness, Format, Validity, and Range, evaluating reconciliation metrics against load_log.
* Orchestration (dags/waseet_daily.py): Manages file sensing (reschedule mode), holiday branch routing, task execution, and automated failure alerting.
* Distributed Analytics (spark/history_job.py): Aggregates 285,000 historical scan events using explicit schemas, broadcast joins for small lookup dimensions, and partitioned Parquet generation.

---

## 2. Quickstart & Service Initialization

### Prerequisites
* Docker Engine 24+ & Docker Compose v2+
* Python 3.10+

### Start the Infrastructure
# Clone the repository
git clone https://github.com/Malek-cs/Finalproject.git
cd Finalproject

# Boot PostgreSQL and Apache Airflow
docker compose up -d

# Verify services are healthy
docker compose ps

### Ingest Dimension & Reference Data
# Apply DDL migrations
docker compose exec -T postgres psql -U de -d waseet < init/01_create_waseet_db.sql

# Ingest API customer records (handles pagination & retries)
python pipeline/ingest_customers.py

---

## 3. Running Pipelines

### Single Day Execution
# 1. Ingest, clean, and load
python pipeline/pipeline.py --date 2026-05-04

# 2. Run quality assertions and reconciliation audit
python pipeline/quality.py --date 2026-05-04

### Automated 21-Day Backfill (Airflow)
# Unpause DAG
docker compose exec airflow-webserver airflow dags unpause waseet_daily

# Trigger DAG backfill
docker compose exec airflow-webserver airflow dags backfill waseet_daily \
    --start-date 2026-05-01 \
    --end-date 2026-05-21

### PySpark Historical Analytics
# Execute the historical aggregation batch job
spark-submit --driver-memory 4g spark/history_job.py

---

## 4. Operational Incidents & Edge Case Handling

* 2026-05-10: Supplier delivery failure (missing file). wait_for_file sensor timed out after 30s polling, triggered alert_failure callback, preventing empty runs without corrupting the warehouse.
* 2026-05-13: Duplicate batch delivery (2,546 rows, 2x volume). Pre-transform deduplication identified duplicated scan_ids, discarded redundant events, and cleanly loaded exactly 1,248 rows.
* 2026-05-15: Eid Holiday (0 data rows). File arrived header-only; choose_branch routed to skip_day. finish completed cleanly under none_failed_min_one_success.
* 2026-05-19: Schema drift (weight_kg missing). Fail-fast contract check raised a contract violation against baseline_schema.json. Run aborted with exit code 1 before transform.

---

## 5. Spark Historical Job & The Small-Files Analysis

* Explicit Typing: Enforced via StructType over 285k historical records, eliminating the double-read latency of inferSchema=True.
* Broadcast Hash Joins: Explicitly applied via F.broadcast() to hubs.csv (10 rows) and service_levels.csv, bypassing expensive cross-network cluster shuffle stages.
* The Small Files Problem: Partitioning an 18 MB historical dataset across 334 individual calendar days generated over 334 tiny Parquet files (~15–50 KB each). In enterprise HDFS/S3 deployments, micro-files create NameNode metadata pressure and ruin columnar compression efficiency.
* Production Recommendation: Retain historical datasets of this scale unpartitioned or partition coarsely by month (yyyy-MM), targeting file chunk sizes between 128 MB and 512 MB.

---

## 6. Stack Modifications & Deliverables

* Strict Half-Open Idempotency Windows: Replaced standard <= '23:59:59' deletions with [scan_date 00:00:00, next_day 00:00:00) intervals to protect against sub-second precision overlaps.
* Declarative Quality Gating: Configured quality.py to exit with status code 1 upon missing schema baselines, reconciliation gaps, or rule violations, enabling fail-fast DAG pipeline semantics.

---

## 7. What Was Not Finished & Known Limitations

Stated plainly: All core deliverables specified across Ingestion, Processing, Quality, Storage, Orchestration, and Distributed Analytics have been implemented, tested, and validated. 

However, two operational edge cases remain as known production trade-offs:
1. Automated Quarantined Data Reprocessing: Records rejected into quarantine/rejects_YYYY-MM-DD.csv require manual remediation scripts. An automated DLQ (Dead Letter Queue) replay mechanism has not yet been built.
2. Upstream Ingestion Worker Queue: The customer API pagination script relies on synchronous HTTP requests with backoff. At high enterprise scale, this should be migrated to an asynchronous worker queue (such as Celery/Redis).