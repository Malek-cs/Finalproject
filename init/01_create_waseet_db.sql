-- Fact Table: parcel_scans
-- 
-- Defense of the 5 decisions:
-- 1. Grain: Exactly one row represents a single physical scan event for a parcel at a given hub/timestamp.
-- 2. Nullability:
--    - NOT NULL: parcel_id, customer_id, scanned_at, hub_id, scan_type (core event attributes).
--    - NULLABLE: courier_id (unassigned scans before dispatch), weight_kg (not recorded at every scan point),
--      and service_code (optional or unassigned).
-- 3. Foreign keys: Enforced on customer_id, hub_id, courier_id, and service_code to maintain referential
--    integrity against the warehouse dimensions, with bad hub references filtered into rejects upstream in the pipeline.
-- 4. Primary key: scan_id is NOT declared as a PRIMARY KEY constraint. The raw feed contains duplicate scan_id
--    records (re-sent events). Making it a PK would cause pipeline crashes on duplicates rather than graceful deduplication.
-- 5. Index: Single B-tree index on scanned_at because daily reporting, SLA calculations, and idempotent deletions
--    filter directly on the scan timestamp/date range. Additional index on parcel_id for customer parcel lifecycle tracking.

CREATE TABLE parcel_scans (
    scan_id         TEXT,
    parcel_id       TEXT NOT NULL,
    customer_id     INTEGER NOT NULL REFERENCES customers(customer_id),
    scanned_at      TIMESTAMP NOT NULL,
    hub_id          INTEGER NOT NULL REFERENCES hubs(hub_id),
    courier_id      INTEGER REFERENCES couriers(courier_id),
    scan_type       TEXT NOT NULL,
    weight_kg       NUMERIC(8, 2),
    service_code    TEXT REFERENCES service_levels(service_code),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_parcel_scans_scanned_at ON parcel_scans (scanned_at);
CREATE INDEX idx_parcel_scans_parcel_id ON parcel_scans (parcel_id);

-- Run log table: load_log
-- One row per execution tracking pipeline run metrics and auditability.

CREATE TABLE load_log (
    run_id              SERIAL PRIMARY KEY,
    run_date            DATE NOT NULL,
    source_file         TEXT NOT NULL,
    rows_read           INTEGER NOT NULL,
    rows_loaded         INTEGER NOT NULL,
    rows_rejected       INTEGER NOT NULL,
    rows_deduplicated   INTEGER NOT NULL,
    status              TEXT NOT NULL, -- 'SUCCESS', 'FAILED', 'SKIPPED'
    error_message       TEXT,
    executed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);