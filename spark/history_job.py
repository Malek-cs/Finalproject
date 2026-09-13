"""The historical scan file, in Spark.

    spark-submit spark/history_job.py

`data/scans_history.csv` is eleven months of scans - about 285,000 rows and 18 MB.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# 1. Initialize Spark Session with local master
spark = (
    SparkSession.builder.appName("waseet-history")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Resolve data directories
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "curated_scans")

# 2. Explicit Schema Definition (avoids 2nd full scan from inferSchema)
SCHEMA = StructType(
    [
        StructField("scan_id", StringType(), False),
        StructField("parcel_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("scanned_at", StringType(), False),
        StructField("hub_id", IntegerType(), True),
        StructField("courier_id", StringType(), True),
        StructField("scan_type", StringType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("service_code", StringType(), True),
    ]
)

# 3. Read Raw History File
history_path = os.path.join(DATA_DIR, "scans_history.csv")
print(f"[*] Reading historical scans from: {history_path}")
history = spark.read.csv(history_path, schema=SCHEMA, header=True)

# 4. Dimension Data (Hubs & Service Levels)
hubs_path = os.path.join(DATA_DIR, "hubs.csv")
service_levels_path = os.path.join(DATA_DIR, "service_levels.csv")

hubs_df = spark.read.csv(hubs_path, header=True, inferSchema=True)
service_levels_df = spark.read.csv(service_levels_path, header=True, inferSchema=True)

# 5. Transform & Shape:
# - parse scanned_at to timestamp
# - derive scan_date and scan_month for partitioning and analytical aggregations
# - broadcast join small dimensions to avoid shuffles
shaped_df = (
    history.withColumn("scanned_at_ts", F.to_timestamp("scanned_at"))
    .withColumn("scan_date", F.to_date("scanned_at_ts"))
    .withColumn("scan_month", F.date_format("scanned_at_ts", "yyyy-MM"))
    .join(F.broadcast(hubs_df), on="hub_id", how="left")
    .join(F.broadcast(service_levels_df), on="service_code", how="left")
    .drop("scanned_at")  # dropped in favor of clean typed timestamp
)

# Cache curated representation for iterative aggregations
shaped_df.cache()

# 6. Aggregation 1: Scans per hub per day
print("\n=== AGGREGATION 1: Scans per Hub per Day (Sample) ===")
scans_per_hub_day = shaped_df.groupBy("hub_id", "hub_name", "scan_date").agg(
    F.count("scan_id").alias("scan_count")
).orderBy("scan_date", "hub_id")

scans_per_hub_day.show(10, truncate=False)
print("\n--- Physical Plan for Aggregation 1 ---")
scans_per_hub_day.explain(mode="simple")

# 7. Aggregation 2: Share of DELIVERED vs FAILED per region per month
print("\n=== AGGREGATION 2: Delivery vs Failure Share per Region/Month ===")
terminal_scans = shaped_df.filter(F.col("scan_type").isin("DELIVERED", "FAILED"))

outcome_share_df = (
    terminal_scans.groupBy("region", "scan_month")
    .agg(
        F.count(F.when(F.col("scan_type") == "DELIVERED", 1)).alias("delivered_count"),
        F.count(F.when(F.col("scan_type") == "FAILED", 1)).alias("failed_count"),
        F.count("scan_id").alias("total_terminal_scans"),
    )
    .withColumn(
        "delivery_rate_pct",
        F.round((F.col("delivered_count") / F.col("total_terminal_scans")) * 100, 2),
    )
    .withColumn(
        "failure_rate_pct",
        F.round((F.col("failed_count") / F.col("total_terminal_scans")) * 100, 2),
    )
    .orderBy("region", "scan_month")
)

outcome_share_df.show(20, truncate=False)
print("\n--- Physical Plan for Aggregation 2 ---")
outcome_share_df.explain(mode="simple")

# 8. Write Curated Zone to Parquet, partitioned by scan_date
print(f"\n[*] Writing Curated Parquet to: {OUTPUT_DIR}")

# Columns required for downstream consumption in the curated lake zone
curated_export = shaped_df.select(
    "scan_id",
    "parcel_id",
    "customer_id",
    "scanned_at_ts",
    "scan_date",
    "hub_id",
    "hub_name",
    "region",
    "courier_id",
    "scan_type",
    "weight_kg",
    "service_code",
    "promised_hours",
)

(
    curated_export.write.mode("overwrite")
    .partitionBy("scan_date")
    .parquet(OUTPUT_DIR)
)

print("Spark historical pipeline executed successfully.")
spark.stop()