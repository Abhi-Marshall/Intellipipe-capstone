# Databricks notebook source
# MAGIC %md
# MAGIC # Telemetry Generation — Integrated Full-Feature Table
# MAGIC Generates `pipeline_monitoring_metrics` combining:
# MAGIC - **PDF Quality Features:** null_pct, dup_pct, mismatch_count, missingcol_count
# MAGIC - **DOCX Temporal Features:** lag_1h, lag_2h, lag_3h, rolling_mean_3h, rolling_mean_6h, rolling_std_6h, hour_of_day, is_weekend, hour_sin, hour_cos

import pyspark.sql.functions as F
from pyspark.sql import Window
import uuid, math

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────────────────
CATALOG         = "capstone_project"
SCHEMA          = "capstone_schema"
BRONZE_TABLE    = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE    = f"{CATALOG}.{SCHEMA}.silver_orders"
OUTPUT_TABLE    = f"{CATALOG}.{SCHEMA}.pipeline_monitoring_metrics"

# Expected schema columns for mismatch detection
EXPECTED_COLUMNS = ["order_id", "customer_id", "product_id", "order_date",
                    "quantity", "unit_price", "discount", "status", "event_timestamp"]


# ────────────────────────────────────────────────────────────────────────────────
# STEP 1: Load Raw Layers
# ────────────────────────────────────────────────────────────────────────────────
df_bronze = spark.table(BRONZE_TABLE)
df_silver = spark.table(SILVER_TABLE)


# ────────────────────────────────────────────────────────────────────────────────
# STEP 2: Hourly Quality + Volume Metrics from BRONZE
# NOTE: We use Bronze so Silver's cleanup (dropDuplicates, dropNull) doesn't
#       hide anomalies from the model.
# ────────────────────────────────────────────────────────────────────────────────
def compute_bronze_hourly_metrics(df_bronze, expected_cols):
    """
    Computes per-hour quality and volume metrics from the Bronze layer.
    Returns a DataFrame with one row per hour.
    """
    # Derive columns present vs expected (missingcol_count)
    actual_cols    = df_bronze.columns
    missing_count  = len([c for c in expected_cols if c not in actual_cols])

    df_hourly = (
        df_bronze
        .withColumn("run_hour", F.date_trunc("hour", F.col("event_timestamp")))
        .groupBy("run_hour")
        .agg(
            F.count("*").alias("current_volume"),
            # null_pct: from Bronze to catch quality issues before Silver drops them
            (F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0)) * 100.0 / F.count("*")).alias("null_pct"),
            # dup_pct: percentage of duplicate order_ids per hour
            ((F.count("*") - F.countDistinct("order_id")) * 100.0 / F.count("*")).alias("dup_pct"),
        )
        .withColumn("row_count", F.col("current_volume"))
        .withColumn("missingcol_count", F.lit(missing_count))
    )
    return df_hourly


# ────────────────────────────────────────────────────────────────────────────────
# STEP 3: Schema Quality Metrics from SILVER (mismatch_count)
# The Silver layer's quality_score captures rows that passed constraints.
# Mismatch count = rows that failed expectations.
# ────────────────────────────────────────────────────────────────────────────────
def compute_silver_mismatch(df_silver):
    """
    Computes per-hour mismatch count from the Silver layer.
    quality_score < 1.0 indicates a row with quality constraint violations.
    """
    df_mismatch = (
        df_silver
        .withColumn("run_hour", F.date_trunc("hour", F.col("event_timestamp")))
        .groupBy("run_hour")
        .agg(
            # Count rows where quality_score is not perfect (< 1.0)
            F.sum(F.when(F.col("quality_score") < 1.0, 1).otherwise(0)).alias("mismatch_count")
        )
    )
    return df_mismatch


# ────────────────────────────────────────────────────────────────────────────────
# STEP 4: Add DOCX Temporal Features (Lag + Rolling + Cyclical)
# These are critical for detecting time-series volume anomalies.
# ────────────────────────────────────────────────────────────────────────────────
def add_temporal_features(df):
    """
    Adds all temporal features required by the DOCX specification:
    - hour_of_day, day_of_week, is_weekend
    - hour_sin, hour_cos (cyclical encoding)
    - lag_1h, lag_2h, lag_3h
    - rolling_mean_3h, rolling_mean_6h, rolling_std_6h
    - pct_change (volume vs. lag_1h)
    """
    w = Window.orderBy("run_hour")           # ordered window for lag
    w3 = Window.orderBy("run_hour").rowsBetween(-3, -1)   # rolling 3h lookback
    w6 = Window.orderBy("run_hour").rowsBetween(-6, -1)   # rolling 6h lookback

    df = (
        df
        # --- Cyclical Time Encoding ---
        .withColumn("hour_of_day",  F.hour("run_hour").cast("double"))
        .withColumn("day_of_week",  F.dayofweek("run_hour").cast("double"))
        .withColumn("is_weekend",   F.when(F.dayofweek("run_hour").isin([1, 7]), 1).otherwise(0))
        .withColumn("hour_sin",     F.sin(2 * math.pi * F.col("hour_of_day") / 24))
        .withColumn("hour_cos",     F.cos(2 * math.pi * F.col("hour_of_day") / 24))

        # --- Lag Features ---
        .withColumn("lag_1h", F.lag("current_volume", 1).over(w))
        .withColumn("lag_2h", F.lag("current_volume", 2).over(w))
        .withColumn("lag_3h", F.lag("current_volume", 3).over(w))

        # --- Rolling Statistics ---
        .withColumn("rolling_mean_3h", F.avg("current_volume").over(w3))
        .withColumn("rolling_mean_6h", F.avg("current_volume").over(w6))
        .withColumn("rolling_std_6h",  F.stddev("current_volume").over(w6))

        # --- Percentage Change from previous hour ---
        .withColumn("pct_change",
            F.when(F.col("lag_1h").isNotNull() & (F.col("lag_1h") != 0),
                   ((F.col("current_volume") - F.col("lag_1h")) / F.col("lag_1h")) * 100.0
            ).otherwise(0.0)
        )
        # Drop rows where lag features couldn't be computed yet
        .dropna(subset=["lag_1h", "rolling_mean_3h"])
    )
    return df


# ────────────────────────────────────────────────────────────────────────────────
# STEP 5: Assemble Final Monitoring Table
# ────────────────────────────────────────────────────────────────────────────────
df_bronze_metrics  = compute_bronze_hourly_metrics(df_bronze, EXPECTED_COLUMNS)
df_silver_mismatch = compute_silver_mismatch(df_silver)

# Join Bronze metrics + Silver mismatch count
df_combined = (
    df_bronze_metrics
    .join(df_silver_mismatch, on="run_hour", how="left")
    .fillna({"mismatch_count": 0})
)

# Add temporal features
df_final = add_temporal_features(df_combined)

# Add run metadata
df_final = (
    df_final
    .withColumn("run_id", F.expr("uuid()"))
    .withColumn("run_timestamp", F.col("run_hour"))
    .withColumn("pipeline_status", F.lit("completed"))
)

# Final column selection — The Integrated Feature Schema
df_final = df_final.select(
    "run_id",
    "run_timestamp",
    # --- Volume/Business Metrics ---
    "current_volume",
    "row_count",
    "pct_change",
    # --- Temporal Features (DOCX requirement) ---
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "lag_1h",
    "lag_2h",
    "lag_3h",
    "rolling_mean_3h",
    "rolling_mean_6h",
    "rolling_std_6h",
    # --- Quality Features (PDF requirement) ---
    "null_pct",
    "dup_pct",
    "mismatch_count",
    "missingcol_count",
    # --- Metadata ---
    "pipeline_status"
)

# ────────────────────────────────────────────────────────────────────────────────
# STEP 6: Write to Delta Table (Append Mode)
# ────────────────────────────────────────────────────────────────────────────────
(
    df_final.write
    .format("delta")
    .mode("append")
    .saveAsTable(OUTPUT_TABLE)
)

print(f"✅ Telemetry written to {OUTPUT_TABLE}")
print(f"   Total rows written: {df_final.count()}")
df_final.show(5, truncate=False)
