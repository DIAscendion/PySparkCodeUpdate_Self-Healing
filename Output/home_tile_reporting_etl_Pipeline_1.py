"""
_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-12-03
## *Description*: Enhanced Home Tile Reporting ETL with Tile Category Integration
## *Version*: 1
## *Updated on*: 2025-12-03
## *Changes*: Initial version with SOURCE_TILE_METADATA integration
## *Reason*: Product Analytics requested tile-level metadata for category-based reporting
_____________________________________________

===============================================================================
                        CHANGE MANAGEMENT / REVISION HISTORY
===============================================================================
File Name       : home_tile_reporting_etl_Pipeline_1.py
Author          : AAVA
Created Date    : 2025-12-03
Last Modified   : 2025-12-03
Version         : 1.0.0
Release         : R2 – Home Tile Reporting Enhancement with Tile Category

Functional Description:
    This ETL pipeline performs the following:
    - Reads home tile interaction events and interstitial events from source tables
    - Reads tile metadata from SOURCE_TILE_METADATA for category enrichment
    - Computes aggregated metrics:
        • Unique Tile Views
        • Unique Tile Clicks
        • Unique Interstitial Views
        • Unique Primary Button Clicks
        • Unique Secondary Button Clicks
        • CTRs for homepage tiles and interstitial buttons
    - Enriches data with tile_category from metadata table
    - Loads aggregated results into:
        • TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category)
        • TARGET_HOME_TILE_GLOBAL_KPIS
    - Supports idempotent daily partition overwrite
    - Designed for scalable production workloads (Databricks/Spark)

Change Log:
-------------------------------------------------------------------------------
Version     Date          Author          Description
-------------------------------------------------------------------------------
1.0.0       2025-12-03    AAVA           Enhanced version with tile category integration
-------------------------------------------------------------------------------
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import logging

# ------------------------------------------------------------------------------
# LOGGING CONFIGURATION
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
PIPELINE_NAME = "HOME_TILE_REPORTING_ETL_ENHANCED"

SOURCE_HOME_TILE_EVENTS = "analytics_db.SOURCE_HOME_TILE_EVENTS"
SOURCE_INTERSTITIAL_EVENTS = "analytics_db.SOURCE_INTERSTITIAL_EVENTS"
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"  # New metadata table

TARGET_DAILY_SUMMARY = "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY"
TARGET_GLOBAL_KPIS = "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"

PROCESS_DATE = "2025-12-01"  # pass dynamically from ADF/Airflow if needed

# Get active Spark session (Spark Connect compatible)
spark = SparkSession.builder.getOrCreate()

logger.info(f"Starting {PIPELINE_NAME} for process date: {PROCESS_DATE}")

# ------------------------------------------------------------------------------
# DATA QUALITY VALIDATION FUNCTIONS
# ------------------------------------------------------------------------------
def validate_metadata_join(df_summary, df_enhanced):
    """
    Ensure no record loss during metadata join
    
    Args:
        df_summary: Original summary DataFrame before metadata join
        df_enhanced: Enhanced DataFrame after metadata join
    
    Returns:
        bool: True if validation passes
    
    Raises:
        ValueError: If record counts don't match
    """
    original_count = df_summary.count()
    enhanced_count = df_enhanced.count()
    
    logger.info(f"Validation - Original count: {original_count}, Enhanced count: {enhanced_count}")
    
    if original_count != enhanced_count:
        error_msg = f"Record count mismatch: Original={original_count}, Enhanced={enhanced_count}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Metadata join validation passed successfully")
    return True

def validate_source_data(df, table_name, min_records=1):
    """
    Validate source data has minimum required records
    
    Args:
        df: DataFrame to validate
        table_name: Name of the source table
        min_records: Minimum number of records expected
    
    Returns:
        bool: True if validation passes
    """
    record_count = df.count()
    logger.info(f"Source table {table_name} has {record_count} records")
    
    if record_count < min_records:
        logger.warning(f"Source table {table_name} has fewer than {min_records} records")
    
    return True

# ------------------------------------------------------------------------------
# READ SOURCE TABLES
# ------------------------------------------------------------------------------
logger.info("Reading source tables...")

# Read home tile events
df_tile = (
    spark.table(SOURCE_HOME_TILE_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)
validate_source_data(df_tile, SOURCE_HOME_TILE_EVENTS)

# Read interstitial events
df_inter = (
    spark.table(SOURCE_INTERSTITIAL_EVENTS)
    .filter(F.to_date("event_ts") == PROCESS_DATE)
)
validate_source_data(df_inter, SOURCE_INTERSTITIAL_EVENTS)

# Read tile metadata (only active tiles)
df_metadata = (
    spark.table(SOURCE_TILE_METADATA)
    .filter(F.col("is_active") == True)
    .select("tile_id", "tile_category", "tile_name")
)
validate_source_data(df_metadata, SOURCE_TILE_METADATA, min_records=0)

logger.info("Source tables read successfully")

# ------------------------------------------------------------------------------
# DAILY TILE SUMMARY AGGREGATION
# ------------------------------------------------------------------------------
logger.info("Computing daily tile summary aggregations...")

# Aggregate tile events
df_tile_agg = (
    df_tile.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)

# Aggregate interstitial events
df_inter_agg = (
    df_inter.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)

# Combine tile and interstitial aggregations
df_daily_summary = (
    df_tile_agg.join(df_inter_agg, "tile_id", "outer")
    .withColumn("date", F.lit(PROCESS_DATE))
    .select(
        "date",
        "tile_id",
        F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"),
        F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"),
        F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"),
        F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"),
        F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks")
    )
)

logger.info("Daily summary aggregations completed")

# ------------------------------------------------------------------------------
# ENHANCE WITH TILE METADATA (NEW LOGIC)
# ------------------------------------------------------------------------------
logger.info("Enriching daily summary with tile metadata...")

# Left join with metadata to add tile_category
# Using left join ensures no records are lost if metadata is missing
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",  # New column added
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)

# Validate metadata join didn't lose records
validate_metadata_join(df_daily_summary, df_daily_summary_enhanced)

logger.info("Tile metadata enrichment completed successfully")

# ------------------------------------------------------------------------------
# GLOBAL KPIs (UNCHANGED)
# ------------------------------------------------------------------------------
logger.info("Computing global KPIs...")

df_global = (
    df_daily_summary_enhanced.groupBy("date")
    .agg(
        F.sum("unique_tile_views").alias("total_tile_views"),
        F.sum("unique_tile_clicks").alias("total_tile_clicks"),
        F.sum("unique_interstitial_views").alias("total_interstitial_views"),
        F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"),
        F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")
    )
    .withColumn(
        "overall_ctr",
        F.when(F.col("total_tile_views") > 0,
               F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_primary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
    .withColumn(
        "overall_secondary_ctr",
        F.when(F.col("total_interstitial_views") > 0,
               F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)
    )
)

logger.info("Global KPIs computed successfully")

# ------------------------------------------------------------------------------
# WRITE TARGET TABLES – IDEMPOTENT PARTITION OVERWRITE
# ------------------------------------------------------------------------------
def overwrite_partition(df, table, partition_col="date"):
    """
    Write DataFrame to Delta table with idempotent partition overwrite
    
    Args:
        df: DataFrame to write
        table: Target table name
        partition_col: Partition column name (default: 'date')
    """
    logger.info(f"Writing to table {table} with partition overwrite for {PROCESS_DATE}")
    
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("replaceWhere", f"{partition_col} = '{PROCESS_DATE}'")
          .saveAsTable(table)
    )
    
    logger.info(f"Successfully wrote to table {table}")

# Write enhanced daily summary (with tile_category)
overwrite_partition(df_daily_summary_enhanced, TARGET_DAILY_SUMMARY)

# Write global KPIs (unchanged)
overwrite_partition(df_global, TARGET_GLOBAL_KPIS)

logger.info(f"ETL completed successfully for {PROCESS_DATE}")
logger.info(f"Pipeline {PIPELINE_NAME} execution completed")

print(f"✅ ETL completed successfully for {PROCESS_DATE}")
print(f"✅ Enhanced daily summary with tile_category written to {TARGET_DAILY_SUMMARY}")
print(f"✅ Global KPIs written to {TARGET_GLOBAL_KPIS}")
"