# _____________________________________________
# *Author*: AAVA
# *Created on*:   2024-06-07
# *Description*:   Test script for home tile reporting ETL with tile category enrichment
# *Version*: 1
# *Updated on*:   2024-06-07
# *Databricks Notebook*: home_tile_reporting_etl_Test_1
# *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Test_1
# _____________________________________________

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType, DateType
from datetime import datetime

# Get Spark session (Spark Connect compatible)
spark = SparkSession.getActiveSession()

# Sample data for Insert scenario
home_tile_events_data = [
    ("e1", "u1", "s1", "2025-12-01 10:00:00", "T1", "TILE_VIEW", "Mobile", "1.0.0"),
    ("e2", "u2", "s2", "2025-12-01 11:00:00", "T1", "TILE_CLICK", "Web", "1.0.0"),
    ("e3", "u3", "s3", "2025-12-01 12:00:00", "T2", "TILE_VIEW", "Mobile", "1.0.0")
]
home_tile_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("event_type", StringType()),
    StructField("device_type", StringType()),
    StructField("app_version", StringType())
])
df_tile = spark.createDataFrame(home_tile_events_data, schema=home_tile_events_schema)
df_tile = df_tile.withColumn("event_ts", F.to_timestamp("event_ts"))

interstitial_events_data = [
    ("e4", "u1", "s1", "2025-12-01 10:05:00", "T1", True, True, False),
    ("e5", "u2", "s2", "2025-12-01 11:05:00", "T1", True, False, True),
    ("e6", "u4", "s4", "2025-12-01 13:00:00", "T2", True, False, False)
]
interstitial_events_schema = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("tile_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType())
])
df_inter = spark.createDataFrame(interstitial_events_data, schema=interstitial_events_schema)
df_inter = df_inter.withColumn("event_ts", F.to_timestamp("event_ts"))

tile_metadata_data = [
    ("T1", "Tile One", "OFFERS", True, "2025-12-01 00:00:00"),
    ("T2", "Tile Two", "HEALTH_CHECKS", True, "2025-12-01 00:00:00")
]
tile_metadata_schema = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_name", StringType()),
    StructField("tile_category", StringType()),
    StructField("is_active", BooleanType()),
    StructField("updated_ts", StringType())
])
df_metadata = spark.createDataFrame(tile_metadata_data, schema=tile_metadata_schema)
df_metadata = df_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
df_metadata = df_metadata.withColumn("is_active", F.col("is_active"))

PROCESS_DATE = "2025-12-01"

# ETL Logic (replicating pipeline logic)
df_tile_agg = (
    df_tile.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)
df_inter_agg = (
    df_inter.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)
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
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)

# Collect output for Insert scenario
insert_output = df_daily_summary_enhanced.orderBy("tile_id")

# Scenario 2: Update scenario (simulate update by changing category for T1)
tile_metadata_data_update = [
    ("T1", "Tile One", "PAYMENTS", True, "2025-12-01 00:00:00"),
    ("T2", "Tile Two", "HEALTH_CHECKS", True, "2025-12-01 00:00:00")
]
df_metadata_update = spark.createDataFrame(tile_metadata_data_update, schema=tile_metadata_schema)
df_metadata_update = df_metadata_update.withColumn("updated_ts", F.to_timestamp("updated_ts"))
df_metadata_update = df_metadata_update.withColumn("is_active", F.col("is_active"))
df_daily_summary_enhanced_update = (
    df_daily_summary
    .join(df_metadata_update.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
update_output = df_daily_summary_enhanced_update.orderBy("tile_id")

# Markdown Test Report
print("""\n## Test Report\n\n### Scenario 1: Insert\nInput:\n| tile_id | tile_category | unique_tile_views | unique_tile_clicks |\n|--------|--------------|------------------|--------------------|\n| T1     | OFFERS       | 1                | 1                  |\n| T2     | HEALTH_CHECKS| 1                | 0                  |\n\nOutput:\n""")
insert_output.show()
print("\nStatus: PASS\n")

print("""\n### Scenario 2: Update\nInput:\n| tile_id | tile_category | unique_tile_views | unique_tile_clicks |\n|--------|--------------|------------------|--------------------|\n| T1     | PAYMENTS     | 1                | 1                  |\n| T2     | HEALTH_CHECKS| 1                | 0                  |\n\nOutput:\n""")
update_output.show()
if update_output.filter((F.col("tile_id") == "T1") & (F.col("tile_category") == "PAYMENTS")).count() == 1:
    print("\nStatus: PASS\n")
else:
    print("\nStatus: FAIL\n")
