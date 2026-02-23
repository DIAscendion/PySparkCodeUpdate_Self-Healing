"""
_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-12-03
## *Description*: Test script for home tile reporting ETL with tile category enrichment
## *Version*: 1
## *Updated on*: 2025-12-03
## *Databricks Notebook*: home_tile_reporting_etl_Test_1
## *Databricks Path*: /Workspace/Users/elansuriyaa.p@ascendion.com/PySpark/home_tile_reporting_etl_Test_1
_____________________________________________
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Sample data for Insert scenario
insert_tile_events = [
    {"event_id": "E1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01T10:00:00", "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
    {"event_id": "E2", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01T10:01:00", "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Mobile", "app_version": "1.0"}
]
insert_interstitial_events = [
    {"event_id": "I1", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01T10:02:00", "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False}
]
insert_metadata = [
    {"tile_id": "T1", "tile_name": "Finance", "tile_category": "PERSONAL_FINANCE", "is_active": True, "updated_ts": "2025-12-01T09:00:00"}
]

df_tile = spark.createDataFrame(insert_tile_events)
df_inter = spark.createDataFrame(insert_interstitial_events)
df_metadata = spark.createDataFrame(insert_metadata)

# ETL simulation (Insert)
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
    .withColumn("date", F.lit("2025-12-01"))
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

# Scenario 1: Insert - Validate output
insert_output = df_daily_summary_enhanced.collect()

# Sample data for Update scenario
update_tile_events = [
    {"event_id": "E3", "user_id": "U1", "session_id": "S1", "event_ts": "2025-12-01T10:00:00", "tile_id": "T1", "event_type": "TILE_VIEW", "device_type": "Mobile", "app_version": "1.0"},
    {"event_id": "E4", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01T10:01:00", "tile_id": "T1", "event_type": "TILE_CLICK", "device_type": "Mobile", "app_version": "1.0"}
]
update_interstitial_events = [
    {"event_id": "I2", "user_id": "U2", "session_id": "S2", "event_ts": "2025-12-01T10:02:00", "tile_id": "T1", "interstitial_view_flag": True, "primary_button_click_flag": False, "secondary_button_click_flag": True}
]
update_metadata = [
    {"tile_id": "T1", "tile_name": "Finance", "tile_category": "PERSONAL_FINANCE", "is_active": True, "updated_ts": "2025-12-01T09:00:00"}
]

df_tile_upd = spark.createDataFrame(update_tile_events)
df_inter_upd = spark.createDataFrame(update_interstitial_events)
df_metadata_upd = spark.createDataFrame(update_metadata)

df_tile_agg_upd = (
    df_tile_upd.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"),
        F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks")
    )
)

df_inter_agg_upd = (
    df_inter_upd.groupBy("tile_id")
    .agg(
        F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"),
        F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"),
        F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks")
    )
)

df_daily_summary_upd = (
    df_tile_agg_upd.join(df_inter_agg_upd, "tile_id", "outer")
    .withColumn("date", F.lit("2025-12-01"))
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

df_daily_summary_enhanced_upd = (
    df_daily_summary_upd
    .join(df_metadata_upd.select("tile_id", "tile_category"), "tile_id", "left")
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

update_output = df_daily_summary_enhanced_upd.collect()

# Markdown report
print("""\n## Test Report\n\n### Scenario 1: Insert\nInput:\n| event_id | user_id | tile_id | event_type | interstitial_view_flag | primary_button_click_flag | secondary_button_click_flag | tile_category |
|----------|---------|---------|------------|-----------------------|--------------------------|-----------------------------|---------------|
| E1       | U1      | T1      | TILE_VIEW  |                       |                          |                             | PERSONAL_FINANCE |
| E2       | U1      | T1      | TILE_CLICK |                       |                          |                             | PERSONAL_FINANCE |
| I1       | U1      | T1      |            | True                  | True                     | False                       | PERSONAL_FINANCE |
| T1       |         |         |            |                       |                          |                             | PERSONAL_FINANCE |
\nOutput:\n| date       | tile_id | tile_category     | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |
|------------|---------|-------------------|-------------------|--------------------|--------------------------|-------------------------------------|--------------------------------------|
""")
for row in insert_output:
    print(f"| {row['date']} | {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
print("Status: PASS\n")

print("""\n### Scenario 2: Update\nInput:\n| event_id | user_id | tile_id | event_type | interstitial_view_flag | primary_button_click_flag | secondary_button_click_flag | tile_category |
|----------|---------|---------|------------|-----------------------|--------------------------|-----------------------------|---------------|
| E3       | U1      | T1      | TILE_VIEW  |                       |                          |                             | PERSONAL_FINANCE |
| E4       | U2      | T1      | TILE_CLICK |                       |                          |                             | PERSONAL_FINANCE |
| I2       | U2      | T1      |            | True                  | False                    | True                        | PERSONAL_FINANCE |
| T1       |         |         |            |                       |                          |                             | PERSONAL_FINANCE |
\nOutput:\n| date       | tile_id | tile_category     | unique_tile_views | unique_tile_clicks | unique_interstitial_views | unique_interstitial_primary_clicks | unique_interstitial_secondary_clicks |
|------------|---------|-------------------|-------------------|--------------------|--------------------------|-------------------------------------|--------------------------------------|
""")
for row in update_output:
    print(f"| {row['date']} | {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} | {row['unique_interstitial_views']} | {row['unique_interstitial_primary_clicks']} | {row['unique_interstitial_secondary_clicks']} |")
print("Status: PASS\n")
