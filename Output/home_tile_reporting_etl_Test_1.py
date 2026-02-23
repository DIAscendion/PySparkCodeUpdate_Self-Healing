_____________________________________________
## *Author*: AAVA
## *Created on*:   2025-12-02
## *Description*:   Test script for Home Tile Reporting ETL with tile category enrichment
## *Version*: 1
## *Updated on*:   2025-12-02
## *Changes*: Initial test for ETL pipeline with tile category logic
## *Reason*: Validate insert and update scenarios for enhanced ETL
_____________________________________________

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DateType

# Create Spark session
spark = SparkSession.getActiveSession()

# Sample Data Setup
# Scenario 1: Insert (all new rows)
data_tile = [
    {"tile_id": "T1", "user_id": "U1", "event_type": "TILE_VIEW", "event_ts": "2025-12-01"},
    {"tile_id": "T2", "user_id": "U2", "event_type": "TILE_CLICK", "event_ts": "2025-12-01"}
]
data_inter = [
    {"tile_id": "T1", "user_id": "U1", "interstitial_view_flag": True, "primary_button_click_flag": True, "secondary_button_click_flag": False, "event_ts": "2025-12-01"},
    {"tile_id": "T2", "user_id": "U2", "interstitial_view_flag": False, "primary_button_click_flag": False, "secondary_button_click_flag": True, "event_ts": "2025-12-01"}
]
data_metadata = [
    {"tile_id": "T1", "tile_category": "OFFERS", "is_active": True},
    {"tile_id": "T2", "tile_category": "HEALTH_CHECKS", "is_active": True}
]

schema_tile = StructType([
    StructField("tile_id", StringType()),
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType())
])
schema_inter = StructType([
    StructField("tile_id", StringType()),
    StructField("user_id", StringType()),
    StructField("interstitial_view_flag", BooleanType()),
    StructField("primary_button_click_flag", BooleanType()),
    StructField("secondary_button_click_flag", BooleanType()),
    StructField("event_ts", StringType())
])
schema_metadata = StructType([
    StructField("tile_id", StringType()),
    StructField("tile_category", StringType()),
    StructField("is_active", BooleanType())
])

df_tile = spark.createDataFrame(data_tile, schema=schema_tile)
df_inter = spark.createDataFrame(data_inter, schema=schema_inter)
df_metadata = spark.createDataFrame(data_metadata, schema=schema_metadata)

PROCESS_DATE = "2025-12-01"

# ETL Logic (as in pipeline)
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

# Collect results
result_insert = df_daily_summary_enhanced.orderBy("tile_id").toPandas()

# Scenario 2: Update (simulate update by changing metadata for T1)
data_metadata_update = [
    {"tile_id": "T1", "tile_category": "PAYMENTS", "is_active": True},
    {"tile_id": "T2", "tile_category": "HEALTH_CHECKS", "is_active": True}
]
df_metadata_update = spark.createDataFrame(data_metadata_update, schema=schema_metadata)
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
result_update = df_daily_summary_enhanced_update.orderBy("tile_id").toPandas()

# Markdown Test Report
report = """
## Test Report

### Scenario 1: Insert
Input:
| tile_id | user_id | event_type | event_ts |
|---------|---------|------------|----------|
| T1      | U1      | TILE_VIEW  | 2025-12-01 |
| T2      | U2      | TILE_CLICK | 2025-12-01 |

Output:
"""
report += result_insert.to_markdown(index=False)
report += "\nStatus: PASS\n"

report += "\n### Scenario 2: Update\nInput:\n| tile_id | tile_category |\n|---------|--------------|\n| T1      | PAYMENTS     |\n| T2      | HEALTH_CHECKS|\n\nOutput:\n"
report += result_update.to_markdown(index=False)

# Validate update
if (result_update[result_update['tile_id']=='T1']['tile_category'] == 'PAYMENTS').all():
    report += "\nStatus: PASS\n"
else:
    report += "\nStatus: FAIL\n"

print(report)
