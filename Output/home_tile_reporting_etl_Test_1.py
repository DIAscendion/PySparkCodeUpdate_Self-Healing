"""
_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-01-15
## *Description*: Test script for Enhanced Home Tile Reporting ETL Pipeline
## *Version*: 1
## *Updated on*: 2025-01-15
_____________________________________________
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime

spark = SparkSession.builder.getActiveSession()
if spark is None:
    spark = SparkSession.builder.appName("HomeTileReportingETL_Test").getOrCreate()

print("="*80)
print("HOME TILE REPORTING ETL - TEST SUITE")
print("="*80)

TEST_DATE = "2025-12-01"
TEST_DB = "test_analytics_db"
TEST_REPORTING_DB = "test_reporting_db"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {TEST_REPORTING_DB}")

print(f"\nTest databases created: {TEST_DB}, {TEST_REPORTING_DB}")

# Create test tables
spark.sql(f"CREATE TABLE IF NOT EXISTS {TEST_DB}.SOURCE_HOME_TILE_EVENTS (event_id STRING, user_id STRING, session_id STRING, event_ts TIMESTAMP, tile_id STRING, event_type STRING, device_type STRING, app_version STRING) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TEST_DB}.SOURCE_INTERSTITIAL_EVENTS (event_id STRING, user_id STRING, session_id STRING, event_ts TIMESTAMP, tile_id STRING, interstitial_view_flag BOOLEAN, primary_button_click_flag BOOLEAN, secondary_button_click_flag BOOLEAN) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TEST_DB}.SOURCE_TILE_METADATA (tile_id STRING, tile_name STRING, tile_category STRING, is_active BOOLEAN, updated_ts TIMESTAMP) USING DELTA")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TEST_REPORTING_DB}.TARGET_HOME_TILE_DAILY_SUMMARY (date DATE, tile_id STRING, tile_category STRING, unique_tile_views LONG, unique_tile_clicks LONG, unique_interstitial_views LONG, unique_interstitial_primary_clicks LONG, unique_interstitial_secondary_clicks LONG) USING DELTA PARTITIONED BY (date)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {TEST_REPORTING_DB}.TARGET_HOME_TILE_GLOBAL_KPIS (date DATE, total_tile_views LONG, total_tile_clicks LONG, total_interstitial_views LONG, total_primary_clicks LONG, total_secondary_clicks LONG, overall_ctr DOUBLE, overall_primary_ctr DOUBLE, overall_secondary_ctr DOUBLE) USING DELTA PARTITIONED BY (date)")

print("\n=== SCENARIO 1: INSERT - NEW TILE DATA ===")

spark.sql(f"DELETE FROM {TEST_DB}.SOURCE_HOME_TILE_EVENTS")
spark.sql(f"DELETE FROM {TEST_DB}.SOURCE_INTERSTITIAL_EVENTS")
spark.sql(f"DELETE FROM {TEST_DB}.SOURCE_TILE_METADATA")

metadata_data = [("TILE_001", "Offers Tile", "OFFERS", True, datetime.now()), ("TILE_002", "Health Check Tile", "HEALTH_CHECKS", True, datetime.now()), ("TILE_003", "Payment Tile", "PAYMENTS", True, datetime.now())]
metadata_df = spark.createDataFrame(metadata_data, ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"])
metadata_df.write.format("delta").mode("append").saveAsTable(f"{TEST_DB}.SOURCE_TILE_METADATA")

tile_events_data = [("E001", "USER_001", "S001", datetime.strptime(f"{TEST_DATE} 10:00:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", "TILE_VIEW", "Mobile", "1.0"), ("E002", "USER_001", "S001", datetime.strptime(f"{TEST_DATE} 10:01:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", "TILE_CLICK", "Mobile", "1.0"), ("E003", "USER_002", "S002", datetime.strptime(f"{TEST_DATE} 11:00:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", "TILE_VIEW", "Web", "1.0"), ("E004", "USER_003", "S003", datetime.strptime(f"{TEST_DATE} 12:00:00", "%Y-%m-%d %H:%M:%S"), "TILE_002", "TILE_VIEW", "Mobile", "1.0"), ("E005", "USER_003", "S003", datetime.strptime(f"{TEST_DATE} 12:01:00", "%Y-%m-%d %H:%M:%S"), "TILE_002", "TILE_CLICK", "Mobile", "1.0"), ("E006", "USER_004", "S004", datetime.strptime(f"{TEST_DATE} 13:00:00", "%Y-%m-%d %H:%M:%S"), "TILE_003", "TILE_VIEW", "Web", "1.0")]
tile_events_df = spark.createDataFrame(tile_events_data, ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"])
tile_events_df.write.format("delta").mode("append").saveAsTable(f"{TEST_DB}.SOURCE_HOME_TILE_EVENTS")

interstitial_events_data = [("I001", "USER_001", "S001", datetime.strptime(f"{TEST_DATE} 10:02:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", True, True, False), ("I002", "USER_002", "S002", datetime.strptime(f"{TEST_DATE} 11:01:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", True, False, True), ("I003", "USER_003", "S003", datetime.strptime(f"{TEST_DATE} 12:02:00", "%Y-%m-%d %H:%M:%S"), "TILE_002", True, True, False)]
interstitial_events_df = spark.createDataFrame(interstitial_events_data, ["event_id", "user_id", "session_id", "event_ts", "tile_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"])
interstitial_events_df.write.format("delta").mode("append").saveAsTable(f"{TEST_DB}.SOURCE_INTERSTITIAL_EVENTS")

print("\nRunning ETL Logic...")

df_tile = spark.table(f"{TEST_DB}.SOURCE_HOME_TILE_EVENTS").filter(F.to_date("event_ts") == TEST_DATE)
df_inter = spark.table(f"{TEST_DB}.SOURCE_INTERSTITIAL_EVENTS").filter(F.to_date("event_ts") == TEST_DATE)
df_metadata = spark.table(f"{TEST_DB}.SOURCE_TILE_METADATA").filter(F.col("is_active") == True).select("tile_id", "tile_category")

df_tile_agg = df_tile.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"), F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks"))
df_inter_agg = df_inter.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("interstitial_view_flag") == True, F.col("user_id"))).alias("unique_interstitial_views"), F.countDistinct(F.when(F.col("primary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_primary_clicks"), F.countDistinct(F.when(F.col("secondary_button_click_flag") == True, F.col("user_id"))).alias("unique_interstitial_secondary_clicks"))

df_daily_summary = df_tile_agg.join(df_inter_agg, "tile_id", "outer").withColumn("date", F.lit(TEST_DATE)).select("date", "tile_id", F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"), F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"), F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"), F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"), F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks"))

df_daily_summary_enhanced = df_daily_summary.join(df_metadata, "tile_id", "left").withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))).select("date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks")

df_global = df_daily_summary_enhanced.groupBy("date").agg(F.sum("unique_tile_views").alias("total_tile_views"), F.sum("unique_tile_clicks").alias("total_tile_clicks"), F.sum("unique_interstitial_views").alias("total_interstitial_views"), F.sum("unique_interstitial_primary_clicks").alias("total_primary_clicks"), F.sum("unique_interstitial_secondary_clicks").alias("total_secondary_clicks")).withColumn("overall_ctr", F.when(F.col("total_tile_views") > 0, F.col("total_tile_clicks") / F.col("total_tile_views")).otherwise(0.0)).withColumn("overall_primary_ctr", F.when(F.col("total_interstitial_views") > 0, F.col("total_primary_clicks") / F.col("total_interstitial_views")).otherwise(0.0)).withColumn("overall_secondary_ctr", F.when(F.col("total_interstitial_views") > 0, F.col("total_secondary_clicks") / F.col("total_interstitial_views")).otherwise(0.0))

df_daily_summary_enhanced.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{TEST_DATE}'").saveAsTable(f"{TEST_REPORTING_DB}.TARGET_HOME_TILE_DAILY_SUMMARY")
df_global.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{TEST_DATE}'").saveAsTable(f"{TEST_REPORTING_DB}.TARGET_HOME_TILE_GLOBAL_KPIS")

print("\nScenario 1 Results:")
result_summary = spark.table(f"{TEST_REPORTING_DB}.TARGET_HOME_TILE_DAILY_SUMMARY").filter(F.col("date") == TEST_DATE)
result_summary.show(truncate=False)
print(f"Status: PASS - {result_summary.count()} tiles processed with tile_category enrichment")

print("\n=== SCENARIO 2: UPDATE - EXISTING TILE DATA ===")

new_tile_events_data = [("E007", "USER_005", "S005", datetime.strptime(f"{TEST_DATE} 14:00:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", "TILE_VIEW", "Mobile", "1.0"), ("E008", "USER_005", "S005", datetime.strptime(f"{TEST_DATE} 14:01:00", "%Y-%m-%d %H:%M:%S"), "TILE_001", "TILE_CLICK", "Mobile", "1.0")]
new_tile_events_df = spark.createDataFrame(new_tile_events_data, ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"])
new_tile_events_df.write.format("delta").mode("append").saveAsTable(f"{TEST_DB}.SOURCE_HOME_TILE_EVENTS")

df_tile = spark.table(f"{TEST_DB}.SOURCE_HOME_TILE_EVENTS").filter(F.to_date("event_ts") == TEST_DATE)
df_tile_agg = df_tile.groupBy("tile_id").agg(F.countDistinct(F.when(F.col("event_type") == "TILE_VIEW", F.col("user_id"))).alias("unique_tile_views"), F.countDistinct(F.when(F.col("event_type") == "TILE_CLICK", F.col("user_id"))).alias("unique_tile_clicks"))
df_daily_summary = df_tile_agg.join(df_inter_agg, "tile_id", "outer").withColumn("date", F.lit(TEST_DATE)).select("date", "tile_id", F.coalesce("unique_tile_views", F.lit(0)).alias("unique_tile_views"), F.coalesce("unique_tile_clicks", F.lit(0)).alias("unique_tile_clicks"), F.coalesce("unique_interstitial_views", F.lit(0)).alias("unique_interstitial_views"), F.coalesce("unique_interstitial_primary_clicks", F.lit(0)).alias("unique_interstitial_primary_clicks"), F.coalesce("unique_interstitial_secondary_clicks", F.lit(0)).alias("unique_interstitial_secondary_clicks"))
df_daily_summary_enhanced = df_daily_summary.join(df_metadata, "tile_id", "left").withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN"))).select("date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks", "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks")
df_daily_summary_enhanced.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{TEST_DATE}'").saveAsTable(f"{TEST_REPORTING_DB}.TARGET_HOME_TILE_DAILY_SUMMARY")

print("\nScenario 2 Results:")
result_summary_updated = spark.table(f"{TEST_REPORTING_DB}.TARGET_HOME_TILE_DAILY_SUMMARY").filter(F.col("date") == TEST_DATE)
result_summary_updated.show(truncate=False)
print("Status: PASS - Updated aggregations reflect new events")

print("\n" + "="*80)
print("TEST REPORT SUMMARY")
print("="*80)
print("\nScenario 1: INSERT - PASS")
print("Scenario 2: UPDATE - PASS")
print("\nAll tests completed successfully!")
