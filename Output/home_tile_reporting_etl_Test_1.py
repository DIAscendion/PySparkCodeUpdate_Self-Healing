"""
_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-12-03
## *Description*: Test script for Enhanced Home Tile Reporting ETL with Tile Category Integration
## *Version*: 1
## *Updated on*: 2025-12-03
## *Changes*: Initial test version for Pipeline_1
## *Reason*: Validate Pipeline logic with sample data for Insert and Update scenarios
_____________________________________________
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DateType, LongType
from datetime import datetime, date
import sys

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

print("="*80)
print("HOME TILE REPORTING ETL - TEST SUITE")
print("Version: 1.0.0")
print("="*80)

# ------------------------------------------------------------------------------
# TEST CONFIGURATION
# ------------------------------------------------------------------------------
TEST_PROCESS_DATE = "2025-12-01"
test_results = []

# ------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ------------------------------------------------------------------------------
def create_test_databases():
    """Create test databases if they don't exist"""
    spark.sql("CREATE DATABASE IF NOT EXISTS analytics_db")
    spark.sql("CREATE DATABASE IF NOT EXISTS reporting_db")
    print("✅ Test databases created")

def cleanup_test_tables():
    """Drop test tables to ensure clean state"""
    tables = [
        "analytics_db.SOURCE_HOME_TILE_EVENTS",
        "analytics_db.SOURCE_INTERSTITIAL_EVENTS",
        "analytics_db.SOURCE_TILE_METADATA",
        "reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY",
        "reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS"
    ]
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    print("✅ Test tables cleaned up")

def log_test_result(scenario, status, details=""):
    """Log test result"""
    result = {
        "scenario": scenario,
        "status": status,
        "details": details
    }
    test_results.append(result)
    status_icon = "✅" if status == "PASS" else "❌"
    print(f"{status_icon} {scenario}: {status}")
    if details:
        print(f"   Details: {details}")

# ------------------------------------------------------------------------------
# CREATE SOURCE TABLES
# ------------------------------------------------------------------------------
def create_source_tables():
    """Create source tables with proper schema"""
    
    # SOURCE_HOME_TILE_EVENTS
    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_HOME_TILE_EVENTS (
            event_id STRING,
            user_id STRING,
            session_id STRING,
            event_ts TIMESTAMP,
            tile_id STRING,
            event_type STRING,
            device_type STRING,
            app_version STRING
        )
        USING DELTA
        PARTITIONED BY (date(event_ts))
    """)
    
    # SOURCE_INTERSTITIAL_EVENTS
    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_INTERSTITIAL_EVENTS (
            event_id STRING,
            user_id STRING,
            session_id STRING,
            event_ts TIMESTAMP,
            tile_id STRING,
            interstitial_view_flag BOOLEAN,
            primary_button_click_flag BOOLEAN,
            secondary_button_click_flag BOOLEAN
        )
        USING DELTA
        PARTITIONED BY (date(event_ts))
    """)
    
    # SOURCE_TILE_METADATA
    spark.sql("""
        CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA (
            tile_id STRING,
            tile_name STRING,
            tile_category STRING,
            is_active BOOLEAN,
            updated_ts TIMESTAMP
        )
        USING DELTA
    """)
    
    print("✅ Source tables created")

def create_target_tables():
    """Create target tables with proper schema"""
    
    # TARGET_HOME_TILE_DAILY_SUMMARY (with tile_category)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY (
            date DATE,
            tile_id STRING,
            tile_category STRING,
            unique_tile_views LONG,
            unique_tile_clicks LONG,
            unique_interstitial_views LONG,
            unique_interstitial_primary_clicks LONG,
            unique_interstitial_secondary_clicks LONG
        )
        USING DELTA
        PARTITIONED BY (date)
    """)
    
    # TARGET_HOME_TILE_GLOBAL_KPIS
    spark.sql("""
        CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_GLOBAL_KPIS (
            date DATE,
            total_tile_views LONG,
            total_tile_clicks LONG,
            total_interstitial_views LONG,
            total_primary_clicks LONG,
            total_secondary_clicks LONG,
            overall_ctr DOUBLE,
            overall_primary_ctr DOUBLE,
            overall_secondary_ctr DOUBLE
        )
        USING DELTA
        PARTITIONED BY (date)
    """)
    
    print("✅ Target tables created")

# ------------------------------------------------------------------------------
# SCENARIO 1: INSERT - New Records
# ------------------------------------------------------------------------------
def test_scenario_1_insert():
    """Test Scenario 1: Insert new records with tile category enrichment"""
    print("\n" + "="*80)
    print("TEST SCENARIO 1: INSERT - New Records with Tile Category")
    print("="*80)
    
    try:
        # Create sample tile metadata
        metadata_data = [
            ("TILE_001", "Health Check Tile", "HEALTH_CHECKS", True, datetime(2025, 12, 1, 0, 0, 0)),
            ("TILE_002", "Payment Tile", "PAYMENTS", True, datetime(2025, 12, 1, 0, 0, 0)),
            ("TILE_003", "Offers Tile", "OFFERS", True, datetime(2025, 12, 1, 0, 0, 0))
        ]
        
        df_metadata = spark.createDataFrame(
            metadata_data,
            ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        )
        df_metadata.write.format("delta").mode("overwrite").saveAsTable("analytics_db.SOURCE_TILE_METADATA")
        
        # Create sample tile events
        tile_events_data = [
            ("E001", "U001", "S001", datetime(2025, 12, 1, 10, 0, 0), "TILE_001", "TILE_VIEW", "Mobile", "1.0"),
            ("E002", "U001", "S001", datetime(2025, 12, 1, 10, 1, 0), "TILE_001", "TILE_CLICK", "Mobile", "1.0"),
            ("E003", "U002", "S002", datetime(2025, 12, 1, 10, 2, 0), "TILE_002", "TILE_VIEW", "Web", "1.0"),
            ("E004", "U003", "S003", datetime(2025, 12, 1, 10, 3, 0), "TILE_003", "TILE_VIEW", "Mobile", "1.0"),
            ("E005", "U003", "S003", datetime(2025, 12, 1, 10, 4, 0), "TILE_003", "TILE_CLICK", "Mobile", "1.0")
        ]
        
        df_tile_events = spark.createDataFrame(
            tile_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
        )
        df_tile_events.write.format("delta").mode("overwrite").saveAsTable("analytics_db.SOURCE_HOME_TILE_EVENTS")
        
        # Create sample interstitial events
        interstitial_events_data = [
            ("I001", "U001", "S001", datetime(2025, 12, 1, 10, 1, 30), "TILE_001", True, True, False),
            ("I002", "U002", "S002", datetime(2025, 12, 1, 10, 2, 30), "TILE_002", True, False, True),
            ("I003", "U003", "S003", datetime(2025, 12, 1, 10, 4, 30), "TILE_003", True, True, False)
        ]
        
        df_interstitial_events = spark.createDataFrame(
            interstitial_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
        )
        df_interstitial_events.write.format("delta").mode("overwrite").saveAsTable("analytics_db.SOURCE_INTERSTITIAL_EVENTS")
        
        print("\n📊 Input Data:")
        print("\nTile Metadata:")
        df_metadata.show(truncate=False)
        
        print("\nTile Events:")
        df_tile_events.show(truncate=False)
        
        print("\nInterstitial Events:")
        df_interstitial_events.show(truncate=False)
        
        # Run the ETL logic (inline)
        PROCESS_DATE = TEST_PROCESS_DATE
        
        df_tile = spark.table("analytics_db.SOURCE_HOME_TILE_EVENTS").filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = spark.table("analytics_db.SOURCE_INTERSTITIAL_EVENTS").filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata_active = spark.table("analytics_db.SOURCE_TILE_METADATA").filter(F.col("is_active") == True).select("tile_id", "tile_category", "tile_name")
        
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
        
        # Enhanced with tile_category
        df_daily_summary_enhanced = (
            df_daily_summary
            .join(df_metadata_active.select("tile_id", "tile_category"), "tile_id", "left")
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
        
        # Write to target
        df_daily_summary_enhanced.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{PROCESS_DATE}'").saveAsTable("reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY")
        
        # Validate output
        print("\n📊 Output Data:")
        df_output = spark.table("reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY")
        df_output.show(truncate=False)
        
        # Validation checks
        output_count = df_output.count()
        expected_count = 3  # 3 tiles
        
        if output_count == expected_count:
            log_test_result("Scenario 1: Record Count", "PASS", f"Expected {expected_count}, Got {output_count}")
        else:
            log_test_result("Scenario 1: Record Count", "FAIL", f"Expected {expected_count}, Got {output_count}")
        
        # Check tile_category is populated
        category_check = df_output.filter(F.col("tile_category").isNotNull()).count()
        if category_check == expected_count:
            log_test_result("Scenario 1: Tile Category Populated", "PASS", f"All {category_check} records have tile_category")
        else:
            log_test_result("Scenario 1: Tile Category Populated", "FAIL", f"Only {category_check}/{expected_count} records have tile_category")
        
        # Check specific tile categories
        tile_001_category = df_output.filter(F.col("tile_id") == "TILE_001").select("tile_category").first()[0]
        if tile_001_category == "HEALTH_CHECKS":
            log_test_result("Scenario 1: TILE_001 Category", "PASS", f"Category is {tile_001_category}")
        else:
            log_test_result("Scenario 1: TILE_001 Category", "FAIL", f"Expected HEALTH_CHECKS, Got {tile_001_category}")
        
        # Check metrics
        tile_001_data = df_output.filter(F.col("tile_id") == "TILE_001").first()
        if tile_001_data["unique_tile_views"] == 1 and tile_001_data["unique_tile_clicks"] == 1:
            log_test_result("Scenario 1: TILE_001 Metrics", "PASS", "Views=1, Clicks=1")
        else:
            log_test_result("Scenario 1: TILE_001 Metrics", "FAIL", f"Views={tile_001_data['unique_tile_views']}, Clicks={tile_001_data['unique_tile_clicks']}")
        
        print("\n✅ Scenario 1 completed successfully")
        
    except Exception as e:
        log_test_result("Scenario 1: Execution", "FAIL", str(e))
        print(f"\n❌ Scenario 1 failed with error: {str(e)}")
        import traceback
        traceback.print_exc()

# ------------------------------------------------------------------------------
# SCENARIO 2: UPDATE - Existing Records
# ------------------------------------------------------------------------------
def test_scenario_2_update():
    """Test Scenario 2: Update existing records with new data"""
    print("\n" + "="*80)
    print("TEST SCENARIO 2: UPDATE - Existing Records")
    print("="*80)
    
    try:
        # Add new events for the same tiles (simulating update scenario)
        new_tile_events_data = [
            ("E006", "U004", "S004", datetime(2025, 12, 1, 11, 0, 0), "TILE_001", "TILE_VIEW", "Web", "1.0"),
            ("E007", "U005", "S005", datetime(2025, 12, 1, 11, 1, 0), "TILE_002", "TILE_VIEW", "Mobile", "1.0"),
            ("E008", "U005", "S005", datetime(2025, 12, 1, 11, 2, 0), "TILE_002", "TILE_CLICK", "Mobile", "1.0")
        ]
        
        df_new_tile_events = spark.createDataFrame(
            new_tile_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
        )
        
        # Append new events
        df_new_tile_events.write.format("delta").mode("append").saveAsTable("analytics_db.SOURCE_HOME_TILE_EVENTS")
        
        print("\n📊 New Input Data (Appended):")
        df_new_tile_events.show(truncate=False)
        
        # Re-run ETL logic
        PROCESS_DATE = TEST_PROCESS_DATE
        
        df_tile = spark.table("analytics_db.SOURCE_HOME_TILE_EVENTS").filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = spark.table("analytics_db.SOURCE_INTERSTITIAL_EVENTS").filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata_active = spark.table("analytics_db.SOURCE_TILE_METADATA").filter(F.col("is_active") == True).select("tile_id", "tile_category", "tile_name")
        
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
        
        # Enhanced with tile_category
        df_daily_summary_enhanced = (
            df_daily_summary
            .join(df_metadata_active.select("tile_id", "tile_category"), "tile_id", "left")
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
        
        # Write to target (overwrite partition)
        df_daily_summary_enhanced.write.format("delta").mode("overwrite").option("replaceWhere", f"date = '{PROCESS_DATE}'").saveAsTable("reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY")
        
        # Validate output
        print("\n📊 Updated Output Data:")
        df_output = spark.table("reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY")
        df_output.show(truncate=False)
        
        # Validation checks
        tile_001_data = df_output.filter(F.col("tile_id") == "TILE_001").first()
        if tile_001_data["unique_tile_views"] == 2:  # U001 + U004
            log_test_result("Scenario 2: TILE_001 Updated Views", "PASS", f"Views updated to 2")
        else:
            log_test_result("Scenario 2: TILE_001 Updated Views", "FAIL", f"Expected 2, Got {tile_001_data['unique_tile_views']}")
        
        tile_002_data = df_output.filter(F.col("tile_id") == "TILE_002").first()
        if tile_002_data["unique_tile_views"] == 2 and tile_002_data["unique_tile_clicks"] == 1:  # U002 + U005 views, U005 click
            log_test_result("Scenario 2: TILE_002 Updated Metrics", "PASS", "Views=2, Clicks=1")
        else:
            log_test_result("Scenario 2: TILE_002 Updated Metrics", "FAIL", f"Views={tile_002_data['unique_tile_views']}, Clicks={tile_002_data['unique_tile_clicks']}")
        
        # Check tile_category is still preserved
        if tile_001_data["tile_category"] == "HEALTH_CHECKS" and tile_002_data["tile_category"] == "PAYMENTS":
            log_test_result("Scenario 2: Tile Categories Preserved", "PASS", "Categories maintained after update")
        else:
            log_test_result("Scenario 2: Tile Categories Preserved", "FAIL", "Categories changed after update")
        
        print("\n✅ Scenario 2 completed successfully")
        
    except Exception as e:
        log_test_result("Scenario 2: Execution", "FAIL", str(e))
        print(f"\n❌ Scenario 2 failed with error: {str(e)}")
        import traceback
        traceback.print_exc()

# ------------------------------------------------------------------------------
# GENERATE TEST REPORT
# ------------------------------------------------------------------------------
def generate_test_report():
    """Generate markdown test report"""
    print("\n" + "="*80)
    print("TEST REPORT SUMMARY")
    print("="*80)
    
    report = "\n## Test Report\n\n"
    report += f"**Test Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    report += f"**Pipeline Version**: 1.0.0\n\n"
    report += f"**Process Date**: {TEST_PROCESS_DATE}\n\n"
    
    # Summary table
    report += "### Test Results Summary\n\n"
    report += "| Scenario | Status | Details |\n"
    report += "|----------|--------|---------|\n"
    
    pass_count = 0
    fail_count = 0
    
    for result in test_results:
        status_icon = "✅" if result["status"] == "PASS" else "❌"
        report += f"| {result['scenario']} | {status_icon} {result['status']} | {result['details']} |\n"
        if result["status"] == "PASS":
            pass_count += 1
        else:
            fail_count += 1
    
    report += f"\n**Total Tests**: {len(test_results)}\n"
    report += f"**Passed**: {pass_count}\n"
    report += f"**Failed**: {fail_count}\n\n"
    
    if fail_count == 0:
        report += "### ✅ All tests passed successfully!\n"
    else:
        report += f"### ⚠️ {fail_count} test(s) failed. Please review the details above.\n"
    
    print(report)
    return report

# ------------------------------------------------------------------------------
# MAIN TEST EXECUTION
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        print("\n🚀 Starting test execution...\n")
        
        # Setup
        create_test_databases()
        cleanup_test_tables()
        create_source_tables()
        create_target_tables()
        
        # Run test scenarios
        test_scenario_1_insert()
        test_scenario_2_update()
        
        # Generate report
        report = generate_test_report()
        
        print("\n" + "="*80)
        print("TEST EXECUTION COMPLETED")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Test execution failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
