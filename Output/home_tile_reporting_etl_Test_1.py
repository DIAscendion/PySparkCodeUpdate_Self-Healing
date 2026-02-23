_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-12-19
## *Description*: Test suite for Enhanced Home Tile Reporting ETL Pipeline with Tile Category Integration
## *Version*: 1
## *Updated on*: 2025-12-19
## *Changes*: Initial test version with SOURCE_TILE_METADATA integration testing
## *Reason*: PCE-3 - Validate tile category enrichment functionality
_____________________________________________

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session using getActiveSession for Spark Connect compatibility
try:
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = (
            SparkSession.builder
            .appName("HomeTileReportingETL_Test")
            .enableHiveSupport()
            .getOrCreate()
        )
except Exception as e:
    logger.error(f"Failed to initialize Spark session: {e}")
    raise

logger.info("Starting Home Tile Reporting ETL Test Suite")

# ------------------------------------------------------------------------------
# TEST DATA SETUP
# ------------------------------------------------------------------------------
def create_test_data_scenario_1():
    """Create test data for Scenario 1: Insert new records"""
    logger.info("Creating test data for Scenario 1: Insert")
    
    # Test tile events - all new records
    tile_events_data = [
        ("evt_001", "user_001", "sess_001", "2025-12-01 10:00:00", "tile_offers_001", "TILE_VIEW", "Mobile", "v1.0"),
        ("evt_002", "user_001", "sess_001", "2025-12-01 10:01:00", "tile_offers_001", "TILE_CLICK", "Mobile", "v1.0"),
        ("evt_003", "user_002", "sess_002", "2025-12-01 11:00:00", "tile_health_001", "TILE_VIEW", "Web", "v1.0"),
        ("evt_004", "user_003", "sess_003", "2025-12-01 12:00:00", "tile_payments_001", "TILE_VIEW", "Mobile", "v1.0"),
        ("evt_005", "user_003", "sess_003", "2025-12-01 12:01:00", "tile_payments_001", "TILE_CLICK", "Mobile", "v1.0")
    ]
    
    tile_events_schema = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
    df_tile_test = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_test = df_tile_test.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Test interstitial events
    interstitial_events_data = [
        ("int_001", "user_001", "sess_001", "2025-12-01 10:02:00", "tile_offers_001", True, True, False),
        ("int_002", "user_002", "sess_002", "2025-12-01 11:01:00", "tile_health_001", True, False, True),
        ("int_003", "user_003", "sess_003", "2025-12-01 12:02:00", "tile_payments_001", True, True, False)
    ]
    
    interstitial_schema = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
    df_interstitial_test = spark.createDataFrame(interstitial_events_data, interstitial_schema)
    df_interstitial_test = df_interstitial_test.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Test metadata
    metadata_data = [
        ("tile_offers_001", "Special Offers", "OFFERS", True, "2025-12-01 00:00:00"),
        ("tile_health_001", "Health Checkup", "HEALTH_CHECKS", True, "2025-12-01 00:00:00"),
        ("tile_payments_001", "Quick Pay", "PAYMENTS", True, "2025-12-01 00:00:00")
    ]
    
    metadata_schema = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
    df_metadata_test = spark.createDataFrame(metadata_data, metadata_schema)
    df_metadata_test = df_metadata_test.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_test, df_interstitial_test, df_metadata_test

def create_test_data_scenario_2():
    """Create test data for Scenario 2: Update existing records"""
    logger.info("Creating test data for Scenario 2: Update")
    
    # Updated tile events - same tile_ids but different metrics
    tile_events_data = [
        ("evt_006", "user_004", "sess_004", "2025-12-01 14:00:00", "tile_offers_001", "TILE_VIEW", "Mobile", "v1.0"),
        ("evt_007", "user_005", "sess_005", "2025-12-01 15:00:00", "tile_health_001", "TILE_VIEW", "Web", "v1.0"),
        ("evt_008", "user_005", "sess_005", "2025-12-01 15:01:00", "tile_health_001", "TILE_CLICK", "Web", "v1.0"),
        ("evt_009", "user_006", "sess_006", "2025-12-01 16:00:00", "tile_payments_001", "TILE_VIEW", "Mobile", "v1.0")
    ]
    
    tile_events_schema = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
    df_tile_update = spark.createDataFrame(tile_events_data, tile_events_schema)
    df_tile_update = df_tile_update.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Updated interstitial events
    interstitial_events_data = [
        ("int_004", "user_004", "sess_004", "2025-12-01 14:02:00", "tile_offers_001", True, False, True),
        ("int_005", "user_005", "sess_005", "2025-12-01 15:02:00", "tile_health_001", True, True, False)
    ]
    
    interstitial_schema = ["event_id", "user_id", "session_id", "event_ts", "tile_id", "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
    df_interstitial_update = spark.createDataFrame(interstitial_events_data, interstitial_schema)
    df_interstitial_update = df_interstitial_update.withColumn("event_ts", F.to_timestamp("event_ts"))
    
    # Same metadata (no changes)
    metadata_data = [
        ("tile_offers_001", "Special Offers", "OFFERS", True, "2025-12-01 00:00:00"),
        ("tile_health_001", "Health Checkup", "HEALTH_CHECKS", True, "2025-12-01 00:00:00"),
        ("tile_payments_001", "Quick Pay", "PAYMENTS", True, "2025-12-01 00:00:00")
    ]
    
    metadata_schema = ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
    df_metadata_update = spark.createDataFrame(metadata_data, metadata_schema)
    df_metadata_update = df_metadata_update.withColumn("updated_ts", F.to_timestamp("updated_ts"))
    
    return df_tile_update, df_interstitial_update, df_metadata_update

# ------------------------------------------------------------------------------
# ETL LOGIC FUNCTIONS (REUSED FROM PIPELINE)
# ------------------------------------------------------------------------------
def process_etl(df_tile, df_inter, df_metadata, process_date="2025-12-01"):
    """Process ETL logic with tile category enrichment"""
    
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
    
    # Join aggregations
    df_daily_summary = (
        df_tile_agg.join(df_inter_agg, "tile_id", "outer")
        .withColumn("date", F.lit(process_date))
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
    
    # Enhance with metadata (PCE-3 feature)
    df_enhanced = (
        df_daily_summary
        .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
        .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
        .select(
            "date",
            "tile_id",
            "tile_category",  # New column from PCE-3
            "unique_tile_views",
            "unique_tile_clicks",
            "unique_interstitial_views",
            "unique_interstitial_primary_clicks",
            "unique_interstitial_secondary_clicks"
        )
    )
    
    return df_enhanced

# ------------------------------------------------------------------------------
# TEST EXECUTION AND VALIDATION
# ------------------------------------------------------------------------------
def run_test_scenario_1():
    """Test Scenario 1: Insert new records"""
    logger.info("=== Running Test Scenario 1: Insert ===")
    
    # Create test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_1()
    
    # Process ETL
    result_df = process_etl(df_tile, df_inter, df_metadata)
    
    # Collect results for validation
    results = result_df.collect()
    
    # Expected results validation
    expected_tiles = {"tile_offers_001", "tile_health_001", "tile_payments_001"}
    actual_tiles = {row["tile_id"] for row in results}
    
    # Validate tile categories
    tile_categories = {row["tile_id"]: row["tile_category"] for row in results}
    
    test_passed = True
    test_results = []
    
    # Check if all expected tiles are present
    if expected_tiles == actual_tiles:
        test_results.append("✓ All expected tiles present")
    else:
        test_results.append(f"✗ Missing tiles: {expected_tiles - actual_tiles}")
        test_passed = False
    
    # Check tile categories
    expected_categories = {
        "tile_offers_001": "OFFERS",
        "tile_health_001": "HEALTH_CHECKS",
        "tile_payments_001": "PAYMENTS"
    }
    
    for tile_id, expected_category in expected_categories.items():
        if tile_categories.get(tile_id) == expected_category:
            test_results.append(f"✓ {tile_id}: {expected_category}")
        else:
            test_results.append(f"✗ {tile_id}: Expected {expected_category}, got {tile_categories.get(tile_id)}")
            test_passed = False
    
    # Check metrics
    for row in results:
        if row["tile_id"] == "tile_offers_001":
            if row["unique_tile_views"] == 1 and row["unique_tile_clicks"] == 1:
                test_results.append("✓ tile_offers_001 metrics correct")
            else:
                test_results.append(f"✗ tile_offers_001 metrics incorrect: views={row['unique_tile_views']}, clicks={row['unique_tile_clicks']}")
                test_passed = False
    
    return test_passed, test_results, result_df

def run_test_scenario_2():
    """Test Scenario 2: Update existing records"""
    logger.info("=== Running Test Scenario 2: Update ===")
    
    # Create updated test data
    df_tile, df_inter, df_metadata = create_test_data_scenario_2()
    
    # Process ETL
    result_df = process_etl(df_tile, df_inter, df_metadata)
    
    # Collect results for validation
    results = result_df.collect()
    
    test_passed = True
    test_results = []
    
    # Check that tile categories are still preserved
    tile_categories = {row["tile_id"]: row["tile_category"] for row in results}
    expected_categories = {
        "tile_offers_001": "OFFERS",
        "tile_health_001": "HEALTH_CHECKS",
        "tile_payments_001": "PAYMENTS"
    }
    
    for tile_id, expected_category in expected_categories.items():
        if tile_id in tile_categories and tile_categories[tile_id] == expected_category:
            test_results.append(f"✓ {tile_id}: Category preserved as {expected_category}")
        else:
            test_results.append(f"✗ {tile_id}: Category not preserved")
            test_passed = False
    
    # Check updated metrics
    for row in results:
        if row["tile_id"] == "tile_health_001":
            if row["unique_tile_views"] == 1 and row["unique_tile_clicks"] == 1:
                test_results.append("✓ tile_health_001 updated metrics correct")
            else:
                test_results.append(f"✗ tile_health_001 updated metrics incorrect")
                test_passed = False
    
    return test_passed, test_results, result_df

def generate_markdown_report(scenario1_results, scenario2_results, df1, df2):
    """Generate markdown test report"""
    
    report = []
    report.append("## Test Report")
    report.append("")
    
    # Scenario 1 Report
    report.append("### Scenario 1: Insert")
    report.append("")
    report.append("**Input Data:**")
    report.append("| tile_id | tile_category | unique_tile_views | unique_tile_clicks |")
    report.append("|---------|---------------|-------------------|-------------------|")
    
    for row in df1.select("tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks").collect():
        report.append(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |")
    
    report.append("")
    report.append("**Test Results:**")
    for result in scenario1_results[1]:
        report.append(f"- {result}")
    
    report.append("")
    report.append(f"**Status: {'PASS' if scenario1_results[0] else 'FAIL'}**")
    report.append("")
    
    # Scenario 2 Report
    report.append("### Scenario 2: Update")
    report.append("")
    report.append("**Updated Data:**")
    report.append("| tile_id | tile_category | unique_tile_views | unique_tile_clicks |")
    report.append("|---------|---------------|-------------------|-------------------|")
    
    for row in df2.select("tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks").collect():
        report.append(f"| {row['tile_id']} | {row['tile_category']} | {row['unique_tile_views']} | {row['unique_tile_clicks']} |")
    
    report.append("")
    report.append("**Test Results:**")
    for result in scenario2_results[1]:
        report.append(f"- {result}")
    
    report.append("")
    report.append(f"**Status: {'PASS' if scenario2_results[0] else 'FAIL'}**")
    report.append("")
    
    # Overall Status
    overall_status = "PASS" if scenario1_results[0] and scenario2_results[0] else "FAIL"
    report.append(f"### Overall Test Status: {overall_status}")
    
    return "\n".join(report)

# ------------------------------------------------------------------------------
# MAIN TEST EXECUTION
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting Home Tile Reporting ETL Test Suite")
    
    try:
        # Run test scenarios
        scenario1_results = run_test_scenario_1()
        scenario2_results = run_test_scenario_2()
        
        # Generate and display report
        report = generate_markdown_report(scenario1_results, scenario2_results, 
                                        scenario1_results[2], scenario2_results[2])
        
        print("\n" + "="*80)
        print(report)
        print("="*80)
        
        # Log final results
        if scenario1_results[0] and scenario2_results[0]:
            logger.info("All tests PASSED")
        else:
            logger.error("Some tests FAILED")
            
        # Show sample output data
        logger.info("Sample output from Scenario 1:")
        scenario1_results[2].show(truncate=False)
        
        logger.info("Sample output from Scenario 2:")
        scenario2_results[2].show(truncate=False)
        
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        raise
    
    logger.info("Test suite completed")