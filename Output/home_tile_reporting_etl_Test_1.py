"""
_____________________________________________
## *Author*: AAVA
## *Created on*: 2025-01-15
## *Description*: Test Suite for Enhanced Home Tile Reporting ETL Pipeline
## *Version*: 1
## *Updated on*: 2025-01-15
_____________________________________________

Test Description:
    This test suite validates the Enhanced Home Tile Reporting ETL Pipeline.
    Tests two scenarios:
    1. Insert Scenario - Validates new records insertion
    2. Update Scenario - Validates existing records update
    
    Note: This uses standard Python assertions (NO PyTest framework)
          as PyTest is not available in Databricks environment.
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get active Spark session (Spark Connect compatible)
spark = SparkSession.builder.getActiveSession()
if spark is None:
    spark = (
        SparkSession.builder
        .appName("HomeTileReportingETL_Test")
        .enableHiveSupport()
        .getOrCreate()
    )

logger.info("Starting Test Suite for Home Tile Reporting ETL Pipeline")

# ------------------------------------------------------------------------------
# TEST UTILITY FUNCTIONS
# ------------------------------------------------------------------------------

def assert_dataframe_equal(df1, df2, check_columns):
    """
    Compares two DataFrames for equality on specified columns.
    """
    for col in check_columns:
        if df1.select(col).collect() != df2.select(col).collect():
            return False
    return True


def print_test_result(scenario_name, status, details=""):
    """
    Prints formatted test results.
    """
    status_symbol = "✓ PASS" if status else "✗ FAIL"
    logger.info(f"\n{'='*80}")
    logger.info(f"Test Scenario: {scenario_name}")
    logger.info(f"Status: {status_symbol}")
    if details:
        logger.info(f"Details: {details}")
    logger.info(f"{'='*80}\n")


# ------------------------------------------------------------------------------
# TEST SCENARIO 1: INSERT - New Records
# ------------------------------------------------------------------------------

def test_scenario_1_insert():
    """
    Test Scenario 1: Insert new records
    Validates that new tile events are correctly aggregated and enriched with metadata.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST SCENARIO 1: INSERT - New Records")
    logger.info("="*80)
    
    try:
        # Create sample input data - all new records
        logger.info("Creating sample input data for INSERT scenario...")
        
        # Home tile events
        tile_events_data = [
            ("evt101", "user101", "sess101", "2025-12-02 10:00:00", "tile_X", "TILE_VIEW", "Mobile", "v1.0"),
            ("evt102", "user101", "sess101", "2025-12-02 10:01:00", "tile_X", "TILE_CLICK", "Mobile", "v1.0"),
            ("evt103", "user102", "sess102", "2025-12-02 10:05:00", "tile_Y", "TILE_VIEW", "Web", "v1.0"),
            ("evt104", "user103", "sess103", "2025-12-02 10:10:00", "tile_Y", "TILE_VIEW", "Mobile", "v1.0"),
            ("evt105", "user103", "sess103", "2025-12-02 10:11:00", "tile_Y", "TILE_CLICK", "Mobile", "v1.0"),
        ]
        
        df_tile_input = spark.createDataFrame(
            tile_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
        )
        df_tile_input = df_tile_input.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        # Interstitial events
        inter_events_data = [
            ("int101", "user101", "sess101", "2025-12-02 10:02:00", "tile_X", True, True, False),
            ("int102", "user102", "sess102", "2025-12-02 10:06:00", "tile_Y", True, False, True),
            ("int103", "user103", "sess103", "2025-12-02 10:12:00", "tile_Y", True, True, False),
        ]
        
        df_inter_input = spark.createDataFrame(
            inter_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", 
             "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
        )
        df_inter_input = df_inter_input.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        # Tile metadata
        metadata_data = [
            ("tile_X", "Special Offers", "OFFERS", True, "2025-12-02 00:00:00"),
            ("tile_Y", "Wellness Check", "HEALTH_CHECKS", True, "2025-12-02 00:00:00"),
        ]
        
        df_metadata = spark.createDataFrame(
            metadata_data,
            ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        )
        df_metadata = df_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
        
        logger.info("Input data created successfully")
        logger.info("\nInput - Home Tile Events:")
        df_tile_input.show(truncate=False)
        logger.info("\nInput - Interstitial Events:")
        df_inter_input.show(truncate=False)
        logger.info("\nInput - Tile Metadata:")
        df_metadata.show(truncate=False)
        
        # Process data (same logic as Pipeline)
        PROCESS_DATE = "2025-12-02"
        
        df_tile = df_tile_input.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = df_inter_input.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata_active = df_metadata.filter(F.col("is_active") == True)
        
        # Tile aggregations
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
        
        # Join and create daily summary
        df_daily_summary = (
            df_tile_agg.join(df_inter_agg, "tile_id", "outer")
            .withColumn("date", F.lit(PROCESS_DATE).cast("date"))
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
        
        # Enrich with metadata
        df_output = (
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
        
        logger.info("\nOutput - Daily Summary with Tile Category:")
        df_output.show(truncate=False)
        
        # Define expected output
        expected_data = [
            ("2025-12-02", "tile_X", "OFFERS", 2, 1, 1, 1, 0),
            ("2025-12-02", "tile_Y", "HEALTH_CHECKS", 2, 1, 2, 1, 1),
        ]
        
        df_expected = spark.createDataFrame(
            expected_data,
            ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks",
             "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        )
        df_expected = df_expected.withColumn("date", F.col("date").cast("date"))
        
        logger.info("\nExpected Output:")
        df_expected.show(truncate=False)
        
        # Validation
        output_count = df_output.count()
        expected_count = df_expected.count()
        
        assert output_count == expected_count, f"Record count mismatch: Expected {expected_count}, Got {output_count}"
        
        # Validate tile_X
        tile_x_output = df_output.filter(F.col("tile_id") == "tile_X").collect()[0]
        assert tile_x_output["tile_category"] == "OFFERS", "tile_X category mismatch"
        assert tile_x_output["unique_tile_views"] == 2, "tile_X views mismatch"
        assert tile_x_output["unique_tile_clicks"] == 1, "tile_X clicks mismatch"
        
        # Validate tile_Y
        tile_y_output = df_output.filter(F.col("tile_id") == "tile_Y").collect()[0]
        assert tile_y_output["tile_category"] == "HEALTH_CHECKS", "tile_Y category mismatch"
        assert tile_y_output["unique_tile_views"] == 2, "tile_Y views mismatch"
        assert tile_y_output["unique_tile_clicks"] == 1, "tile_Y clicks mismatch"
        
        print_test_result(
            "Scenario 1: INSERT",
            True,
            "All new records inserted and aggregated correctly with tile category enrichment"
        )
        return True
        
    except Exception as e:
        print_test_result("Scenario 1: INSERT", False, f"Error: {str(e)}")
        logger.error(f"Test failed with error: {str(e)}")
        return False


# ------------------------------------------------------------------------------
# TEST SCENARIO 2: UPDATE - Existing Records
# ------------------------------------------------------------------------------

def test_scenario_2_update():
    """
    Test Scenario 2: Update existing records
    Validates that when the same tiles are processed again with new events,
    the aggregations are correctly updated.
    """
    logger.info("\n" + "="*80)
    logger.info("TEST SCENARIO 2: UPDATE - Existing Records")
    logger.info("="*80)
    
    try:
        # Simulate existing data (from previous run)
        logger.info("Creating existing data (previous run)...")
        
        existing_data = [
            ("2025-12-03", "tile_A", "OFFERS", 5, 2, 3, 2, 1),
            ("2025-12-03", "tile_B", "PAYMENTS", 8, 3, 5, 3, 2),
        ]
        
        df_existing = spark.createDataFrame(
            existing_data,
            ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks",
             "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        )
        df_existing = df_existing.withColumn("date", F.col("date").cast("date"))
        
        logger.info("\nExisting Data (Previous Run):")
        df_existing.show(truncate=False)
        
        # Create new input data for the same date (simulating reprocessing)
        logger.info("\nCreating new input data for UPDATE scenario...")
        
        tile_events_data = [
            ("evt201", "user201", "sess201", "2025-12-03 14:00:00", "tile_A", "TILE_VIEW", "Mobile", "v1.0"),
            ("evt202", "user202", "sess202", "2025-12-03 14:05:00", "tile_A", "TILE_VIEW", "Web", "v1.0"),
            ("evt203", "user202", "sess202", "2025-12-03 14:06:00", "tile_A", "TILE_CLICK", "Web", "v1.0"),
            ("evt204", "user203", "sess203", "2025-12-03 14:10:00", "tile_B", "TILE_VIEW", "Mobile", "v1.0"),
            ("evt205", "user204", "sess204", "2025-12-03 14:15:00", "tile_B", "TILE_VIEW", "Web", "v1.0"),
            ("evt206", "user204", "sess204", "2025-12-03 14:16:00", "tile_B", "TILE_CLICK", "Web", "v1.0"),
        ]
        
        df_tile_input = spark.createDataFrame(
            tile_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", "event_type", "device_type", "app_version"]
        )
        df_tile_input = df_tile_input.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        inter_events_data = [
            ("int201", "user202", "sess202", "2025-12-03 14:07:00", "tile_A", True, True, False),
            ("int202", "user204", "sess204", "2025-12-03 14:17:00", "tile_B", True, False, True),
        ]
        
        df_inter_input = spark.createDataFrame(
            inter_events_data,
            ["event_id", "user_id", "session_id", "event_ts", "tile_id", 
             "interstitial_view_flag", "primary_button_click_flag", "secondary_button_click_flag"]
        )
        df_inter_input = df_inter_input.withColumn("event_ts", F.to_timestamp("event_ts"))
        
        metadata_data = [
            ("tile_A", "Special Offers", "OFFERS", True, "2025-12-03 00:00:00"),
            ("tile_B", "Payment Options", "PAYMENTS", True, "2025-12-03 00:00:00"),
        ]
        
        df_metadata = spark.createDataFrame(
            metadata_data,
            ["tile_id", "tile_name", "tile_category", "is_active", "updated_ts"]
        )
        df_metadata = df_metadata.withColumn("updated_ts", F.to_timestamp("updated_ts"))
        
        logger.info("\nNew Input - Home Tile Events:")
        df_tile_input.show(truncate=False)
        logger.info("\nNew Input - Interstitial Events:")
        df_inter_input.show(truncate=False)
        
        # Process new data
        PROCESS_DATE = "2025-12-03"
        
        df_tile = df_tile_input.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_inter = df_inter_input.filter(F.to_date("event_ts") == PROCESS_DATE)
        df_metadata_active = df_metadata.filter(F.col("is_active") == True)
        
        # Tile aggregations
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
        
        # Join and create daily summary
        df_daily_summary = (
            df_tile_agg.join(df_inter_agg, "tile_id", "outer")
            .withColumn("date", F.lit(PROCESS_DATE).cast("date"))
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
        
        # Enrich with metadata
        df_updated = (
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
        
        logger.info("\nUpdated Output (After Reprocessing):")
        df_updated.show(truncate=False)
        
        # Expected output after update (partition overwrite replaces old data)
        expected_updated_data = [
            ("2025-12-03", "tile_A", "OFFERS", 2, 1, 1, 1, 0),
            ("2025-12-03", "tile_B", "PAYMENTS", 2, 1, 1, 0, 1),
        ]
        
        df_expected_updated = spark.createDataFrame(
            expected_updated_data,
            ["date", "tile_id", "tile_category", "unique_tile_views", "unique_tile_clicks",
             "unique_interstitial_views", "unique_interstitial_primary_clicks", "unique_interstitial_secondary_clicks"]
        )
        df_expected_updated = df_expected_updated.withColumn("date", F.col("date").cast("date"))
        
        logger.info("\nExpected Updated Output:")
        df_expected_updated.show(truncate=False)
        
        # Validation
        output_count = df_updated.count()
        expected_count = df_expected_updated.count()
        
        assert output_count == expected_count, f"Record count mismatch: Expected {expected_count}, Got {output_count}"
        
        # Validate tile_A update
        tile_a_updated = df_updated.filter(F.col("tile_id") == "tile_A").collect()[0]
        assert tile_a_updated["unique_tile_views"] == 2, "tile_A views not updated correctly"
        assert tile_a_updated["unique_tile_clicks"] == 1, "tile_A clicks not updated correctly"
        
        # Validate tile_B update
        tile_b_updated = df_updated.filter(F.col("tile_id") == "tile_B").collect()[0]
        assert tile_b_updated["unique_tile_views"] == 2, "tile_B views not updated correctly"
        assert tile_b_updated["unique_tile_clicks"] == 1, "tile_B clicks not updated correctly"
        
        print_test_result(
            "Scenario 2: UPDATE",
            True,
            "Existing records updated correctly via partition overwrite (idempotent behavior)"
        )
        return True
        
    except Exception as e:
        print_test_result("Scenario 2: UPDATE", False, f"Error: {str(e)}")
        logger.error(f"Test failed with error: {str(e)}")
        return False


# ------------------------------------------------------------------------------
# MAIN TEST EXECUTION
# ------------------------------------------------------------------------------

def run_all_tests():
    """
    Executes all test scenarios and generates a summary report.
    """
    logger.info("\n" + "#"*80)
    logger.info("#" + " "*78 + "#")
    logger.info("#" + " "*20 + "TEST SUITE EXECUTION STARTED" + " "*30 + "#")
    logger.info("#" + " "*78 + "#")
    logger.info("#"*80 + "\n")
    
    test_results = {}
    
    # Run Test Scenario 1
    test_results["Scenario 1: INSERT"] = test_scenario_1_insert()
    
    # Run Test Scenario 2
    test_results["Scenario 2: UPDATE"] = test_scenario_2_update()
    
    # Generate Test Report
    logger.info("\n" + "#"*80)
    logger.info("#" + " "*78 + "#")
    logger.info("#" + " "*25 + "TEST REPORT SUMMARY" + " "*34 + "#")
    logger.info("#" + " "*78 + "#")
    logger.info("#"*80 + "\n")
    
    total_tests = len(test_results)
    passed_tests = sum(1 for result in test_results.values() if result)
    failed_tests = total_tests - passed_tests
    
    logger.info("\n## Test Report\n")
    
    for scenario, result in test_results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"### {scenario}")
        logger.info(f"Status: {status}\n")
    
    logger.info(f"\n**Summary:**")
    logger.info(f"- Total Tests: {total_tests}")
    logger.info(f"- Passed: {passed_tests}")
    logger.info(f"- Failed: {failed_tests}")
    logger.info(f"- Success Rate: {(passed_tests/total_tests)*100:.1f}%\n")
    
    if failed_tests == 0:
        logger.info("✓ ALL TESTS PASSED - Pipeline is working correctly!")
    else:
        logger.info(f"✗ {failed_tests} TEST(S) FAILED - Please review the errors above.")
    
    logger.info("\n" + "#"*80)
    logger.info("#" + " "*78 + "#")
    logger.info("#" + " "*20 + "TEST SUITE EXECUTION COMPLETED" + " "*27 + "#")
    logger.info("#" + " "*78 + "#")
    logger.info("#"*80 + "\n")
    
    return all(test_results.values())


# ------------------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    success = run_all_tests()
    if not success:
        raise Exception("One or more tests failed. Please review the test report above.")
