=============================================
Author: Ascendion AAVA
Date: 
Description: Technical specification for adding SOURCE_TILE_METADATA table and extending target summary with tile category
=============================================

# Technical Specification for Home Tile Reporting Enhancement - Tile Category Integration

## Introduction

### Business Context
Product Analytics has requested the addition of tile-level metadata to enrich reporting dashboards. Currently, all tile metrics (views, clicks, CTRs, interstitial interactions) are aggregated only at tile_id level with no business grouping. This enhancement will add tile category information to enable better reporting and performance tracking at a functional level.

### Enhancement Objectives
- Add new source table `SOURCE_TILE_METADATA` for tile categorization
- Extend existing ETL pipeline to incorporate tile metadata
- Modify target table `TARGET_HOME_TILE_DAILY_SUMMARY` to include tile category
- Enable category-level performance analysis and reporting
- Maintain backward compatibility with existing processes

### Scope
- **In Scope**: New metadata table creation, ETL pipeline modification, target table enhancement
- **Out of Scope**: Global KPI table changes, historical data backfill, dashboard UI updates

## Code Changes

### 1. ETL Pipeline Modifications (`home_tile_reporting_etl.py`)

#### 1.1 Configuration Updates
```python
# Add new source table configuration
SOURCE_TILE_METADATA = "analytics_db.SOURCE_TILE_METADATA"
```

#### 1.2 Source Data Reading Enhancement
```python
# Read tile metadata table
df_metadata = spark.table(SOURCE_TILE_METADATA).filter(F.col("is_active") == True)
```

#### 1.3 Daily Summary Aggregation Logic Update
**Current Logic Location**: Lines 45-65 in `home_tile_reporting_etl.py`

**Modified Logic**:
```python
# Enhanced daily summary with metadata join
df_daily_summary_enhanced = (
    df_daily_summary
    .join(df_metadata.select("tile_id", "tile_category"), "tile_id", "left")
    .withColumn("tile_category", F.coalesce(F.col("tile_category"), F.lit("UNKNOWN")))
    .select(
        "date",
        "tile_id",
        "tile_category",  # New column
        "unique_tile_views",
        "unique_tile_clicks",
        "unique_interstitial_views",
        "unique_interstitial_primary_clicks",
        "unique_interstitial_secondary_clicks"
    )
)
```

#### 1.4 Write Operation Update
**Current Location**: Lines 85-95

**Modified Code**:
```python
# Update the overwrite partition call
overwrite_partition(df_daily_summary_enhanced, TARGET_DAILY_SUMMARY)
```

### 2. Error Handling and Data Quality Checks
```python
# Add data quality validation
def validate_metadata_join(df_summary, df_enhanced):
    """Ensure no record loss during metadata join"""
    original_count = df_summary.count()
    enhanced_count = df_enhanced.count()
    
    if original_count != enhanced_count:
        raise ValueError(f"Record count mismatch: Original={original_count}, Enhanced={enhanced_count}")
    
    return True

# Add validation call after join
validate_metadata_join(df_daily_summary, df_daily_summary_enhanced)
```

## Data Model Updates

### 1. New Source Table: `SOURCE_TILE_METADATA`

**Table Structure** (Already defined in `SOURCE_TILE_METADATA.sql`):
```sql
CREATE TABLE IF NOT EXISTS analytics_db.SOURCE_TILE_METADATA 
(
    tile_id        STRING    COMMENT 'Tile identifier',
    tile_name      STRING    COMMENT 'User-friendly tile name',
    tile_category  STRING    COMMENT 'Business or functional category of tile',
    is_active      BOOLEAN   COMMENT 'Indicates if tile is currently active',
    updated_ts     TIMESTAMP COMMENT 'Last update timestamp'
)
USING DELTA
COMMENT 'Master metadata for homepage tiles, used for business categorization and reporting enrichment';
```

### 2. Target Table Enhancement: `TARGET_HOME_TILE_DAILY_SUMMARY`

**Current Schema** (from `TargetDDL.sql`):
- date
- tile_id
- unique_tile_views
- unique_tile_clicks
- unique_interstitial_views
- unique_interstitial_primary_clicks
- unique_interstitial_secondary_clicks

**Enhanced Schema**:
```sql
ALTER TABLE reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
ADD COLUMN tile_category STRING COMMENT 'Functional category of the tile';
```

**Complete Enhanced Table Structure**:
```sql
CREATE TABLE IF NOT EXISTS reporting_db.TARGET_HOME_TILE_DAILY_SUMMARY 
(
    date                                DATE       COMMENT 'Reporting date',
    tile_id                             STRING     COMMENT 'Tile identifier',
    tile_category                       STRING     COMMENT 'Functional category of the tile',
    unique_tile_views                   LONG       COMMENT 'Distinct users who viewed the tile',
    unique_tile_clicks                  LONG       COMMENT 'Distinct users who clicked the tile',
    unique_interstitial_views           LONG       COMMENT 'Distinct users who viewed interstitial',
    unique_interstitial_primary_clicks  LONG       COMMENT 'Distinct users who clicked primary button',
    unique_interstitial_secondary_clicks LONG      COMMENT 'Distinct users who clicked secondary button'
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily aggregated tile-level metrics for reporting with category enrichment';
```

### 3. Data Lineage Updates

**New Data Flow**:
```
SOURCE_HOME_TILE_EVENTS ────┐
                            ├─── ETL Pipeline ─── TARGET_HOME_TILE_DAILY_SUMMARY
SOURCE_INTERSTITIAL_EVENTS ─┤                           (Enhanced with tile_category)
                            │
SOURCE_TILE_METADATA ───────┘
```

## Source-to-Target Mapping

### Enhanced Mapping Table

| Source Table | Source Column | Target Table | Target Column | Transformation Rule | Data Type | Business Rule |
|--------------|---------------|--------------|---------------|-------------------|-----------|---------------|
| SOURCE_HOME_TILE_EVENTS | tile_id | TARGET_HOME_TILE_DAILY_SUMMARY | tile_id | Direct mapping | STRING | Primary key for aggregation |
| SOURCE_HOME_TILE_EVENTS | event_ts | TARGET_HOME_TILE_DAILY_SUMMARY | date | `to_date(event_ts)` | DATE | Partition key |
| SOURCE_HOME_TILE_EVENTS | user_id, event_type | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_views | `countDistinct(when(event_type='TILE_VIEW', user_id))` | LONG | Count distinct users who viewed |
| SOURCE_HOME_TILE_EVENTS | user_id, event_type | TARGET_HOME_TILE_DAILY_SUMMARY | unique_tile_clicks | `countDistinct(when(event_type='TILE_CLICK', user_id))` | LONG | Count distinct users who clicked |
| SOURCE_INTERSTITIAL_EVENTS | user_id, interstitial_view_flag | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_views | `countDistinct(when(interstitial_view_flag=true, user_id))` | LONG | Count distinct interstitial views |
| SOURCE_INTERSTITIAL_EVENTS | user_id, primary_button_click_flag | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_primary_clicks | `countDistinct(when(primary_button_click_flag=true, user_id))` | LONG | Count distinct primary clicks |
| SOURCE_INTERSTITIAL_EVENTS | user_id, secondary_button_click_flag | TARGET_HOME_TILE_DAILY_SUMMARY | unique_interstitial_secondary_clicks | `countDistinct(when(secondary_button_click_flag=true, user_id))` | LONG | Count distinct secondary clicks |
| **SOURCE_TILE_METADATA** | **tile_category** | **TARGET_HOME_TILE_DAILY_SUMMARY** | **tile_category** | **`coalesce(tile_category, 'UNKNOWN')`** | **STRING** | **Left join with default fallback** |

### New Transformation Rules

#### 1. Metadata Enrichment Logic
```sql
-- Pseudocode for tile category mapping
SELECT 
    summary.*,
    COALESCE(metadata.tile_category, 'UNKNOWN') as tile_category
FROM daily_summary summary
LEFT JOIN tile_metadata metadata 
    ON summary.tile_id = metadata.tile_id 
    AND metadata.is_active = true
```

#### 2. Data Quality Rules
- **Null Handling**: If `tile_category` is null, default to 'UNKNOWN'
- **Active Filter**: Only join with active tiles (`is_active = true`)
- **Record Preservation**: Left join ensures no summary records are lost
- **Case Sensitivity**: Tile categories should be uppercase for consistency

#### 3. Business Logic Rules
- **Category Standardization**: Valid categories include 'OFFERS', 'HEALTH_CHECKS', 'PAYMENTS', 'PERSONAL_FINANCE', 'UNKNOWN'
- **Backward Compatibility**: Existing tile_id records without metadata get 'UNKNOWN' category
- **Metadata Updates**: Changes to tile categories will reflect in next day's processing

## Assumptions and Constraints

### Assumptions
1. **Data Availability**: `SOURCE_TILE_METADATA` table will be populated before ETL execution
2. **Data Quality**: Tile metadata will be maintained by the product team
3. **Performance**: Left join with metadata table will not significantly impact ETL performance
4. **Schema Stability**: Target table schema changes are acceptable for this enhancement
5. **Backward Compatibility**: Existing downstream consumers can handle new column addition

### Constraints
1. **No Historical Backfill**: Enhancement applies only to future data processing
2. **Global KPI Unchanged**: `TARGET_HOME_TILE_GLOBAL_KPIS` table remains unmodified
3. **Partition Strategy**: Existing date-based partitioning strategy is maintained
4. **Data Retention**: Same retention policies apply to enhanced target table
5. **Processing Window**: ETL execution time should not increase significantly

### Technical Constraints
1. **Delta Lake Compatibility**: All changes must be compatible with Delta Lake format
2. **Spark Version**: Changes must work with current Spark/Databricks environment
3. **Memory Usage**: Join operations should not exceed current memory allocations
4. **Idempotency**: Enhanced ETL must maintain idempotent behavior for reprocessing

## Implementation Plan

### Phase 1: Infrastructure Setup
1. Create `SOURCE_TILE_METADATA` table in analytics_db
2. Add sample metadata records for testing
3. Update table permissions and access controls

### Phase 2: ETL Enhancement
1. Modify `home_tile_reporting_etl.py` with metadata join logic
2. Add data quality validation functions
3. Update error handling and logging

### Phase 3: Target Schema Update
1. Add `tile_category` column to `TARGET_HOME_TILE_DAILY_SUMMARY`
2. Test schema evolution compatibility
3. Validate partition overwrite behavior

### Phase 4: Testing and Validation
1. Unit testing for new transformation logic
2. Integration testing with sample data
3. Performance testing for join operations
4. Regression testing for existing functionality

## References

### JIRA Stories
- **PCE-3**: Add New Source Table SOURCE_TILE_METADATA and Extend Target Summary Table with Tile Category

### Source Files
- `Input/home_tile_reporting_etl.py` - Current ETL implementation
- `Input/SourceDDL.sql` - Existing source table definitions
- `Input/TargetDDL.sql` - Current target table definitions  
- `Input/SOURCE_TILE_METADATA.sql` - New metadata table definition

### Technical Documentation
- Delta Lake Documentation for schema evolution
- PySpark SQL Functions Reference
- Databricks Best Practices for ETL pipelines

---

## Cost Estimation and Justification

### Token Usage Analysis
- **Input Tokens**: Approximately 4,200 tokens (including prompt, JIRA story, and source files)
- **Output Tokens**: Approximately 2,800 tokens (this technical specification document)
- **Model Used**: GPT-4 (detected from processing context)

### Cost Calculation
**Assuming GPT-4 pricing (example rates)**:
- Input Cost: 4,200 tokens × $0.03/1K tokens = $0.126
- Output Cost: 2,800 tokens × $0.06/1K tokens = $0.168
- **Total Estimated Cost**: $0.294

**Formula**: 
```
Total Cost = (Input_Tokens × Input_Rate_Per_1K) + (Output_Tokens × Output_Rate_Per_1K)
Total Cost = (4,200 × $0.03/1K) + (2,800 × $0.06/1K) = $0.294
```

*Note: Actual costs may vary based on current API pricing and exact token counts.*