# Release Note - NVSK Script Update

## PR #197: Enhanced NVSK Reporting with Year-wise Analytics and Data Quality Improvements

**Author:** Vivek-M-08  
**Target Branch:** release-5.1.0  
**Date:** December 11, 2025

---

## Summary

This major update transforms the NVSK data processing script with comprehensive enhancements including year-wise analytics, improved data quality controls, schema optimization, and execution date tracking across all reports. The update enables temporal analysis of program growth and ensures more reliable reporting through enhanced data validation.

---

## Key Changes

### 1. **Schema Optimization for Date Handling**
- **Critical Update:** Changed `createdAt` field from `StringType` to `TimestampType` in projects schema
- Eliminates issues with `java.util.GregorianCalendar` string representations
- Enables native Spark timestamp operations for accurate year extraction
- Improves performance and reliability of date-based operations

### 2. **Execution Date Tracking**
- Added `execution_date` field to all three reports (Report 1, Report 2, Report 3)
- Captures timestamp at script initialization: `datetime.datetime.now().strftime("%Y-%m-%d")`
- Enables temporal tracking and audit trail for all generated reports
- Applied consistently across CSV and JSON outputs

### 3. **Data Quality Improvements**
- **Report 1 (District-wise):** Filters out records with null or empty `state_name` or `district_name`
- **Report 3 (Program-wise):** Filters out records with null or empty `State Name` or `District Name`
- Implemented using combined conditions: `isNotNull()` and `trim() != ""`
- Ensures cleaner, more reliable output by removing incomplete geographic records

### 4. **Restructured Reporting Logic**

**Previous Structure:**
1. District-wise Micro Improvement Projects
2. Summary Statistics (Overall totals)
3. Program-wise Detailed Report

**New Structure:**
1. District-wise Micro Improvement Projects (with execution_date)
2. Summary Statistics (Year-wise breakdown with execution_date)
3. Program-wise Detailed Report (with execution_date)

### 5. **Enhanced Summary Statistics - Year-wise Analytics (Report 2)**

**Major Transformation:** Summary report now provides year-wise breakdown instead of single overall totals

**Implementation Details:**
- Extracts year directly from `createdAt` timestamp field using `F.year(F.col("createdAt"))`
- **Unique Leaders Calculation:** Groups by year and counts distinct `userId` values per year
- **Unique Schools Calculation:** 
  - Explodes `userProfile.userLocations` array
  - Filters for school type locations with non-null IDs
  - Groups by year and counts distinct school IDs per year
- Joins leader and school counts by year using left join
- Sorts output chronologically by year

**Before:**
```
Number of Unique Leaders | Number of Unique Schools
305                      | 197
```

**After:**
```
Number of Unique Leaders | Number of Unique Schools | Year | execution_date
156                      | 49                       | 2021 | 2025-12-11
208                      | 127                      | 2022 | 2025-12-11
87                       | 60                       | 2023 | 2025-12-11
```
