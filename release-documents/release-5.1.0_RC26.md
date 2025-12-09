# Release Note - NVSK Script Update

## PR #197: Updated NVSK Script

**Author:** Vivek-M-08  
**Target Branch:** release-5.1.0  
**Date:** December 9, 2025

---

## Summary
This update enhances the NVSK data processing script with improved data quality controls, restructured reporting logic, and the addition of execution date tracking across all reports.

---

## Key Changes

### 1. **Execution Date Tracking**
- Added `execution_date` field to all generated reports (district-wise, program-wise, and summary reports)
- Enables temporal tracking of when each report was generated
- Execution date is captured at script start and applied consistently across all outputs

### 2. **Data Quality Improvements**
- **Report 1 (District-wise):** Added filtering to exclude records with null or empty state/district names
- **Report 2 (Program-wise):** Added filtering to exclude records with null or empty state/district names
- Ensures cleaner, more reliable output data by removing incomplete geographic records

### 3. **Report Restructuring**
The three reports have been reorganized for better logical flow:

**Previous Order:**
1. District-wise Micro Improvement Projects
2. Summary Statistics (Unique Leaders/Schools)
3. Program-wise Detailed Report

**New Order:**
1. District-wise Micro Improvement Projects
2. Program-wise Detailed Report
3. Summary Statistics (Unique Leaders/Schools)

### 4. **Enhanced Summary Statistics (Report 3)**
- **Major Change:** Summary report now provides year-wise breakdown instead of overall totals
- Implemented MongoDB aggregation pipeline to calculate unique leaders and schools per year
- Uses MongoDB's `$group`, `$addFields`, and `$reduce` operations for efficient year-based aggregation
- Extracts year from project `createdAt` timestamp
- Filters school locations directly in the aggregation pipeline
- Output now includes "Year" column alongside unique user and school counts

### 5. **JSON Schema Updates**
Updated JSON keys across all reports to include the new execution_date field:
- **Report 1:** Added "execution_date" to district-wise improvement keys
- **Report 2:** Added "execution_date" to program-wise improvement keys
- **Report 3:** Added both "Year" and "execution_date" to summary statistics keys

---

## Technical Details

### Files Modified
- `pyspark_project_batch.py` (151 additions, 64 deletions)

### Impact Areas
1. **Data Quality:** Better filtering reduces noise in location-based reports
2. **Reporting:** Year-wise summary statistics provide temporal insights into program growth
3. **Auditability:** Execution dates enable tracking of when reports were generated
4. **Data Structure:** All output CSVs and JSONs now include execution timestamp

### Database Operations
- Enhanced MongoDB aggregation in Report 3 with multi-stage pipeline
- Added `$year` extraction from timestamp fields
- Implemented `$reduce` operation for nested array processing

---

## Output Changes

### Updated CSV/JSON Schemas

**Report 1 - District-wise Micro Improvement:**
- Added: `execution_date` column

**Report 2 - Program-wise Improvement:**
- Added: `execution_date` column

**Report 3 - Improvement Journey Summary:**
- Added: `Year` column (breakdown by year)
- Added: `execution_date` column
- Changed from: Single row of overall totals
- Changed to: Multiple rows showing year-over-year statistics
