# Release Notes: NVSK Script Enhancement

## Overview
This release introduces key enhancements to the NVSK script in the ML Analytics Service, fulfilling the requirement for expanded reporting capabilities. The updates focus on generating additional CSV reports for better insights into unique leaders/schools and program-wise breakdowns, while improving log management for enhanced traceability.

**Release Date:** November 21, 2025  
**Version:** 1.1.0  
**Author:** Vivek-M-08  
**Related Pull Request:** [#192](https://github.com/Sunbird-Ed/ml-analytics-service/pull/192)  
**Target Branch:** release-5.1.0

---

## What's New

### Enhanced CSV Reporting
- **Unique Leaders/Schools Report**: New CSV output capturing distinct leaders and associated schools, providing a consolidated view for leadership analytics
- **Program-Wise Breakdown Report**: Additional CSV report offering granular breakdowns by program, enabling deeper analysis of participation and outcomes

These reports are generated automatically during script execution, ensuring seamless integration with existing workflows.

### Logging Improvements
- Revised logging paths to a structured directory format for better organization
- Enhanced log levels for report generation steps with timestamps and context-specific details
- Separate success and error log files for improved debugging and monitoring:
  - `nvsk_success.log` - Tracks successful operations
  - `nvsk_error.log` - Captures errors and exceptions



---

## Installation & Upgrade Instructions

### Step 1: Create New Script File
Create a new file in the `urgent_data_metrics` directory:

```bash
touch urgent_data_metrics/updated_imp_project_metrics.py
```

### Step 2: Add Script Code
Copy the code from the updated script: **Source:** [imp_project_metrics.py](https://github.com/Sunbird-Ed/ml-analytics-service/blob/release-5.1.0/urgent_data_metrics/imp_project_metrics.py)

Add this code to your new `updated_imp_project_metrics.py` file.

### Step 3: Update Configuration
Add the following new log configuration entries to your `config.ini` file:

```ini
nvsk_project_success = /Users/user/Documents/Diksha/dev/ml-analytics-service/Logs/nvsk_success.log
nvsk_project_error = /Users/user/Documents/Diksha/dev/ml-analytics-service/Logs/nvsk_error.log
```

**Note:** Ensure that the `/Users/user/Documents/Diksha/dev/ml-analytics-service/Logs` path is replaced with the server log path, refer the above config path from project_success.

### Step 4: Execute the Script
Trigger the Python script using your standard deployment process:

```bash
python urgent_data_metrics/updated_imp_project_metrics.py
```

### Step 5: Verify Deployment
After execution, verify:
1. New CSV files are generated in the expected output directory
2. Log files are created at the configured paths
3. Check `nvsk_success.log` for successful operations
4. Review `nvsk_error.log` for any issues

---

## Technical Details

### New CSV Outputs
1. **Unique Leaders/Schools CSV**
   - Contains deduplicated list of leaders and their associated schools
   - Useful for leadership analytics and school mapping
   
2. **Program-Wise Breakdown CSV**
   - Provides detailed metrics segmented by program
   - Enables program-specific performance analysis

### Logging Structure
```
logs/
├── nvsk_success.log
└── nvsk_error.log
```
---

## References
- **Pull Request:** https://github.com/Sunbird-Ed/ml-analytics-service/pull/192
- **Repository:** https://github.com/Sunbird-Ed/ml-analytics-service
- **Target Release Branch:** release-5.1.0

---

