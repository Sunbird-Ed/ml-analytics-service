#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date
source /opt/sparkjobs/ml-analytics-service/shell_script_config
driver_memory=$driver_memory
executor_memory=$executor_memory

echo "RUNNING Weekly JOB"
echo ""
echo "$(date)"
echo "====================================="
echo "Every Week (Thursday) NVSK Data Upload to S3 Cloud Storage == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} /opt/sparkjobs/ml-analytics-service/urgent_data_metrics/imp_project_metrics.py

echo "Every Week (Thursday) NVSK Data Upload to S3 Cloud Storage == Completed"
echo "*************************************"

echo "COMPLETED"
