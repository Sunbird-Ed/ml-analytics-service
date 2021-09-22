#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date

echo "RUNNING JOB"

echo ""
echo "$(date)"
echo "====================================="
echo "Daily Projects Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.6/site-packages/pyspark/bin/spark-submit --driver-memory 5g /opt/sparkjobs/ml-analytics-service/projects/pyspark_project_batch.py
echo "Daily Projects Batch Job Ingestion == Completed"
echo "*************************************"

echo ""
echo "$(date)"
echo "====================================="
echo "Daily Observation Status Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.6/site-packages/pyspark/bin/spark-submit --driver-memory 5g /opt/sparkjobs/ml-analytics-service/observations/pyspark_observation_status_batch.py
echo "Daily Observation Status Batch Job Ingestion == Completed"
echo "*************************************"

echo "COMPLETED"

