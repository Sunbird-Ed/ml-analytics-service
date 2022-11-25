#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date
echo "RUNNING JOB"

echo ""
echo "$(date)"
echo "====================================="
echo "Daily program dashboard Started-InProgress report run == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/projects/pyspark_started_inProgress_programdashboard.py
echo "Daily program dashboard Started-InProgress report run == Completed"
echo "*************************************"
echo "COMPLETED"

echo ""
echo "$(date)"
echo "====================================="
echo "Daily program dashboard Submit report run == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/projects/pyspark_submitted_programdashboard.py
echo "Daily program dashboard Submit report run == Completed"
echo "*************************************"
echo "COMPLETED"