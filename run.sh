#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date
# source /opt/sparkjobs/ml-analytics-service/shell_script_config
echo "RUNNING JOB"

# PROJECT: Gather Program IDs
echo ""
echo "$(date)"
echo "====================================="
echo "Gather Program IDs == Started"
. /opt/sparkjobs/spark_venv/bin/activate && python /opt/sparkjobs/ml-analytics-service/projects/py_gather_program.py
echo "Gather == Completed"
echo "*************************************"

# PROJECT: Deletion
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Projects Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/projects/pyspark_project_deletion_batch.py
echo "Daily Projects Batch Job Ingestion == Completed"
echo "*************************************"

# PROJECT: Ingestion Program-wise
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Projects Batch Job Ingestion == Started"
filename='/opt/sparkjobs/ml-analytics-service/projects/program_ids.txt'
n=1
while read line; do
	. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/projects/pyspark_project_batch.py --program_id ${line/,}
n=$((n+1))
done < $filename
echo "Daily Projects Batch Job Ingestion == Completed"
echo "*************************************"

# OBSERVATION : Deletion and Ingestion
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Observation Status Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/observations/pyspark_observation_status_batch.py
echo "Daily Observation Status Batch Job Ingestion == Completed"
echo "*************************************"

# SURVEY : Deletion and Ingestion
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Survey Status Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory 50g --executor-memory 50g /opt/sparkjobs/ml-analytics-service/survey/pyspark_survey_status.py
echo "Daily Survey Status Batch Job Ingestion == Completed"
echo "*************************************"

echo "COMPLETED"

