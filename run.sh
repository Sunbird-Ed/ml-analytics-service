#!/bin/bash
source /etc/profile
export PYSPARK_PYTHON=python3
export TZ=Asia/Kolkata date
source /opt/sparkjobs/ml-analytics-service/shell_script_config
echo "RUNNING JOB"
driver_memory=$driver_memory
executor_memory=$executor_memory

# PROJECT: Gather Program IDs
echo ""
echo "$(date)"
echo "====================================="
echo "Gather Program IDs == Started"
. /opt/sparkjobs/spark_venv/bin/activate && python /opt/sparkjobs/ml-analytics-service/projects/py_gather_program.py
echo "Gather == Completed"
echo "*************************************"

previous_date=$(date -d "$current_date - 1 day" +"%Y-%m-%d")
echo $previous_date
file="./checker.txt"
read last_ingestion_date < "$file"
echo $last_ingestion_date
if [[ $last_ingestion_date != $previous_date ]]; then
   #PROJECT: Deletion
   echo ""
   echo "$(date)"
   echo "====================================="
   echo "Daily Projects Batch Job Deletion == Started"
   . /opt/sparkjobs/spark_venv/bin/activate && python /opt/sparkjobs/ml-analytics-service/projects/pyspark_project_deletion_batch.py
   echo "Daily Projects Batch Job Deletion == Completed"
   echo "*************************************"

   # PROJECT: Ingestion Program-wise
   echo ""
   echo "$(date)"
   echo "====================================="
   echo "Daily Projects Batch Job Ingestion == Started"
   filename="/opt/sparkjobs/ml-analytics-service/projects/program_ids.txt"
    while read line; do
 		echo $line 		
 		. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/projects/pyspark_project_batch.py --program_id ${line/,}
		. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/projects/pyspark_prj_status.py --program_id ${line/,}
		. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/projects/pyspark_prj_status_prglevel.py --program_id ${line/,}
 	n=$((n+1))
 	done < $filename
 	echo "Daily Projects Batch Job Ingestion == Completed"
 	echo "*************************************"
fi

# OBSERVATION : Deletion and Ingestion
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Observation Status Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/observations/pyspark_observation_status_batch.py
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/observations/pyspark_obs_status_batch.py.py
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/observations/pyspark_obs_domain_criteria_batch.py
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/observations/pyspark_obs_domain_batch.py
echo "Daily Observation Status Batch Job Ingestion == Completed"
echo "*************************************"

# SURVEY : Deletion and Ingestion
echo ""
echo "$(date)"
echo "====================================="
echo "Daily Survey Status Batch Job Ingestion == Started"
. /opt/sparkjobs/spark_venv/bin/activate && /opt/sparkjobs/spark_venv/lib/python3.8/site-packages/pyspark/bin/spark-submit --driver-memory ${driver_memory} --executor-memory ${executor_memory} /opt/sparkjobs/ml-analytics-service/survey/pyspark_survey_status.py
echo "Daily Survey Status Batch Job Ingestion == Completed"
echo "*************************************"

echo "COMPLETED"

