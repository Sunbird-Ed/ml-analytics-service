
import os, shutil
import json, sys, time
import requests, gc
import pyspark.sql.functions as F
import logging
import datetime
import pyspark.sql.utils as ut
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from udf_func import *
from pyspark.sql.types import *
from pyspark.sql import Row
from slackclient import SlackClient
from pyspark.sql.functions import element_at, split, col
from configparser import ConfigParser,ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
sys.path.append(config.get("COMMON", "cloud_module_path"))

from cloud import MultiCloud


cloud_init = MultiCloud()

# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'project_success_error')
file_name_for_output_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-output.log"
file_name_for_debug_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-debug.log"

# Remove old log entries
files_with_date_pattern = [file 
for file in os.listdir(file_path_for_output_and_debug_log) 
if re.match(r"\d{2}-\w+-\d{4}-*", 
file)]

for file_name in files_with_date_pattern:
    file_path = os.path.join(file_path_for_output_and_debug_log, file_name)
    if os.path.isfile(file_path):
        file_date = file_name.split('.')[0]
        date = file_date.split('-')[0] + '-' + file_date.split('-')[1] + '-' + file_date.split('-')[2]
        if date < number_of_days_logs_kept:
            os.remove(file_path)


formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# Handler for output and debug Log
output_logHandler = RotatingFileHandler(f"{file_name_for_output_log}")
output_logHandler.setFormatter(formatter)

debug_logHandler = RotatingFileHandler(f"{file_name_for_debug_log}")
debug_logHandler.setFormatter(formatter)

# Add the successLoger
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}", when="w0",backupCount=1)
successLogger.addHandler(output_logHandler)
successLogger.addHandler(successBackuphandler)

# Add the Errorloger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}",when="w0",backupCount=1)
errorLogger.addHandler(output_logHandler)
errorLogger.addHandler(errorBackuphandler)

# Add the Infologer
infoLogger = logging.getLogger('info log')
infoLogger.setLevel(logging.INFO)
debug_logBackuphandler = TimedRotatingFileHandler(f"{file_name_for_debug_log}",when="w0",backupCount=1)
infoLogger.addHandler(debug_logHandler)
infoLogger.addHandler(debug_logBackuphandler)

interval = '1901-01-01/2101-01-01'

url = config.get('VAM', 'druid_query_url')
dashdata = json.loads(config.get("VAM", "program_dashboard_data"))

local_path =  config.get("OUTPUT_DIR", "project_rollup")
blob_path =  config.get("COMMON", "projects_program_csv")

class Creator:

    def task_state(self):
        task_state_df = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID", "Declared Board",
                                            "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective", "Category", "Project start date of the user",
                                            "Project completion date of the user", "Project Duration", "Project Status", "Tasks", "Sub-Tasks", "Task Evidence", "Task Remarks", 
                                            "Project Evidence", "Project Remarks")
        task_state_df = task_state_df.dropDuplicates()
        task_state_df = task_state_df.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc(), col("Tasks").asc())
        task_state_df.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/task_detail/STATEWISE_{solname}_{datetime.datetime.now().date()}")
        task_state_df.unpersist()

    def filter_task_state(self):
        filter_task_state_df = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "Org Name", "Program Name", 
                                        "Project Title", "Project Objective", "Project Status", "Project completion date of the user", "Tasks", "Task Evidence", 
                                        "Task Remarks", "Project Evidence", "Project Remarks", "Task Sequence")
        filter_task_state_df = filter_task_state_df.dropDuplicates()
        filter_task_state_df = filter_task_state_df.sort(col("District"), col("Block"), col("UUID"), col("Task Sequence"))
        filter_task_state_df = filter_task_state_df.drop("Task Sequence")
        filter_task_state_df.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/filter_task_detail/STATEWISE_{solname}_{datetime.datetime.now().date()}")
        filter_task_state_df.unpersist()
    
    def status_state(self):
        status_state_df = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID",
                                        "Declared Board", "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective",
                                        "Project start date of the user", "Project completion date of the user", "Project Duration", "Last Synced date", 
                                        "Project Status")
        status_state_df = status_state_df.dropDuplicates()
        status_state_df = status_state_df.na.fill("null")
        status_state_df = status_state_df.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc())
        status_state_df.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/status_detail/STATEWISE_{solname}_{datetime.datetime.now().date()}")
        status_state_df.unpersist()

    # District wise
    def task_dist(self, dist):
        dist_task = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID", "Declared Board",
                                            "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective", "Category", "Project start date of the user",
                                            "Project completion date of the user", "Project Duration", "Project Status", "Tasks", "Sub-Tasks", "Task Evidence", "Task Remarks", 
                                            "Project Evidence", "Project Remarks").where(state_proj_df['District'] == dist)
        dist_task = dist_task.dropDuplicates()
        dist_task = dist_task.na.fill("null")
        dist_task = dist_task.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc(), col("Tasks").asc())
        dist_task.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/task_detail/{dist}/{solname}/distwise_{datetime.datetime.now().date()}")
        dist_task.unpersist()

    def filter_task_dist(self, dist):
        filter_dist_task = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "Org Name", "Program Name", 
                                    "Project Title", "Project Objective", "Project Status", "Project completion date of the user", "Tasks", "Task Evidence", 
                                    "Task Remarks", "Project Evidence", "Project Remarks").where(state_proj_df['District'] == dist)
        filter_dist_task = filter_dist_task.dropDuplicates()
        filter_dist_task = filter_dist_task.na.fill("null")
        filter_dist_task = filter_dist_task.sort(col("District").asc(), col("Block").asc(), col("UUID").asc(), col("Task Sequence").asc())
        filter_dist_task = filter_dist_task.drop("Task Sequence")                              
        filter_dist_task.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/filter_task_detail/{dist}/{solname}/distwise_{datetime.datetime.now().date()}")
        filter_dist_task.unpersist()
    
    def status_dist(self, dist):
        dist_status = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID",
                                        "Declared Board", "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective",
                                        "Project start date of the user", "Project completion date of the user", "Project Duration", "Last Synced date", 
                                        "Project Status").where(state_proj_df['District'] == dist)
        dist_status = dist_status.dropDuplicates()
        dist_status = dist_status.na.fill("null")
        dist_status = dist_status.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc())
        dist_status.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/status_detail/{dist}/{solname}/distwise_{datetime.datetime.now().date()}")
        dist_status.unpersist()


# Schema for the dataframe
schema = StructType([
    StructField("events", ArrayType(
        StructType(
            [
                StructField("createdBy", StringType(), True),
                StructField("user_type", StringType(), True),
                StructField("designation", StringType(), True),
                StructField("state_name", StringType(), True),
                StructField("district_name", StringType(), True),
                StructField("block_name", StringType(), True),
                StructField("school_name", StringType(), True),
                StructField("school_code", StringType(), True),
                StructField("board_name", StringType(), True),
                StructField("organisation_name", StringType(), True),
                StructField("program_name", StringType(), True),
                StructField("program_externalId", StringType(), True),
                StructField("project_id", StringType(), True),
                StructField("project_title_editable", StringType(), True),
                StructField("project_description", StringType(), True),
                StructField("area_of_improvement", StringType(), True),
                StructField("project_created_date", StringType(), True),
                StructField("project_completed_date", StringType(), True),
                StructField("project_duration", StringType(), True),
                StructField("status_of_project", StringType(), True),
                StructField("tasks", StringType(), True),
                StructField("sub_task", StringType(), True),
                StructField("task_evidence", StringType(), True),
                StructField("task_remarks", StringType(), True),
                StructField("project_evidence", StringType(), True),
                StructField("project_remarks", StringType(), True),
                StructField("project_last_sync", StringType(), True),
                StructField("task_sequence", StringType(), True),
            ]
        )), True
                )
])

# Gather the data for Druid based on Program, Solution and State
for values in dashdata:
    pid, sid, stname, tc, tec = values['programId'], values['solutionId'], values['stateName'], values["taskCount"], values['taskEvidenceCount']
    pec, filters, solname, require = values['projectEvidenceCount'], values["filters"], values["solutionName"], values["isNeeded"]
    if require != "all":
        all_data = "solo"
    druid_query = {
        "queryType": "scan", 
        "dataSource": "sl-project", 
        "resultFormat": "list",
        "columns": ["createdBy","user_type","designation","state_name","district_name","block_name","school_name","school_code","board_name","organisation_name","program_name",
                    "program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_created_date","project_completed_date","project_duration",
                    "status_of_project","tasks","sub_task","task_evidence","task_remarks","project_evidence","project_remarks", "program_id", "solution_id", "private_program", 
                    "sub_task_deleted_flag", "task_deleted_flag", "project_deleted_flag", "project_last_sync", "task_sequence",
                    "task_count","task_evidence_count", "project_evidence_count"
                    ],
        "intervals": [interval],
        "batchSize": 100000,
        "filter": {"type": "and",
                    "fields": ""
                }
        }
    if filters:
        druid_query["filter"]["fields"] = [{
                            "type": "selector",
                            "dimension": "program_id",
                            "value": f"{pid}"
                        },
                        {
                            "type": "selector",
                            "dimension": "solution_id",
                            "value": f"{sid}"
                        },
                        {
                            "type": "selector",
                            "dimension": "private_program",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "sub_task_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "task_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "project_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "bound",
                            "dimension": "task_count",
                            "lower": tc,
                            "ordering": "numeric"
                        },
                        {
                            "type": "bound",
                            "dimension": "task_evidence_count",
                            "lower": tec,
                            "ordering": "numeric"
                        },
                        {
                            "type": "bound",
                            "dimension": "project_evidence_count",
                            "lower": pec,
                            "ordering": "numeric"
                        }                                               
                    ]
    else:
        druid_query["columns"] = druid_query["columns"][:-3]
        druid_query["filter"]["fields"] = [{
                            "type": "selector",
                            "dimension": "program_id",
                            "value": f"{pid}"
                        },
                        {
                            "type": "selector",
                            "dimension": "solution_id",
                            "value": f"{sid}"
                        },
                        {
                            "type": "selector",
                            "dimension": "private_program",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "sub_task_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "task_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "project_deleted_flag",
                            "value": "false"
                        },
                        {
                            "type": "selector",
                            "dimension": "status_of_project",
                            "value": "submitted"
                        }]

    response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(druid_query))
    time.sleep(150)
    data_list = response.json()
    del tc, pec, tec, sid, pid  
    successLogger.debug("Gathered druid data")

# Start spark server
    spark = SparkSession.builder.appName("projects").config(
        "spark.driver.memory", "50g"
    ).config(
        "spark.executor.memory", "100g"
    ).config(
        "spark.memory.offHeap.enabled", True
    ).config(
        "spark.memory.offHeap.size", "32g"
    ).getOrCreate()
    sc = spark.sparkContext
    df_rdd = sc.parallelize(data_list)
    df = spark.createDataFrame(df_rdd, schema)
    df = df.withColumn("exploded_events", F.explode_outer(F.col("events")))
    successLogger.debug("Started spark")

# Renaming & pre-processing the dataframe
    state_proj_df = df.select(
                df["exploded_events"]["block_name"].alias("Block"),
                df["exploded_events"]["project_title_editable"].alias("Project Title"),
                df["exploded_events"]["task_evidence"].alias("Task Evidence"),
                df["exploded_events"]["user_type"].alias("User Type"),
                df["exploded_events"]["designation"].alias("User sub type"),
                df["exploded_events"]["school_code"].alias("School ID"),
                df["exploded_events"]["project_duration"].alias("Project Duration"),
                df["exploded_events"]["status_of_project"].alias("Project Status"),
                df["exploded_events"]["sub_task"].alias("Sub-Tasks"),
                df["exploded_events"]["tasks"].alias("Tasks"),
                df["exploded_events"]["project_id"].alias("Project ID"),
                df["exploded_events"]["project_description"].alias("Project Objective"),
                df["exploded_events"]["program_externalId"].alias("Program ID"),
                df["exploded_events"]["organisation_name"].alias("Org Name"),
                df["exploded_events"]["createdBy"].alias("UUID"),
                df["exploded_events"]["area_of_improvement"].alias("Category"),
                df["exploded_events"]["school_name"].alias("School Name"),
                df["exploded_events"]["board_name"].alias("Declared Board"),
                df["exploded_events"]["district_name"].alias("District"),
                df["exploded_events"]["program_name"].alias("Program Name"),
                df["exploded_events"]["state_name"].alias("Declared State"),
                df["exploded_events"]["task_remarks"].alias("Task Remarks"),
                df["exploded_events"]["project_evidence"].alias("Project Evidence"),
                df["exploded_events"]["project_remarks"].alias("Project Remarks"),
                df["exploded_events"]["project_created_date"].alias("Project start date of the user"),
                df["exploded_events"]["project_completed_date"].alias("Project completion date of the user"),
                df["exploded_events"]["project_last_sync"].alias("Last Synced date"),
                df["exploded_events"]["task_sequence"].alias("Task Sequence"),
    )

    state_proj_df = state_proj_df.na.fill(value="Null")    
    creator = Creator()
# For task dataframe - state
    if require == "task" and all_data == "solo":
        creator.task_state()

# For filter task dataframe - state
    elif require == "filter-task" and all_data == "solo":
        creator.filter_task_state()

# For status dataframe - state
    elif require == "status" and all_data == "solo":
        creator.status_state()

# For all - state
    else:
        creator.task_state()
        creator.filter_task_state()
        creator.status_state()

    successLogger.debug("State data stored in file")

# District wise logic 
    unique_district = state_proj_df.select(state_proj_df["District"]).distinct().rdd.flatMap(lambda x: x).collect()
    for districts in unique_district:
        # Task csv
        if require == "task" and all_data == "solo":
            creator.task_dist(districts)
        
        # Filter Task csv
        elif require == "filter-task" and all_data == "solo":
            creator.filter_task_dist(districts)

        # Status csv
        elif require == "status" and all_data == "solo":
            creator.status_dist(districts)

        # All csv
        else:
            creator.task_dist(districts)
            creator.filter_task_dist(districts)
            creator.status_dist(districts)

    successLogger.debug("District data stored in file")
    state_proj_df.unpersist()

# Zip files and remove
    shutil.make_archive(f'{local_path}/' + f'{stname}_submitted_{datetime.datetime.now().date()}', 'zip', f'{local_path}/'+f'{stname}')
    successLogger.debug("Zipped data stored")

# Upload in Azure    
for files in os.listdir(local_path):
    if files.endswith(".zip"):
        state_dir = files.split('_')[0]
        cloud_init.upload_to_cloud(blob_Path = blob_path, local_Path = local_path, file_Name = files)

gc.collect()
shutil.rmtree(local_path)
