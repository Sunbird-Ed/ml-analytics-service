
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
from azure.storage.blob import ContentSettings
from pyspark.sql.functions import element_at, split, col
from configparser import ConfigParser,ExtendedInterpolation
from azure.storage.blob import BlockBlobService, PublicAccess
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# Success logger
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successHandler = RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Error Logger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS', 'project_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'project_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

interval = '1901-01-01/2101-01-01'

url = config.get('VAM', 'druid_query_url')
dashdata = json.loads(config.get("VAM", "program_dashboard_data"))

# Azure details
blob_service_client = BlockBlobService(
    account_name=config.get("AZURE", "public_account_name"), 
    account_key=config.get("AZURE", "public_access_key")
)
container_name = config.get("AZURE", "public_container_name")
local_path =  config.get("OUTPUT_DIR", "project_rollup")
blob_path =  config.get("AZURE", "projects_program_csv")

class Creator:
   
    def status_state(self):
        status_state_df = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID",
                                        "Declared Board", "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective",
                                        "Project start date of the user", "Project completion date of the user", "Project Duration", "Last Synced date", 
                                        "Project Status")
        status_state_df = status_state_df.dropDuplicates()
        status_state_df = status_state_df.na.fill("null")
        status_state_df = status_state_df.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc())
        status_state_df.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/STATEWISE_{solname}_{datetime.datetime.now().date()}")
        status_state_df.unpersist()

    # District wise    
    def status_dist(self, dist):
        dist_status = state_proj_df.select("UUID", "User Type", "User sub type", "Declared State", "District", "Block", "School Name", "School ID",
                                        "Declared Board", "Org Name", "Program Name", "Program ID", "Project ID", "Project Title", "Project Objective",
                                        "Project start date of the user", "Project completion date of the user", "Project Duration", "Last Synced date", 
                                        "Project Status").where(state_proj_df['District'] == dist)
        dist_status = dist_status.dropDuplicates()
        dist_status = dist_status.na.fill("null")
        dist_status = dist_status.sort(col("UUID").asc(), col("Program ID").asc(), col("Project ID").asc())
        dist_status.coalesce(1).write.option("header", True).mode('overwrite').csv(f"{local_path}/{stname}/{dist}/{solname}/distwise_{datetime.datetime.now().date()}")
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
    pid, sid, stname = values['programId'], values['solutionId'], values['stateName']
    solname = values["solutionName"]

    druid_query = {
        "queryType": "scan", 
        "dataSource": "sl-project", 
        "resultFormat": "list",
        "columns": ["createdBy","user_type","designation","state_name","district_name","block_name","school_name","school_code","board_name","organisation_name","program_name",
                    "program_externalId","project_id","project_title_editable","project_description","area_of_improvement","project_created_date","project_completed_date","project_duration",
                    "status_of_project","tasks","sub_task","task_evidence","task_remarks","project_evidence","project_remarks", "program_id", "solution_id", "private_program", 
                    "sub_task_deleted_flag", "task_deleted_flag", "project_deleted_flag", "project_last_sync", "task_sequence",
                    "task_count","task_evidence_count", "project_evidence_count",
                    ],
        "intervals": [interval],
        "batchSize": 100000,
        "filter": {"type": "and",
                    "fields": ""
                }
        }
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
                            "type": "in",
                            "dimension": "status_of_project",
                            "values": ["started", "inProgress"]
                        }]

    response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(druid_query))
    time.sleep(150)
    data_list = response.json()
    del sid, pid  
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
    creator.status_state()

    successLogger.debug("State data stored in file")

# District wise logic 
    unique_district = state_proj_df.select(state_proj_df["District"]).distinct().rdd.flatMap(lambda x: x).collect()
    for districts in unique_district:
        creator.status_dist(districts)

    successLogger.debug("District data stored in file")
    state_proj_df.unpersist()

# Zip files and remove
    shutil.make_archive(f'{local_path}/' + f'{stname}_started_inProgress_{datetime.datetime.now().date()}', 'zip', f'{local_path}/'+f'{stname}')
    successLogger.debug("Zipped data stored")

# Upload in Azure    
for files in os.listdir(local_path):
    if files.endswith(".zip"):
        state_dir = files.split('_')[0]
        blob_service_client.create_blob_from_path(
                        container_name,
                        os.path.join(f"{blob_path}/{state_dir}",files),
                        f"{local_path}/{files}")

gc.collect()
shutil.rmtree(local_path)
