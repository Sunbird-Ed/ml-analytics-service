# -----------------------------------------------------------------

# -----------------------------------------------------------------

import json, sys, time
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, re, argparse
import requests
import pyspark.sql.utils as ut
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, array, struct, explode, udf
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from datetime import datetime
from pyspark.sql import DataFrame
from typing import Iterable
import os
import shutil

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

# --- Logging Configuration ---
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# --- Argument Parsing ---
details = argparse.ArgumentParser(description='Pass the ProgramID for archival check')
details.add_argument('--program_id', metavar='--program_id', type=str, help='Program ID to check', required=True)
args = details.parse_args()

program_Id = args.program_id
program_unique_id = ObjectId(program_Id)

# This will be set by the pre-check logic
archive_mode = False 
mode_info = "DAILY" # Default, will change to ARCHIVE if conditions are met

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successLogger.propagate = False
successHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_success'),
    when="w0",
    backupCount=4
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorLogger.propagate = False
errorHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_error'),
    when="w0",
    backupCount=4
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
programsCollec = db[config.get('MONGO', 'programs_collection')]
solutionsCollec = db[config.get('MONGO', 'solutions_collection')]

# Archival configuration
ARCHIVAL_DATE = "1998-01-01T00:00:00.000Z"
druid_sql_url = config.get("DRUID", "sql_url")
druid_batch_end_point = config.get("DRUID", "batch_url")
# TODO : chnage the project_datasource_name to read from config
# project_datasource_name = json.loads(config.get("DRUID", "project_injestion_spec"))["spec"]["dataSchema"]["dataSource"]
project_datasource_name = "sl-project-agg"
print(project_datasource_name)
headers = {'Content-Type': 'application/json'}

successLogger.info(f"Archival Pre-Check Started for Program ID: {program_Id}")

# -----------------------------------------------------------------
# --- NEW: Archival Pre-Check Logic ---
# -----------------------------------------------------------------
try:
    # 1. Mongo Check (Programs): Check if program is completed
    is_program_completed = False
    program_doc = programsCollec.find_one({"_id": program_unique_id})
    
    if not program_doc:
        successLogger.warning(f"Program {program_Id} not found in programs collection. Exiting.")
        sys.exit(0)
    
    program_end_date_str = program_doc.get("endDate")
    if program_end_date_str:
        try:
            # Try parsing ISODate format
            program_end_date = program_end_date_str
            if isinstance(program_end_date_str, str):
                 program_end_date = datetime.fromisoformat(program_end_date_str.replace('Z', '+00:00'))
            
            if program_end_date < datetime.now(program_end_date.tzinfo):
                is_program_completed = True
                successLogger.info(f"Program {program_Id} is COMPLETED (End Date: {program_end_date_str}).")
            else:
                successLogger.info(f"Program {program_Id} is NOT COMPLETED (End Date: {program_end_date_str}). Will proceed with daily ingestion logic.")
        except Exception as e:
            errorLogger.error(f"Could not parse endDate '{program_end_date_str}' for program {program_Id}. Error: {e}")
            sys.exit(1)
    else:
        successLogger.info(f"Program {program_Id} has no endDate. Not considered for archival. Exiting.")
        sys.exit(0)

    # 2. Mongo Check (Solutions): Log Improvement Project Solutions
    solution_cursor = solutionsCollec.find(
        {"programId": program_unique_id, "resourceType": "Improvement Project Solution"},
        {"_id": 1, "name": 1}
    )
    solution_list = list(solution_cursor)
    if solution_list:
        successLogger.info(f"Found {len(solution_list)} Improvement Project Solutions for this program in MongoDB:")
        for solution in solution_list:
            successLogger.info(f"  - Solution ID: {solution['_id']}, Name: {solution.get('name', 'N/A')}")
    else:
        successLogger.info(f"No 'Improvement Project Solution' types found for this program.")

    # 3. Druid Check: Check if program data already exists
    is_in_druid = False
    druid_query = {
        "query": f"SELECT COUNT(DISTINCT(solution_id)) AS \"count\" FROM \"{project_datasource_name}\" WHERE program_id = '{program_Id}'"
    }
    
    try:
        response = requests.post(druid_sql_url, headers=headers, json=druid_query)
        successLogger.debug(f"Druid response status code: {response.status_code}")
        
        if response.status_code >= 400:
            if f"Object '{project_datasource_name}' not found" in response.text:
                successLogger.warning(f"Druid check: Datasource '{project_datasource_name}' not found. This is normal for a first-time run.")
                successLogger.warning("Proceeding, assuming program is missing.")
                is_in_druid = False # Set to False and continue
            else:
                raise Exception(f"Druid returned an unexpected error. Status: {response.status_code}, Body: {response.text}")
        else:
            result_count = response.json()[0]['count']
            if result_count > 0:
                is_in_druid = True
                successLogger.info(f"Druid check SUCCESS. Program already ingested.")
                successLogger.info(f"Found {result_count} Improvement Project Solutions for this program in Druid:")
            else:
                successLogger.info(f"Druid check SUCCESS. Found 0 records. Program is missing from Druid.")
            
    except Exception as e:

        errorLogger.error(f"Failed to query Druid. Error: {e}")
        errorLogger.error(f"Failed Druid request body: {json.dumps(druid_query)}")
        if 'response' in locals():
            errorLogger.error(f"Failed Druid response status code: {response.status_code}")
            errorLogger.error(f"Failed Druid response text: {response.text}")
        else:
            errorLogger.error("Failed Druid request: No response object was created (e.g., connection error).")
        sys.exit(1)

    # # 4. Decision & Action:
    # if is_program_completed and not is_in_druid:
    #     successLogger.info(f"DECISION: Program {program_Id} is COMPLETED and MISSING from Druid. Proceeding with archival ingestion.")
    #     archive_mode = True # This is the internal flag we'll use
    #     mode_info = "ARCHIVE"
    # elif is_program_completed and is_in_druid:
    #     successLogger.info(f"DECISION: Program {program_Id} is COMPLETED andf ALREADY EXISTS in Druid. No action needed. Exiting.")
    #     sys.exit(0)
    # else:
    #     # This case should be caught by earlier checks, but as a safeguard:
    #     successLogger.info("DECISION: Program is not eligible for archival. Exiting.")
    #     sys.exit(0)

    # 4. Decision & Action:
    if is_program_completed:
        if not is_in_druid:
            successLogger.info(f"DECISION: Program {program_Id} is COMPLETED and MISSING from Druid. Proceeding with archival ingestion.")
            archive_mode = True 
            mode_info = "ARCHIVE"
        else:
            successLogger.info(f"DECISION: Program {program_Id} is COMPLETED and ALREADY EXISTS in Druid. No action needed. Exiting.")
            sys.exit(0)
    else:
        successLogger.info(f"DECISION: Program {program_Id} is ACTIVE (not completed). Proceeding with daily ingestion.")
        archive_mode = False 
        mode_info = "DAILY"

except Exception as e:
    errorLogger.error(f"An error occurred during pre-check: {e}", exc_info=True)
    sys.exit(1)
# -----------------------------------------------------------------
# --- END: Archival Pre-Check Logic ---
# -----------------------------------------------------------------


successLogger.info(f"***** Spark Job Started for Program ID: {program_Id} *****")
successLogger.info(f"Starting ingestion - Mode: {mode_info}, Program ID: {str(program_unique_id)}")

# --- Optimized Spark Configuration ---
spark = SparkSession.builder \
    .appName("projects_optimized_sl_project") \
    .config("spark.driver.memory", "50g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "10") \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "32g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "100MB") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")
# --- End Optimized Spark Configuration ---

try:
 def melt(df: DataFrame,id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)
except Exception as e:
   errorLogger.error(e,exc_info=True)

orgSchema = ArrayType(StructType([
    StructField("orgId", StringType(), False),
    StructField("orgName", StringType(), False)
]))

def orgName(val):
  orgarr = []
  if val is not None:
    for org in val: # 'org' is a Row object
        orgObj = {}
        # Use bracket notation for Row object
        if "isSchool" in org and org["isSchool"] == False: 
            orgObj['orgId'] = org['organisationId']
            orgObj['orgName'] = org["orgName"]
            orgarr.append(orgObj)
  return orgarr
orgInfo_udf = udf(orgName,orgSchema)

successLogger.info("Mongo Query started") 

# -----------------------------------------------------------------
base_match = [
    {"isAPrivateProgram": False}, 
    {"isDeleted": False},
    {"programId": program_unique_id},
]

project_query = {"$match": {"$and": base_match}}
# -----------------------------------------------------------------

projects_cursorMongo = projectsCollec.aggregate(
    [project_query,
    {
        "$project": {
            "_id": {"$toString": "$_id"},
            "projectTemplateId": {"$toString": "$projectTemplateId"},
            "solutionInformation": {"name": 1,"_id":{"$toString": "$solutionInformation._id"}},
            "title": {
                "$reduce": {
                    "input": { "$split": ["$title", "\n"] },
                    "initialValue": "",
                    "in": { "$concat": ["$$value", " ", "$$this"] }
                    }
            },
            "remarks":1,
            "attachments":1,
            "programId": {"$toString": "$programId"},
            "programInformation": {"name": 1},
            "metaInformation": {"duration": 1,"goal":1},
            "syncedAt": 1,
            "updatedAt": 1,
            "isDeleted": 1,
            "categories": 1,
            "tasks": 1, # This 'tasks' field will be processed by mapPartitions
            "status": 1,
            "userId": 1,
            "description": {
                "$reduce": {
                    "input": { "$split": ["$description", "\n"] },
                    "initialValue": "",
                    "in": { "$concat": ["$$value", " ", "$$this"] }
                    }
            },
            "createdAt": 1,
            "programExternalId": 1,
            "isAPrivateProgram": 1,
            "hasAcceptedTAndC": 1,
            "userRoleInformation": 1,
            "userProfile": 1,
            "certificate": 1
        }
    }]
)

# This schema must match the output of the mapPartitions function
projects_schema = StructType([
    StructField('_id', StringType(), True),
    StructField('projectTemplateId', StringType(), True),
    StructField(
        'solutionInformation',
        StructType([StructField('name', StringType(), True),
          StructField('_id', StringType(), True)])
    ),
    StructField('title', StringType(), True),
    StructField('programId', StringType(), True),
    StructField('programExternalId', StringType(), True),
    StructField(
        'programInformation',
        StructType([StructField('name', StringType(), True)])
    ),
    StructField(
        'metaInformation',
        StructType([StructField('duration', StringType(), True),
                    StructField('goal', StringType(), True)
                    ])
    ),
    StructField('updatedAt', TimestampType(), True),
    StructField('syncedAt', TimestampType(), True),
    StructField('isDeleted', BooleanType(), True),
    StructField('status', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('description', StringType(), True),
    StructField('createdAt', TimestampType(), True),
    StructField('isAPrivateProgram', BooleanType(), True),
    StructField('hasAcceptedTAndC', BooleanType(), True),
    StructField(
        'categories',
        ArrayType(
            StructType([StructField('name', StringType(), True)])
        ), True
    ),
    StructField(
          'userRoleInformation',
          StructType([
              StructField('role', StringType(), True)
         ])
    ),
    StructField(
          'userProfile',
          StructType([
              StructField('rootOrgId', StringType(), True),
              StructField(
                  'framework',
                  StructType([
                    StructField('board',ArrayType(StringType()), True)
                ])
             ),
             StructField(
                'organisations',ArrayType(
                     StructType([
                        StructField('organisationId', StringType(), True),
                        StructField('orgName', StringType(), True),
                        StructField('isSchool', BooleanType(), True)
                     ]), True)
             ),
          StructField(
                'profileUserTypes',ArrayType(
                     StructType([
                        StructField('type', StringType(), True)
                     ]), True)
             ),
          StructField(
              'userLocations', ArrayType(
                  StructType([
                     StructField('name', StringType(), True),
                     StructField('type', StringType(), True),
                     StructField('id', StringType(), True),
                     StructField('code', StringType(), True)
                  ]),True)
          )
          ])
    ),
    # This 'taskarr' is created by the mapPartitions function
    StructField(
        'taskarr',
         ArrayType(
             StructType([
                  StructField('tasks', StringType(), True),
                  StructField('_id', StringType(), True),
                  StructField('task_sequence', IntegerType(), True),
                  StructField('sub_task_id', StringType(), True),
                  StructField('sub_task', StringType(), True),
                  StructField('sub_task_date',TimestampType(), True),
                  StructField('sub_task_status', StringType(), True),
                  StructField('sub_task_end_date', StringType(), True),
                  StructField('sub_task_deleted_flag', BooleanType(), True),
                  StructField('task_evidence',StringType(), True),
                  StructField('remarks',StringType(), True),
                  StructField('assignee',StringType(), True),
                  StructField('startDate',StringType(), True),
                  StructField('endDate',StringType(), True),
                  StructField('syncedAt',TimestampType(), True),
                  StructField('status',StringType(), True),
                  StructField('task_evidence_status',StringType(), True),
                  StructField('deleted_flag',StringType(), True),
                  StructField('sub_task_start_date',StringType(), True),
                  StructField('prj_remarks',StringType(), True),
                  StructField('prj_evidence',StringType(), True),
                  StructField('prjEvi_type',StringType(), True),
                  StructField('taskEvi_type',StringType(), True)
              ])
          ),True
    ),
    StructField('remarks', StringType(), True),
    StructField('certificate',
          StructType([
            StructField('osid', StringType(), True),
            StructField('status', StringType(), True),
            StructField('issuedOn', StringType(), True),
            StructField('templateUrl', StringType(),True),
            StructField('eligible',BooleanType(), True)
        ])
    ),
    StructField(
        'attachments',
        ArrayType(
            StructType([StructField('sourcePath', StringType(), True)])
        ), True
    )
])


# This function is a direct port of udf function recreate_task_data and task_detail logic.
def process_project_partition(partition_iterator):

    # --- This is the logic from task_detail(task,del_flg,cntr) ---
    def task_detail(task, del_flg, cntr):
      if (type(task)==dict) :
        taskObj = {}
        taskObj["_id"] = task.get("_id")
        taskObj["tasks"] = task.get("name")
        taskObj["task_sequence"] = cntr
        taskObj["deleted_flag"] = del_flg

        try:
            if len(task.get("attachments", [])) > 0:
                taskObj["task_evidence_status"] = True
            else:
                taskObj["task_evidence_status"] = False
        except:
            taskObj["task_evidence_status"] =  False

        taskObj["assignee"] = task.get("assignee", "")
        taskObj["startDate"] = task.get("startDate", "")
        taskObj["endDate"] = task.get("endDate", "")
        taskObj["syncedAt"] = task.get("syncedAt")
        taskObj["status"] = task.get("status", "")
        return taskObj
    # --- End of task_detail logic ---


    # --- This is the logic from recreate_task_data(prj_data) ---
    processed_projects = []
    for prj in partition_iterator:
        prjinfo = []
        # Creating project level remarks and evidence obj
        try:
            project_attachments = prj.get("attachments", [])
            if project_attachments:
                for cnt, attachment in enumerate(project_attachments):
                    prjObj = {}
                    if cnt == 0:
                        if "remarks" in prj:
                            prjObj["prj_remarks"] = prj["remarks"]
                    
                    try:
                        prjObj["prjEvi_type"] = attachment.get("type")
                        if prjObj["prjEvi_type"] == "link":
                            prjObj["prj_evidence"] = attachment.get("name")
                        else:
                            prjObj["prj_evidence"] = attachment.get("sourcePath")
                    except KeyError:
                        pass
                    prjinfo.append(prjObj)
            # Handle case where there are no attachments but there are remarks
            elif prj.get("remarks"):
                 prjObj = {}
                 prjObj["prj_remarks"] = prj["remarks"]
                 prjinfo.append(prjObj)
        except KeyError:
            pass
        
        taskarr = []
        cntr = 1
        for task in prj.get("tasks", []):
            arr_len = 0
            attachLen = len(task.get("attachments", []))
            sub_tskLen = len(task.get("children", []))
            del_flg = task.get("isDeleted", False)
            
            # To get greater length b/w evidence and subtask
            if attachLen > sub_tskLen:
                arr_len = attachLen        
            elif sub_tskLen > attachLen:
                arr_len = sub_tskLen        
            elif ((sub_tskLen == attachLen) & (sub_tskLen == 0)):
                if del_flg == False:
                    taskObj = task_detail(task, del_flg, cntr)
                    if "remarks" in task:
                         taskObj["remarks"] = task["remarks"]
                    taskarr.append(taskObj)
                arr_len = sub_tskLen # This was 0
            elif (sub_tskLen == attachLen):
                arr_len = sub_tskLen
           
            # creating task level remarks and evidence obj
            for index in range(arr_len):
                if del_flg == False:
                    taskObj = task_detail(task, del_flg, cntr)
                    
                    # Try to get task evidence
                    try:
                        attachment = task.get("attachments", [])[index]
                        taskObj["taskEvi_type"] = attachment.get("type")
                        if taskObj["taskEvi_type"] == "link":
                            taskObj["task_evidence"] = attachment.get("name")
                        else:
                            taskObj["task_evidence"] = attachment.get("sourcePath")
                    except:
                        pass
                    
                    # Try to get sub-task name
                    try:
                        taskObj["sub_task"] = task.get("children", [])[index].get("name")
                    except:
                        pass
                    
                    # Add task remarks only on first row for this task
                    if index == 0:
                        if "remarks" in task:
                            taskObj["remarks"] = task["remarks"]
                    
                    # Sub task data    
                    try:
                        if "children" in task and index < len(task.get("children", [])):
                            sub_task = task.get("children", [])[index]
                            sub_del_flg = sub_task.get("isDeleted", False)
                            
                            if sub_del_flg == False:
                                taskObj["sub_task_date"] = sub_task.get("syncedAt")
                                taskObj["sub_task_id"] = sub_task.get("_id")
                                taskObj["sub_task_status"] = sub_task.get("status")
                                taskObj["sub_task_deleted_flag"] = sub_del_flg
                                taskObj["sub_task_start_date"] = sub_task.get("startDate", "")
                                taskObj["sub_task_end_date"] = sub_task.get("endDate", None)
                                taskarr.append(taskObj)
                            else:
                                taskarr.append(taskObj)
                        else:
                            # This appends even if sub-task loop fails (e.g., more attachments than subtasks)
                            taskarr.append(taskObj)
                    except IndexError:
                        taskarr.append(taskObj)
            cntr = cntr + 1
        
        # Formatting project level remarks and evidence
        prjinfo_len = len(prjinfo)
        taskarr_len = len(taskarr)
        
        if taskarr_len == 0 and prjinfo_len > 0:
            # Project has evidence/remarks but no tasks, add prjinfo as rows
            taskarr.extend(prjinfo)
        elif ((taskarr_len > prjinfo_len) & (prjinfo_len !=0)) | ((taskarr_len == prjinfo_len) & (prjinfo_len !=0)):
            for ind in range(prjinfo_len):
                taskarr[ind].update(prjinfo[ind])
        elif (taskarr_len < prjinfo_len):
            try:
                for ind in range(taskarr_len):
                    # Update existing task rows
                    taskarr[ind].update(prjinfo[ind])
                # Add remaining prjinfo as new rows
                for ind in range(taskarr_len, prjinfo_len):
                    taskarr.append(prjinfo[ind])
            except IndexError:
                 pass # Should not happen, but for safety

        prj["taskarr"] = taskarr
        
        if "tasks" in prj:
            del prj["tasks"]
        
        # Ensure all fields from schema are present
        for field in projects_schema.fields:
            if field.name not in prj:
                prj[field.name] = None
                
        processed_projects.append(prj)
        
    return iter(processed_projects)
# --- END: In-line function for mapPartitions ---

projects_list = list(projects_cursorMongo)
successLogger.info("Mongo Query completed and found total records: " + str(len(projects_list)))

if not projects_list:
    successLogger.warning(f"No projects found for program {str(program_unique_id)} in {mode_info} mode. Exiting.")
    sys.exit(0)

prj_rdd = spark.sparkContext.parallelize(projects_list, 200) 

successLogger.info("Applying mapPartitions (task flattening)")
processed_rdd = prj_rdd.mapPartitions(process_project_partition)
processed_rdd.cache() 
successLogger.info("Completed mapPartitions")

#RDD to Dataframe conversion
projects_df = spark.createDataFrame(processed_rdd, projects_schema)
prj_rdd.unpersist()
processed_rdd.unpersist()

successLogger.info("Flattening data started")
projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True ,
        "project imported from library"
    ).otherwise("user created project")
)

projects_df = projects_df.withColumn(
    "project_title",
    F.when(
        projects_df["solutionInformation"]["name"].isNotNull() == True,
        F.regexp_replace(projects_df["solutionInformation"]["name"], "\n|\"", "")
    ).otherwise(F.regexp_replace(projects_df["title"], "\n|\"", ""))
)

projects_df = projects_df.withColumn(
    "project_deleted_flag",
    F.when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == True),
        "true"
    ).when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "private_program",
    F.when(
        (projects_df["isAPrivateProgram"].isNotNull() == True) &
        (projects_df["isAPrivateProgram"] == True),
        "true"
    ).when(
        (projects_df["isAPrivateProgram"].isNotNull() == True) &
        (projects_df["isAPrivateProgram"] == False),
        "false"
    ).otherwise("true")
)

projects_df = projects_df.withColumn(
    "project_terms_and_condition",
    F.when(
        (projects_df["hasAcceptedTAndC"].isNotNull() == True) &
        (projects_df["hasAcceptedTAndC"] == True),
        "true"
    ).when(
        (projects_df["hasAcceptedTAndC"].isNotNull() == True) &
        (projects_df["hasAcceptedTAndC"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
                 "project_evidence_status",
                 F.when(
                      F.size(F.col("attachments"))>=1,True
                 ).otherwise(False)
)

projects_df = projects_df.withColumn(
    "project_completed_date",
    F.when(
        projects_df["status"] == "submitted",
        projects_df["updatedAt"]
    ).otherwise(None)
)
projects_df = projects_df.withColumn(
    "exploded_categories", F.explode_outer(F.col("categories"))
)

category_df = projects_df.groupBy('_id').agg(F.collect_list('exploded_categories.name').alias("category_name"))
category_df = category_df.withColumn("categories_name", F.concat_ws(", ", "category_name"))

projects_df = projects_df.join(category_df, "_id", how = "left")
category_df.unpersist()
projects_df = projects_df.withColumn("parent_channel", F.lit("SHIKSHALOKAM"))

projects_df = projects_df.withColumn(
    "exploded_taskarr", F.explode_outer(projects_df["taskarr"])
)

projects_df = projects_df.withColumn(
    "task_evidence",F.when(
    (projects_df["exploded_taskarr"]["task_evidence"].isNotNull() == True) &
    (projects_df["exploded_taskarr"]["taskEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["task_evidence"]
        )
    ).when(
        (projects_df["exploded_taskarr"]["task_evidence"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["task_evidence"]!="") &
        (projects_df["exploded_taskarr"]["taskEvi_type"] == "link"),
            F.concat(
                F.lit("'"),
                F.regexp_replace(projects_df["exploded_taskarr"]["task_evidence"], "\n|\"", ""),
                F.lit("'")
            )
    ).otherwise(projects_df["exploded_taskarr"]["task_evidence"])
)

projects_df = projects_df.withColumn(
    "task_deleted_flag",
    F.when(
        (projects_df["exploded_taskarr"]["deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["deleted_flag"] == True),
        "true"
    ).when(
        (projects_df["exploded_taskarr"]["deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["deleted_flag"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "sub_task_deleted_flag",
    F.when((
        projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == True),
        "true"
    ).when(
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == False),
        "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn(
    "project_evidence",F.when(
    (projects_df["exploded_taskarr"]["prj_evidence"].isNotNull() == True) &
    (projects_df["exploded_taskarr"]["prjEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["prj_evidence"]
        )
    ).when(
        (projects_df["exploded_taskarr"]["prj_evidence"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["prj_evidence"]!="") &
        (projects_df["exploded_taskarr"]["prjEvi_type"] == "link"),
            F.concat(
                F.lit("'"),
                F.regexp_replace(projects_df["exploded_taskarr"]["prj_evidence"], "\n|\"", ""),
                F.lit("'")
            )
    ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
)

projects_df = projects_df.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
projects_df = projects_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))

projects_df = projects_df.withColumn("project_goal",F.regexp_replace(F.col("metaInformation.goal"), "\n|\"", ""))
projects_df = projects_df.withColumn("area_of_improvement",F.when((F.col("categories_name").isNotNull()) & (F.col("categories_name")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("categories_name"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("categories_name")))
projects_df = projects_df.withColumn("tasks",F.when((F.col("exploded_taskarr.tasks").isNotNull()) & (F.col("exploded_taskarr.tasks")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("exploded_taskarr.tasks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.tasks")))
projects_df = projects_df.withColumn("sub_task",F.when((F.col("exploded_taskarr.sub_task").isNotNull()) & (F.col("exploded_taskarr.sub_task")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("exploded_taskarr.sub_task"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.sub_task")))
projects_df = projects_df.withColumn("program_name",F.regexp_replace(F.col("programInformation.name"), "\n|\"", ""))
projects_df = projects_df.withColumn("task_remarks",F.when((F.col("exploded_taskarr.remarks").isNotNull()) & (F.col("exploded_taskarr.remarks")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("exploded_taskarr.remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.remarks")))
projects_df = projects_df.withColumn("project_remarks",F.when((F.col("exploded_taskarr.prj_remarks").isNotNull()) & (F.col("exploded_taskarr.prj_remarks")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("exploded_taskarr.prj_remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.prj_remarks")))

projects_df = projects_df.withColumn(
                 "evidence_status",
                F.when(
                    (projects_df["project_evidence_status"]== True) & (projects_df["exploded_taskarr"]["task_evidence_status"]==True),True
                ).when(
                    (projects_df["project_evidence_status"]== True) & (projects_df["exploded_taskarr"]["task_evidence_status"]==False),True
                ).when(
                    (projects_df["project_evidence_status"]== False) & (projects_df["exploded_taskarr"]["task_evidence_status"]==True),True
                ).when(
                    (projects_df["project_evidence_status"]== True) & (projects_df["exploded_taskarr"]["task_evidence_status"]=="null"),True
                ).otherwise(False)
)

prj_df_expl_ul = projects_df.withColumn(
   "exploded_userLocations",F.explode_outer(projects_df["userProfile"]["userLocations"])
)

projects_df = projects_df.withColumn(
    "project_title_editable", F.when((F.col("title").isNotNull()) & (F.col("title")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("title"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("title"))
)
projects_df = projects_df.withColumn(
    "project_description", F.when((F.col("description").isNotNull()) & (F.col("description")!=""),F.concat(F.lit("'"),F.regexp_replace(F.col("description"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("description"))
)

pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'
projects_df = projects_df.withColumn('certificate_issued_on', F.regexp_replace(F.col("certificate.issuedOn"), pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))

projects_df = projects_df.withColumn('certificate_status_customised', F.when(((F.col("certificate.eligible").isNotNull()) & (F.col("certificate.eligible") == True) & (F.col("certificate.osid").isNotNull())),F.lit("Issued")).otherwise(F.lit("")))

projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["project_created_type"],
    projects_df["project_title"],
    projects_df["project_title_editable"],
    projects_df["programId"].alias("program_id"),
    projects_df["programExternalId"].alias("program_externalId"),
    projects_df["program_name"],
    projects_df["metaInformation"]["duration"].alias("project_duration"),
    projects_df["syncedAt"].alias("project_last_sync"),
    projects_df["updatedAt"].alias("project_updated_date"),
    projects_df["project_deleted_flag"],
    projects_df["area_of_improvement"],
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["project_description"],
    projects_df["project_goal"],projects_df["project_evidence"],
    projects_df["parent_channel"],
    projects_df["createdAt"].alias("project_created_date"),
    projects_df["exploded_taskarr"]["_id"].alias("task_id"),
    projects_df["tasks"],projects_df["project_remarks"],
    projects_df["exploded_taskarr"]["assignee"].alias("task_assigned_to"),
    projects_df["exploded_taskarr"]["startDate"].alias("task_start_date"),
    projects_df["exploded_taskarr"]["endDate"].alias("task_end_date"),
    projects_df["exploded_taskarr"]["syncedAt"].alias("tasks_date"),projects_df["exploded_taskarr"]["status"].alias("tasks_status"),
    projects_df["task_evidence"],
    projects_df["exploded_taskarr"]["task_evidence_status"].alias("task_evidence_status"),
    projects_df["exploded_taskarr"]["sub_task_id"].alias("sub_task_id"),
    projects_df["sub_task"],
    projects_df["exploded_taskarr"]["sub_task_status"].alias("sub_task_status"),
    projects_df["exploded_taskarr"]["sub_task_date"].alias("sub_task_date"),
    projects_df["exploded_taskarr"]["sub_task_start_date"].alias("sub_task_start_date"),
    projects_df["exploded_taskarr"]["sub_task_end_date"].alias("sub_task_end_date"),
    projects_df["private_program"],
    projects_df["task_deleted_flag"],projects_df["sub_task_deleted_flag"],
    projects_df["project_terms_and_condition"],
    projects_df["task_remarks"],projects_df["exploded_taskarr"]["task_sequence"].alias("task_sequence"),
    projects_df["project_completed_date"],
    projects_df["solutionInformation"]["_id"].alias("solution_id"),
    projects_df["userRoleInformation"]["role"].alias("designation"),
    projects_df["userProfile"]["rootOrgId"].alias("channel"),
    projects_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
    projects_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
    projects_df["certificate"]["osid"].alias("certificate_id"),
    projects_df["certificate"]["status"].alias("certificate_status"),
    projects_df["certificate_status_customised"],
    projects_df["certificate_issued_on"],
    projects_df["certificate"]["templateUrl"].alias("certificate_template_url"),
    F.concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
    F.concat_ws(",",F.array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type"),
    projects_df["evidence_status"]
)

# This logic is required for columns in the final sl-project Druid spec
projects_task_cnt = projects_df_cols.groupBy("project_id").agg(F.countDistinct(F.col("task_id")).alias("task_count"))
projects_prj_evi= projects_df_cols.groupBy("project_id").agg(F.countDistinct("project_evidence").alias("project_evidence_count"))
projects_dff = projects_task_cnt.join(projects_prj_evi,["project_id"],"left")
projects_tsk_evi = projects_df_cols.groupBy("project_id").agg(F.countDistinct("task_evidence").alias("task_evidence_count"))
projects_df_cols = projects_df_cols.join(projects_dff,["project_id"],"left")
projects_df_cols = projects_df_cols.join(projects_tsk_evi,["project_id"],"left")

successLogger.info("Flattening data completed")
projects_df.unpersist()
projects_prj_evi.unpersist()
projects_task_cnt.unpersist()
projects_dff.unpersist()
projects_task_cnt.unpersist()
projects_tsk_evi.unpersist()
projects_df_cols = projects_df_cols.dropDuplicates()

successLogger.info("Getting Entities data from userLocations")

entities_df = melt(prj_df_expl_ul,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
prj_df_expl_ul.unpersist()
entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId")))
entities_df = entities_df.withColumn("variable1",F.concat(F.col("type"),F.lit("_name")))
entities_df = entities_df.withColumn("variable2",F.concat(F.col("type"),F.lit("_code")))

entities_df_id=entities_df.groupBy("_id").pivot("variable").agg(F.first("id"))
entities_df_name=entities_df.groupBy("_id").pivot("variable1").agg(F.first("name"))
entities_df_value=entities_df.groupBy("_id").pivot("variable2").agg(F.first("value"))

entities_df_med=entities_df_id.join(entities_df_name,["_id"],how='outer')
entities_df_res=entities_df_med.join(entities_df_value,["_id"],how='outer')
entities_df_res=entities_df_res.drop('null')

entities_df.unpersist()

projects_df_final = projects_df_cols.join(entities_df_res,projects_df_cols["project_id"]==entities_df_res["_id"],how='left')\
        .drop(entities_df_res["_id"])

successLogger.info("Final Dataframe is ready to be written to json")
entities_df_res.unpersist()
projects_df_cols.unpersist()
final_projects_df = projects_df_final.dropDuplicates()

necessary_columns = ["state_name","state_externalId","district_name","district_externalId","block_name",
                    "block_externalId","organisation_name","organisation_id"]
final_df_columns = final_projects_df.columns
for miss_cols in necessary_columns:
    if miss_cols not in final_df_columns:
        final_projects_df = final_projects_df.withColumn(miss_cols, lit(None).cast(StringType()))

projects_df_final.unpersist()

successLogger.info("Json file generation started")
final_projects_df.coalesce(1).write.format("json").mode("overwrite").save(
    config.get("OUTPUT_DIR", "project") + "/"
)
successLogger.info("Json file generation completed")
final_projects_df.unpersist()

file_suffix = f"_{program_unique_id}" if program_unique_id else ""
output_file_name = f"sl_projects{file_suffix}.json"
output_dir = config.get("OUTPUT_DIR", "project")

for filename in os.listdir(output_dir):
    if filename.endswith(".json"):
        os.rename(
           os.path.join(output_dir, filename),
           os.path.join(output_dir, output_file_name)
        )
        successLogger.info(f"Renamed output file to: {output_file_name}")
        break

sys.path.append(config.get("COMMON", "cloud_module_path"))
from cloud import MultiCloud
cloud_init = MultiCloud()
local_path = config.get("OUTPUT_DIR", "project")
blob_path = config.get("COMMON", "projects_blob_path") 

# Upload to local S3 (moving file)
# successLogger.info("Uploading to local S3/moving file" )
# target_dir = "/Users/user/Documents/Diksha/dev/ml-analytics-service/local_S3/projects"
# if os.path.exists(os.path.join(local_path, output_file_name)):
#     shutil.move(
#         os.path.join(local_path, output_file_name),
#         os.path.join(target_dir, output_file_name)
#     )
#     successLogger.info(f"Moved {output_file_name} to {target_dir}")
# else:
#     successLogger.warning(f"Could not find {output_file_name} to move.")

# Upload to cloud
successLogger.info("Uploading to cloud storage started")
if os.path.exists(os.path.join(local_path, output_file_name)):
    cloud_init.upload_to_cloud(blob_Path=blob_path, local_Path=local_path, file_Name=output_file_name)
    successLogger.info(f"Uploaded {output_file_name} to cloud storage at {blob_path}")
    
    # Optional: Remove local file after upload
    # TODO: Uncomment below lines to remove local file after upload
    # os.remove(os.path.join(local_path, output_file_name))
    # successLogger.info(f"Removed local file {output_file_name}")
else:
    successLogger.warning(f"Could not find {output_file_name} to upload.")

successLogger.info("Uploading to cloud storage completed")


dimensionsArr = []
entitiesArr = ["state_externalId", "block_externalId", "district_externalId", "cluster_externalId", "school_externalId",\
              "state_name","block_name","district_name","cluster_name","school_name","board_name","state_code", \
              "block_code", "district_code", "cluster_code", "school_code"]
dimensionsArr = list(set(entitiesArr))

submissionReportColumnNamesArr = [
    'project_title', 'project_goal', 'project_created_date', 'project_last_sync',
    'area_of_improvement', 'status_of_project', 'tasks', 'tasks_date', 'tasks_status',
    'sub_task', 'sub_task_status', 'sub_task_date', 'task_start_date', 'task_end_date',
    'sub_task_start_date', 'sub_task_end_date', 'designation', 'project_deleted_flag',
    'task_evidence', 'task_evidence_status', 'project_id', 'task_id', 'sub_task_id',
    'project_created_type', 'task_assigned_to', 'channel', 'parent_channel', 'program_id',
    'program_name', 'project_updated_date', 'createdBy', 'project_title_editable',
    'project_duration', 'program_externalId', 'private_program', 'task_deleted_flag',
    'sub_task_deleted_flag', 'project_terms_and_condition','task_remarks',
    'organisation_name','project_description','project_completed_date','solution_id',
    'project_remarks','project_evidence','organisation_id','user_type', 'certificate_id',
    'certificate_status','certificate_issued_on','certificate_status_customised','certificate_template_url',{"type":"long","name":"task_count"},{"type":"long","name":"task_evidence_count"},{"type":"long","name":"project_evidence_count"},{"type":"long","name":"task_sequence"}
]

dimensionsArr.extend(submissionReportColumnNamesArr)

# --- Dynamic Druid Spec for sl-project ---
payload = {}
#TODO : Change to project_injestion_spec once testing is done
payload = json.loads(config.get("DRUID","project_injestion_spec_cloud"))


#LOCAL DRUID INGESTION CONFIGURATION
# 1. Set File Path
# payload["spec"]["ioConfig"]["inputSource"] = {
#     "type": "local",
#     "baseDir": target_dir, 
#     "filter": output_file_name 
# }
# 2. Set Ingestion Mode and Timestamp based on archive_mode
# if archive_mode:
#     # --- ARCHIVAL PATH ---
#     successLogger.info("Setting Druid spec for ARCHIVAL ingestion (overwrite).")
#     payload['spec']['ioConfig'].update({"appendToExisting":True})
#     successLogger.info("Set 'appendToExisting: True' for ARCHIVAL program run.")
#     payload["spec"]["dataSchema"]["timestampSpec"] = {
#         "column": "!!_time", 
#         "format": "iso",
#         "missingValue": f"{ARCHIVAL_DATE}" 
#     }
#     successLogger.info(f"Replaced timestampSpec to ingest all data at constant time: {ARCHIVAL_DATE}")
# else: 
#     # --- DAILY PATH ---
#     successLogger.info("Setting Druid spec for DAILY ingestion.")
#     payload['spec']['ioConfig'].update({"appendToExisting":True})
#     successLogger.info("Set 'appendToExisting: True' for DAILY program run.")
#     successLogger.info(f"Using original timestampSpec")

# CLOUD DRUID INGESTION CONFIGURATION
if archive_mode:
    # --- ARCHIVAL PATH ---
    successLogger.info("Setting Druid spec for ARCHIVAL ingestion (overwrite).")
    current_cloud = re.split("://+", payload["spec"]["ioConfig"]["inputSource"]["uris"][0])[0]
    uri = re.split("://+", payload["spec"]["ioConfig"]["inputSource"]["uris"][0])[1]
    edited_uri = re.split(".json", uri)[0]
    payload["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
    payload['spec']['ioConfig'].update({"appendToExisting":True})
    successLogger.info("Set 'appendToExisting: True' for ARCHIVAL program run.")
    payload["spec"]["dataSchema"]["timestampSpec"] = {
        "column": "!!_time", 
        "format": "iso",
        "missingValue": f"{ARCHIVAL_DATE}" 
    }
    successLogger.info(f"Replaced timestampSpec to ingest all data at constant time: {ARCHIVAL_DATE}")

else: 
    # --- DAILY PATH ---
    successLogger.info("Setting Druid spec for DAILY ingestion.")
    current_cloud = re.split("://+", payload["spec"]["ioConfig"]["inputSource"]["uris"][0])[0]
    uri = re.split("://+", payload["spec"]["ioConfig"]["inputSource"]["uris"][0])[1]
    edited_uri = re.split(".json", uri)[0]
    payload["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
    payload['spec']['ioConfig'].update({"appendToExisting":True})
    successLogger.info("Set 'appendToExisting: True' for DAILY program run.")
    successLogger.info(f"Using original timestampSpec")

payload["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = dimensionsArr
datasources = [payload["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(payload)]


successLogger.info(f"Druid Ingestion started for Mode: {mode_info}")
for i, j in zip(datasources,ingestion_specs):
    successLogger.info("Pushing the data to druid datasources: " + str(i))
    #TODO : Don't print the entire spec in production logs
    successLogger.info("Pushing the project_injestion_spec data to druid : " + str(j))
    start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)

    if start_supervisor.status_code == 200:
        successLogger.info("Started the batch ingestion task sucessfully for the datasource " + i)
    else:
        errorLogger.error("failed to start batch ingestion task" + i)
        errorLogger.error("failed to start batch ingestion task " + str(start_supervisor.status_code))
        errorLogger.error(start_supervisor.text)

successLogger.info(f"Druid Ingestion complete for Mode: {mode_info}")
successLogger.info(f"***** Spark Job Completed for Program ID: {program_Id} *****")
