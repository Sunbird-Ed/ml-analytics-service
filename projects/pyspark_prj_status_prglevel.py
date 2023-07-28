# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author :Shakthiehswari, Ashwini, Snehangsu
# Description : Extracts the Status of the Project submissions 
#  either Started / In-Progress / Submitted along with the users 
#  entity information
# -----------------------------------------------------------------

import json, sys, time, csv
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, re, argparse
import requests
import pyspark.sql.utils as ut
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import datetime
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from udf_func import *
from pyspark.sql.functions import element_at, split, col


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


root_path = config_path[0]
sys.path.append(root_path)

from lib.mongoLogs import insertLog , getLogs
from cloud_storage.cloud import MultiCloud

cloud_init = MultiCloud()

details = argparse.ArgumentParser(description='Pass the ProgramID')
details.add_argument('--program_id',metavar='--program_id', type=str, help='Program IDs', required=False)
args = details.parse_args()
program_unique_id = None
if args.program_id :
 program_Id = args.program_id
 program_unique_id  = ObjectId(program_Id)

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

#Check for duplicate
duplicate_checker = None
data_fixer = None
datasource_name = json.loads(config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec"))["spec"]["dataSchema"]["dataSource"]
try:

    # construct query for log 
    today = str(current_date)
    query = { "dataSource" : datasource_name , "taskCreatedDate" : today , "programId" : program_Id }
   
    logCheck = getLogs(query)

    if not logCheck['duplicateChecker']:
    #   not a duplicate run 
      duplicate_checker = logCheck['duplicateChecker']
    else:
    #   duplicate run 
      duplicate_checker = logCheck['duplicateChecker']
      infoLogger.info(f"ABORT: 'Duplicate-run' {datasource_name} for {program_unique_id}")
    #   check if data fix required 
      if logCheck['dataFixer']:
         data_fixer = True
      else:
        druid_id = logCheck['response']['taskId']
        druid_status = requests.get(f'{config.get("DRUID", "batch_url")}/{druid_id}/status')
        druid_status.raise_for_status()
        ingest_status = druid_status.json()["status"]["status"]
        if ingest_status == 'SUCCESS':
            # Check: Date is duplicate
            duplicate_checker = True
            infoLogger.info(f"ABORT: 'Duplicate-run' for {datasource_name}")
        else:
            # Check: Date duplicate but ingestion didn't get processed
            duplicate_checker = False
            data_fixer = True
except FileNotFoundError:
   pass

# FOLLOW: Exit of dupliate run
if duplicate_checker: 
    errorLogger.error("Duplicate Run -- ABORT")
    sys.exit()

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
datasource_name = json.loads(config.get("DRUID","project_injestion_spec"))["spec"]["dataSchema"]["dataSource"]

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
    for org in val:
        orgObj = {}
        if org["isSchool"] == False:
            orgObj['orgId'] = org['organisationId']
            orgObj['orgName'] = org["orgName"]
            orgarr.append(orgObj)
  return orgarr
orgInfo_udf = udf(orgName,orgSchema)

successLogger.debug(
        "Program started  " + str(datetime.datetime.now())
   )	   
infoLogger.info(f"START: {datasource_name} for {program_unique_id}\n")
spark = SparkSession.builder.appName("projects").getOrCreate()

sc = spark.sparkContext

successLogger.debug(
        "Mongo Query start time  " + str(datetime.datetime.now())
   )

if program_unique_id :
   project_query = {"$match": {"$and":[{"programId": program_unique_id},{"isAPrivateProgram": False},{"isDeleted":False}]}}
else :
   project_query = {"$match": {"$and":[{"isAPrivateProgram": False},{"isDeleted":False}]}} 

projects_cursorMongo = projectsCollec.aggregate(
    [project_query,
    {
        "$project": {
            "_id": {"$toString": "$_id"},
            "projectTemplateId": {"$toString": "$projectTemplateId"},
            "remarks":1,
            "attachments":1,
            "programId": {"$toString": "$programId"},
            "programInformation": {"name": 1},
            "tasks": 1,
            "status": 1,
            "userId": 1,
            "programExternalId": 1,
            "isAPrivateProgram": 1,
            "userRoleInformation": 1,
            "userProfile": 1,
            "certificate": 1
        }
    }]
)

successLogger.debug(
        "Mongo Query end time  " + str(datetime.datetime.now())
   )
projects_schema = StructType([
    StructField('_id', StringType(), True),
    StructField('projectTemplateId', StringType(), True),
    StructField('title', StringType(), True),
    StructField('programId', StringType(), True),
    StructField('programExternalId', StringType(), True),
    StructField(
        'programInformation',
        StructType([StructField('name', StringType(), True)])
    ),
    StructField('status', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('isAPrivateProgram', BooleanType(), True),
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

successLogger.debug(
        "Function call start time  " + str(datetime.datetime.now())
   )
func_return = recreate_task_data(projects_cursorMongo)
successLogger.debug(
        "Function return end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "RDD converstion start time  " + str(datetime.datetime.now())
   ) 
prj_rdd = spark.sparkContext.parallelize(list(func_return))
successLogger.debug(
        "RDD converstion end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "RDD to Dataframe conversion start time  " + str(datetime.datetime.now())
   )
projects_df = spark.createDataFrame(prj_rdd,projects_schema)
successLogger.debug(
        "RDD to Dataframe conversion end time  " + str(datetime.datetime.now())
   )
prj_rdd.unpersist()

successLogger.debug(
        "Flattening data start time  " + str(datetime.datetime.now())
   )
projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True ,
        "project imported from library"
    ).otherwise("user created project")
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
                 "project_evidence_status",
                 F.when(
                      size(F.col("attachments"))>=1,True
                 ).otherwise(False)
)

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
                regexp_replace(projects_df["exploded_taskarr"]["task_evidence"], "\n|\"", ""),
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
                regexp_replace(projects_df["exploded_taskarr"]["prj_evidence"], "\n|\"", ""),
                F.lit("'")
            )
    ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
)
   
projects_df = projects_df.withColumn("tasks",F.when((F.col("exploded_taskarr.tasks").isNotNull()) & (F.col("exploded_taskarr.tasks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.tasks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.tasks")))
projects_df = projects_df.withColumn("sub_task",F.when((F.col("exploded_taskarr.sub_task").isNotNull()) & (F.col("exploded_taskarr.sub_task")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.sub_task"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.sub_task")))	
projects_df = projects_df.withColumn("program_name",regexp_replace(F.col("programInformation.name"), "\n|\"", ""))
projects_df = projects_df.withColumn("task_remarks",F.when((F.col("exploded_taskarr.remarks").isNotNull()) & (F.col("exploded_taskarr.remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.remarks")))
projects_df = projects_df.withColumn("project_remarks",F.when((F.col("exploded_taskarr.prj_remarks").isNotNull()) & (F.col("exploded_taskarr.prj_remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.prj_remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.prj_remarks")))

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

pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'
projects_df = projects_df.withColumn('certificate_issued_on', F.regexp_replace(F.col("certificate.issuedOn"), pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))

projects_df = projects_df.withColumn('certificate_status_customised', F.when(((F.col("certificate.eligible").isNotNull()) & (F.col("certificate.eligible") == True) & (F.col("certificate.osid").isNotNull())),F.lit("Issued")).otherwise(F.lit("")))

projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["project_created_type"],
    projects_df["programId"].alias("program_id"),
    projects_df["programExternalId"].alias("program_externalId"),
    projects_df["program_name"],
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["parent_channel"],
    projects_df["private_program"],
    projects_df["certificate_status_customised"],
    projects_df["evidence_status"]    
)

successLogger.debug(
        "Flattening data end time  " + str(datetime.datetime.now())
   )
projects_df.unpersist()
projects_df_cols = projects_df_cols.dropDuplicates()

successLogger.debug(
        "Get Entities start time  " + str(datetime.datetime.now())
   )
entities_df = melt(prj_df_expl_ul,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
prj_df_expl_ul.unpersist()
entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId")))
entities_df = entities_df.withColumn("variable1",F.concat(F.col("type"),F.lit("_name")))
entities_df = entities_df.withColumn("variable2",F.concat(F.col("type"),F.lit("_code")))

entities_df_id=entities_df.groupBy("_id").pivot("variable").agg(first("id"))

entities_df_name=entities_df.groupBy("_id").pivot("variable1").agg(first("name"))

entities_df_value=entities_df.groupBy("_id").pivot("variable2").agg(first("value"))

entities_df_med=entities_df_id.join(entities_df_name,["_id"],how='outer')
entities_df_res=entities_df_med.join(entities_df_value,["_id"],how='outer')
entities_df_res=entities_df_res.drop('null')


entities_df.unpersist()
successLogger.debug(
        "Get Entities end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "Final Dataframe start time  " + str(datetime.datetime.now())
   )
projects_df_final = projects_df_cols.join(entities_df_res,projects_df_cols["project_id"]==entities_df_res["_id"],how='left')\
        .drop(entities_df_res["_id"])
successLogger.debug(
        "Final Dataframe end time  " + str(datetime.datetime.now())
   )
entities_df_res.unpersist()
projects_df_cols.unpersist()
final_projects_df = projects_df_final.dropDuplicates()

necessary_columns = ["state_name","state_externalId","district_name","district_externalId","block_name",
                    "block_externalId","organisation_name","organisation_id"]
final_df_columns = final_projects_df.columns
for miss_cols in necessary_columns:
    if miss_cols not in final_df_columns:
        infoLogger.info(f"MISSED: {miss_cols}")
        final_projects_df = final_projects_df.withColumn(miss_cols, lit(None).cast(StringType()))        
        infoLogger.info(f"UPDATED: {final_projects_df.columns}")

projects_df_final.unpersist()

successLogger.debug(
        "Logic for submission distinct count by program level start time  " + str(datetime.datetime.now())
)
final_projects_tasks_distinctCnt_prgmlevel = final_projects_df.groupBy("program_name", "program_id","status_of_project", "state_name","state_externalId","private_program","project_created_type","parent_channel").agg(countDistinct(when(F.col("certificate_status_customised") == "Issued",True),F.col("project_id")).alias("no_of_certificate_issued"), countDistinct(F.col("project_id")).alias("unique_projects"),countDistinct(F.col("createdBy")).alias("unique_users"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "submitted"),True),F.col("project_id")).alias("no_of_imp_with_evidence"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "inProgress"),True),F.col("project_id")).alias("no_of_imp_with_evidence_inprogress"))

# Add6.0 date fixer
if not data_fixer:
    final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.withColumn("time_stamp", current_timestamp())
else:
    final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.withColumn("time_stamp", lit(date_format(date_sub(current_timestamp(), 1), "yyyy-MM-dd HH:mm:ss.SSS")))

final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.dropDuplicates()
final_projects_tasks_distinctCnt_prgmlevel.coalesce(1).write.format("json").mode("overwrite").save(
config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/"
)
successLogger.debug(
        "Logic for submission distinct count by program level end time  " + str(datetime.datetime.now())
)

final_projects_df.unpersist()
final_projects_tasks_distinctCnt_prgmlevel.unpersist()


successLogger.debug("Renaming file start time  " + str(datetime.datetime.now()))

#projects submission distinct count by program level
for filename in os.listdir(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")+"/"):
    if filename.endswith(".json"):
       if program_unique_id : 
        os.rename(
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/" + filename,
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + f"/ml_projects_distinctCount_prgmlevel_{program_unique_id}.json"
        )
       else :
        os.rename(
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/" + filename,
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json"
        )

successLogger.debug("Renaming file end time  " + str(datetime.datetime.now())) 
successLogger.debug("Uploading to Azure start time  " + str(datetime.datetime.now()))


#projects submission distinct count program level
local_distinctCnt_prgmlevel_path = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")
blob_distinctCnt_prgmlevel_path = config.get("COMMON", "projects_distinctCnt_prgmlevel_blob_path")

for files in os.listdir(local_distinctCnt_prgmlevel_path):
    if "ml_projects_distinctCount_prgmlevel.json" in files or f"ml_projects_distinctCount_prgmlevel_{program_unique_id}.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_distinctCnt_prgmlevel_path, local_Path = local_distinctCnt_prgmlevel_path, file_Name = files)

successLogger.debug("Uploading to azure end time  " + str(datetime.datetime.now()))	
successLogger.debug("Removing file start time  " + str(datetime.datetime.now()))

if program_unique_id :
 #projects submission distinct count program level
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + f"/ml_projects_distinctCount_prgmlevel_{program_unique_id}.json")
else :
 #projects submission distinct count program level
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json")

successLogger.debug("Removing file end time  " + str(datetime.datetime.now()))

druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

successLogger.debug("Ingestion start time  " + str(datetime.datetime.now()))

#projects submission distinct count program level
ml_distinctCnt_prgmlevel_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec"))
ml_distinctCnt_prgmlevel_projects_datasource = ml_distinctCnt_prgmlevel_projects_spec["spec"]["dataSchema"]["dataSource"]

if program_unique_id:
    current_cloud = re.split("://+", ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[0]
    uri = re.split("://+", ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[1]
    edited_uri = re.split(".json", uri)[0]
    ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
    ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"].update({"appendToExisting":True})
    

distinctCnt_prgmlevel_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_prgmlevel_projects_spec), headers=headers)

new_row = {
    "programId" : program_Id ,
    "dataSource": ml_distinctCnt_prgmlevel_projects_datasource,
    "taskId": distinctCnt_prgmlevel_projects_start_supervisor.json()["task"],
    "taskCreatedDate" : str(datetime.datetime.now().date())
}

new_row['statusCode'] = distinctCnt_prgmlevel_projects_start_supervisor.status_code

insertLog(new_row)

# with open(f'{config.get("COMMON","projects_tracker_prglvl_path")}', 'a', newline='') as file:
#     writer = csv.DictWriter(file, fieldnames=["program_id", "datasource", "task_id", "task_created_date"])
#     writer.writerow(new_row)

if distinctCnt_prgmlevel_projects_start_supervisor.status_code == 200:
    infoLogger.info("Successfully Ingested for {ml_distinctCnt_prgmlevel_projects_datasource}")
    successLogger.debug("started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_prgmlevel_projects_datasource)
else:
    errorLogger.error(
            "failed to start batch ingestion task of ml-project-programLevel-status " + str(distinctCnt_prgmlevel_projects_start_supervisor.status_code)
    )
    errorLogger.error(distinctCnt_prgmlevel_projects_start_supervisor.text)
    