# -----------------------------------------------------------------
# Name : pyspark_sur_dist_count_status.py
# Author : Prashant
# Description : Extracts the Status of the survey submissions 
#  either notStarted / In-Progress / Completed 
#  along distinctCnt data for Survey with the users and submissions. 
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time, re
import datetime
from datetime import date
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
from typing import Iterable
import logging
from datetime import date
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

root_path = config_path[0]
sys.path.append(root_path)

from cloud_storage.cloud import MultiCloud
cloud_init = MultiCloud()

sys.path.append(config.get("COMMON","cloud_module_path"))

# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'survey_streaming_success_error')
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

# Add loggers
formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# handler for output and debug Log
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


datasource_name = json.loads(config.get("DRUID","ml_distinctCnt_survey_status_spec"))["spec"]["dataSchema"]["dataSource"]

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

infoLogger.info(f"*********** Survey Batch Ingestion STARTED AT: {datetime.datetime.now()} ***********\n")

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
surveySubCollec = db[config.get('MONGO', 'survey_submissions_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]

survey_sub_cursorMongo = surveySubCollec.aggregate(
        [{"$match": {"$and":[{"isAPrivateProgram": False},{"deleted":False}]}}, 
        {"$project": {
         "_id": {"$toString": "$_id"},
         "surveyId": {"$toString": "$surveyId"},
         "status": 1,
         "surveyExternalId": 1,
         "updatedAt": 1, 
         "completedDate": 1,
         "createdAt": 1,
         "createdBy": 1,
         "solutionId": {"$toString": "$solutionId"},
         "solutionExternalId": 1,
         "programId": {"$toString": "$programId"},
         "programExternalId": 1,
         "appInformation": {"appName": 1},
         "surveyInformation": {"name": 1},
         "isAPrivateProgram": 1,
         "isRubricDriven":1,
         "criteriaLevelReport":1,
         "userRoleInformation": 1,
         "userProfile": 1,
      }
   }]
)

survey_sub_schema = StructType(
   [
      StructField('status', StringType(), True),
      StructField('surveyId', StringType(), True),
      StructField('surveyExternalId', StringType(), True),
      StructField('entityType', StringType(), True),
      StructField('createdBy', StringType(), True),
      StructField('solutionId', StringType(), True),
      StructField('solutionExternalId', StringType(), True),
      StructField('programId', StringType(), True),
      StructField('programExternalId', StringType(), True),
      StructField('_id', StringType(), True),
      StructField('updatedAt', TimestampType(), True),
      StructField('completedDate', TimestampType(), True),
      StructField('createdAt', TimestampType(), True),
      StructField('isAPrivateProgram', BooleanType(), True),
      StructField(
         'appInformation',
         StructType([StructField('appName', StringType(), True)])
      ),
      StructField(
         'surveyInformation',
         StructType([StructField('name', StringType(), True)]),
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
   ]
)


spark = SparkSession.builder.appName("survey_sub_status").getOrCreate()

sc=spark.sparkContext

survey_sub_rdd = spark.sparkContext.parallelize(list(survey_sub_cursorMongo))

sub_df = spark.createDataFrame(survey_sub_rdd,survey_sub_schema)

sub_df = sub_df.withColumn(
   "private_program",
   F.when(
      (sub_df["isAPrivateProgram"].isNotNull() == True) &
      (sub_df["isAPrivateProgram"] == True),
      "true"
   ).when(
      (sub_df["isAPrivateProgram"].isNotNull() == True) &
      (sub_df["isAPrivateProgram"] == False),
      "false"
   ).otherwise("true")
)

sub_df = sub_df.withColumn(
   "app_name", 
   F.when(
      sub_df["appInformation"]["appName"].isNull(), 
      F.lit(config.get("ML_APP_NAME", "survey_app"))
   ).otherwise(
      lower(sub_df["appInformation"]["appName"])
   )
)

sub_df = sub_df.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
sub_df = sub_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
sub_df = sub_df.withColumn("parent_channel",F.lit("SHIKSHALOKAM"))

sub_expl_ul = sub_df.withColumn(
   "exploded_userLocations",F.explode_outer(sub_df["userProfile"]["userLocations"])
)

sub_df1 = sub_df.select(
   sub_df["status"].alias("submission_status"), 
   sub_df["createdBy"].alias("user_id"),
   sub_df["solutionId"].alias("solution_id"),
   sub_df["solutionExternalId"].alias("survey_externalId"),
   sub_df["_id"].alias("survey_submission_id"),
   sub_df["surveyId"].alias("survey_id"),
   sub_df["createdAt"].alias("created_date"),
   sub_df["completedDate"].alias("submission_date"),
   sub_df["programId"].alias("program_id"),
   sub_df["programExternalId"].alias("program_externalId"),
   sub_df["app_name"],
   sub_df["private_program"],
   sub_df['surveyInformation']['name'].alias("survey_name"),
   "updatedAt",
   sub_df["userRoleInformation"]["role"].alias("user_sub_type"),
   sub_df["userProfile"]["rootOrgId"].alias("channel"),
   sub_df["parent_channel"],
   concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
   sub_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
   sub_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
   concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type")
)
survey_sub_rdd.unpersist()
sub_df.unpersist()
survey_sub_cursorMongo.close()

entities_df = melt(sub_expl_ul,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
sub_expl_ul.unpersist()
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
sub_df_final = sub_df1.join(entities_df_res,sub_df1["survey_submission_id"]==entities_df_res["_id"],how="left")\
        .drop(entities_df_res["_id"])
sub_df1.unpersist()
entities_df_res.unpersist()


#survey solution dataframe
sol_cursorMongo = solutionCollec.aggregate(
   [
      {"$match": {"type":"survey"}},
      {"$project": {"_id": {"$toString": "$_id"}, "name":1}}
   ]
)

#schema for the survey solution dataframe
sol_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

soln_rdd = spark.sparkContext.parallelize(list(sol_cursorMongo))
soln_df = spark.createDataFrame(soln_rdd,sol_schema)
soln_rdd.unpersist()
sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
sub_soln_df = sub_df_final.join(
   soln_df,
   sub_df_final.solution_id==soln_df._id,
   'left'
).drop(soln_df["_id"])
soln_df.unpersist()
sub_df_final.unpersist()
sub_soln_df = sub_soln_df.withColumnRenamed("name", "solution_name")

#survey program dataframe
pgm_cursorMongo = programCollec.aggregate(
   [{"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
)

#schema for the survey program dataframe
pgm_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

pgm_rdd = spark.sparkContext.parallelize(list(pgm_cursorMongo))

pgm_df = spark.createDataFrame(pgm_rdd,pgm_schema)
pgm_rdd.unpersist()
pgm_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
sub_pgm_df = sub_soln_df.join(
   pgm_df,
   sub_soln_df.program_id==pgm_df._id,
   'left'
).drop(pgm_df["_id"])
pgm_df.unpersist()
sub_pgm_df = sub_pgm_df.withColumnRenamed("name", "program_name")
sub_soln_df.unpersist()

final_df = sub_pgm_df.dropDuplicates()
sub_pgm_df.unpersist()


#survey submission distinct count DF
final_df_distinct_survey_status = final_df.groupBy("program_name","program_id","survey_name","survey_id","submission_status","state_name","state_externalId","district_name","district_externalId","block_name","block_externalId","organisation_name","organisation_id","private_program","parent_channel").agg(countDistinct(F.col("user_id")).alias("unique_users"),countDistinct(F.col("survey_submission_id")).alias("unique_submissions"))
final_df_distinct_survey_status = final_df_distinct_survey_status.withColumn("time_stamp", current_timestamp())
#saving as file
final_df_distinct_survey_status.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR","survey_distinctCount_status") + "/"
)

final_df.unpersist()
final_df_distinct_survey_status.unpersist()

#Changing the file name  
for filename in os.listdir(config.get("OUTPUT_DIR", "survey_distinctCount_status")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "survey_distinctCount_status") + "/" + filename,
         config.get("OUTPUT_DIR", "survey_distinctCount_status") + "/ml_survey_distinctCount_status.json"
      )

#defining the local and blob path 
local_distinctCount_path = config.get("OUTPUT_DIR", "survey_distinctCount_status")
blob_distinctCount_path = config.get("COMMON", "survey_distinctCount_blob_path")


fileList = []

#pusing JSON into Cloud
for files in os.listdir(local_distinctCount_path):
   if "ml_survey_distinctCount_status.json" in files:
      fileList.append("ml_survey_distinctCount_status.json")

# Uploading local file to cloud by calling upload_to_cloud fun.
uploadResponse = cloud_init.upload_to_cloud(blob_distinctCount_path, local_distinctCount_path,"ml_survey_distinctCount_status.json")


successLogger.debug(
                    "cloud upload response : " + str(uploadResponse)
                  )
infoLogger.info(f"successfully uploaded the ml_survey_distinctCount_status.json file to the cloud storage")

time.sleep(3)
ml_distinctCnt_survey_status_spec = {}

#get Druid spec from config
ml_distinctCnt_survey_status_spec = json.loads(config.get("DRUID","ml_distinctCnt_survey_status_spec"))

#Druid INFO
druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

datasources = [ml_distinctCnt_survey_status_spec["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(ml_distinctCnt_survey_status_spec)]

for i,j in zip(datasources,ingestion_specs):
   druid_end_point = config.get("DRUID", "metadata_url") + i
   get_timestamp = requests.get(druid_end_point, headers=headers)
   if get_timestamp.status_code == 200 :
      infoLogger.info(f"fetched timestamp of datasource {i}")
      timestamp = get_timestamp.json()
      #calculating interval from druid get api 
      minTime = timestamp["segments"]["minTime"]
      maxTime = timestamp["segments"]["maxTime"]
      min1 = datetime.datetime.strptime(minTime,"%Y-%m-%dT%H:%M:%S.%fZ")
      max1 = datetime.datetime.strptime(maxTime,"%Y-%m-%dT%H:%M:%S.%fZ")
      new_format = "%Y-%m-%d"
      min1.strftime(new_format)
      max1.strftime(new_format)
      minmonth = "{:02d}".format(min1.month)
      maxmonth = "{:02d}".format(max1.month)
      min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
      max2 = str(max1.year) + "-" + maxmonth  + "-" + str(max1.day)
      interval = min2 + "_" + max2
      time.sleep(50)

      disable_datasource = requests.delete(druid_end_point, headers=headers)
      if disable_datasource.status_code == 200:
         infoLogger.info(f"started deleting the segments from {i}")
         delete_segments = requests.delete(
            druid_end_point + "/intervals/" + interval, headers=headers
         )
         if delete_segments.status_code == 200:
            infoLogger.info(f"successfully deleted the segments between the interval : {interval}")

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 204 or enable_datasource.status_code == 200:
               infoLogger.info(f"started ingesting the aggregated data into the {i} ")

               start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
               if start_supervisor.status_code == 200:
                  infoLogger.info(f"successfully ingested the aggregated data into the {i}")
               else:
                  infoLogger.info(f"failed to ingest the data into {i} ")
            else:
                infoLogger.info(f"Failed to enable {i} | Error: {enable_datasource.status_code}")
         else:
             infoLogger.info(f"failed to delete the datasource {i}")
      else:
            infoLogger.info(f"failed to disable the datasource {i} ")
   elif get_timestamp.status_code == 204:
      infoLogger.info(f"Ingesting the data for the first time")
      start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
      if start_supervisor.status_code == 200:
         infoLogger.info(f"sucessfully ingested the data into the datasource {i}")
      else:
         infoLogger.info(f"Failed to start batch ingestion task in {i}")

infoLogger.info(f"*********** Survey Batch Ingestion COMPLETED AT: {datetime.datetime.now()} ***********\n")
