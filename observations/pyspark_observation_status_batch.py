# -----------------------------------------------------------------
# Name : pyspark_observation_status_batch.py
# Author : Shakthieshwari.A
# Description : Extracts the Status of the observation submissions 
#  either notStarted / In-Progress / Completed along with the users 
#  entity information
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time , re
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
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import element_at, split, col


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


root_path = config_path[0]
sys.path.append(root_path)
from lib.database import Database
from cloud_storage.cloud import MultiCloud
cloud_init = MultiCloud()
from lib.logsHandler import Logs
dataBase = Database() 

logHandler = Logs(config.get('LOGS', 'observation_status_success_error'))

file_name_for_output_log = logHandler.constructOutputLogFile()
file_name_for_debug_log = logHandler.constructDebugLogFile()

logHandler.flushLogs()

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

try:
   def removeduplicate(it):
      seen = []
      for x in it:
         if x not in seen:
            yield x
            seen.append(x)
except Exception as e:
   errorLogger.error(e, exc_info=True)

try:
   def chunks(l, n):
      for i in range(0, len(l), n):
         yield l[i:i + n]
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
   def convert_to_row(d: dict) -> Row:
      return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
   errorLogger.error(e,exc_info=True)

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
    StructField("orgId", StringType(), True),
    StructField("orgName", StringType(), True)
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

def observed_data(vts, entype):
    gathered_entities = []
    entity_breakdown = {}
    entity_breakdown[f"observed_{entype}_name"] = vts["name"]
    try:
        entity_breakdown[f"observed_{entype}_id"] = vts["registryDetails"]["locationId"]
        entity_breakdown[f"observed_{entype}_code"] = vts["registryDetails"]["code"]
    except TypeError:
        entity_breakdown[f"observed_{entype}_id"] = ''
        entity_breakdown[f"observed_{entype}_code"] = ''
    if vts["hierarchy"] is not None:
        for val in vts["hierarchy"]:
            entity_breakdown[f"observed_{val['type']}_id"] = val['id']
            entity_breakdown[f"observed_{val['type']}_name"] = val['name']
            entity_breakdown[f"observed_{val['type']}_code"] = val['code'] 
    return entity_breakdown

entity_observed = udf(lambda x,y:observed_data(x,y), StructType([
                                                         StructField("observed_block_name", StringType(), True),
                                                         StructField("observed_block_code", StringType(), True),
                                                         StructField("observed_block_id", StringType(), True),
                                                         StructField("observed_district_name", StringType(), True),
                                                         StructField("observed_district_code", StringType(), True),
                                                         StructField("observed_district_id", StringType(), True),
                                                         StructField("observed_state_name", StringType(), True),
                                                         StructField("observed_state_code", StringType(), True),
                                                         StructField("observed_state_id", StringType(), True),
                                                         StructField("observed_school_name", StringType(), True),
                                                         StructField("observed_school_code", StringType(), True),
                                                         StructField("observed_school_id", StringType(), True),
                                                         StructField("observed_cluster_name", StringType(), True),
                                                         StructField("observed_cluster_code", StringType(), True),
                                                         StructField("observed_cluster_id", StringType(), True)
]))


db = dataBase.connection()


obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
userRolesCollec = db[config.get("MONGO", 'user_roles_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]

datasource_name = json.loads(config.get("DRUID","observation_status_injestion_spec"))["spec"]["dataSchema"]["dataSource"]	
infoLogger.info(f"START: For {datasource_name} ")


#observation submission dataframe
obs_sub_cursorMongo = obsSubmissionsCollec.aggregate(
   [{"$match": {"$and":[{"isAPrivateProgram": False},{"deleted":False}]}},
    { "$project": {
         "_id": {"$toString": "$_id"},
         "entityId": {"$toString": "$entityId"},
         "status": 1,
         "entityExternalId": 1,
         "entityInformation": {"name": 1,"registryDetails":1,"hierarchy":1},
         "entityType": 1,
         "createdBy": 1,
         "solutionId": {"$toString": "$solutionId"},
         "solutionExternalId": 1,
         "updatedAt": 1, 
         "completedDate": 1,
         "programId": {"$toString": "$programId"},
         "programExternalId": 1,
         "appInformation": {"appName": 1},
         "isAPrivateProgram": 1,
         "isRubricDriven":1,
         "criteriaLevelReport":1,
         "ecm_marked_na": {
            "$reduce": {
               "input": "$evidencesStatus",
               "initialValue": "",
               "in": {
                  "$cond": [
                     {"$eq": [{"$toBool":"$$this.notApplicable"},True]}, 
                     {"$concat" : ["$$value", "$$this.name", ";"]}, 
                     "$$value"
                  ]
               }
            }
         },
         "userRoleInformation": 1,
         "userProfile": 1,
         "themes": 1,
         "criteria": 1
      }},
      {"$addFields":
          {"themes": {"$map":  {"input": "$themes","as": "r","in":{"name":"$$r.name","pointsBasedLevel":"$$r.pointsBasedLevel","externalId":"$$r.externalId","criteria":{"$map": {"input": "$$r.criteria","as": "s","in":{"criteriaId":{"$toString": "$$s.criteriaId"}}}}}}},
          "criteria": {"$map": {"input": "$criteria","as": "r","in":{"name":"$$r.name","score":"$$r.score","_id":{"$toString": "$$r._id"},"parentCriteriaId":{"$toString": "$$r.parentCriteriaId"}}}}}}
    ]
)

#schema for the observation submission dataframe

obs_sub_schema = StructType(
   [
      StructField('status', StringType(), True),
      StructField('entityExternalId', StringType(), True),
      StructField('entityId', StringType(), True),
      StructField('entityType', StringType(), True),
      StructField('createdBy', StringType(), True),
      StructField('solutionId', StringType(), True),
      StructField('solutionExternalId', StringType(), True),
      StructField('programId', StringType(), True),
      StructField('programExternalId', StringType(), True),
      StructField('_id', StringType(), True),
      StructField('updatedAt', TimestampType(), True),
      StructField('completedDate', TimestampType(), True),
      StructField('isAPrivateProgram', BooleanType(), True),
      StructField(
         'entityInformation', StructType([
                StructField('name', StringType(), True),
                StructField('registryDetails', StructType([
                    StructField('code', StringType(), True),
                    StructField('locationId', StringType(), True)
                ]),True),
                StructField('hierarchy',ArrayType(
                    StructType([
                        StructField('code', StringType(), True),
                        StructField('name', StringType(), True),
                        StructField('id', StringType(), True),
                        StructField('type', StringType(), True)
                ]), True),True)])
      ),
      StructField(
         'appInformation',
         StructType([StructField('appName', StringType(), True)])
      ),
      StructField('isRubricDriven',StringType(),True),
      StructField('criteriaLevelReport',StringType(),True),
      StructField('ecm_marked_na', StringType(), True),
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
      StructField(
            'themes',ArrayType(
                     StructType([
                           StructField('name', StringType(), True),
                           StructField('pointsBasedLevel', StringType(), True),
                           StructField('externalId', StringType(), True),
                           StructField('criteria',ArrayType(
                                 StructType([
                                       StructField('criteriaId', StringType(), True)
                                 ])
                           ))
                     ]), True)
      ),
      StructField(
            'criteria',ArrayType(
                       StructType([
                             StructField('name', StringType(), True),
                             StructField('score', StringType(), True),
                             StructField('parentCriteriaId', StringType(), True),
                             StructField('_id', StringType(), True)
                       ]), True)
      )
   ]
)

spark = SparkSession.builder.appName("obs_sub_status").getOrCreate()

sc=spark.sparkContext


obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo))
obs_sub_df1 = spark.createDataFrame(obs_sub_rdd,obs_sub_schema)

obs_sub_df1 = obs_sub_df1.withColumn(
   "app_name", 
   F.when(
      obs_sub_df1["appInformation"]["appName"].isNull(), 
      F.lit(config.get("ML_APP_NAME", "survey_app"))
   ).otherwise(
      lower(obs_sub_df1["appInformation"]["appName"])
   )
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "private_program", 
   F.when(
      (obs_sub_df1["isAPrivateProgram"].isNotNull() == True) & 
      (obs_sub_df1["isAPrivateProgram"] == True),
      "true"
   ).when(
      (obs_sub_df1["isAPrivateProgram"].isNotNull() == True) & 
      (obs_sub_df1["isAPrivateProgram"] == False),
      "false"
   ).otherwise("true")
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "solution_type",
   F.when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) & 
      (obs_sub_df1["isRubricDriven"] == True) &
      (obs_sub_df1["criteriaLevelReport"] == True),
      "observation_with_rubric"
   ).when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) &
      (obs_sub_df1["isRubricDriven"] == True) &
      (obs_sub_df1["criteriaLevelReport"] == False),
      "observation_with_rubric_no_criteria_level_report"
   ).when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) & 
      (obs_sub_df1["isRubricDriven"] == False), 
      "observation_with_out_rubric"
   ).otherwise("observation_with_out_rubric")
)

obs_sub_df1 = obs_sub_df1.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
obs_sub_df1 = obs_sub_df1.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
obs_sub_df1 = obs_sub_df1.withColumn("observedData", entity_observed(F.col("entityInformation"), F.col("entityType")))
obs_sub_df1 = obs_sub_df1.withColumn("parent_channel",F.lit("SHIKSHALOKAM"))

obs_sub_expl_ul = obs_sub_df1.withColumn(
   "exploded_userLocations",F.explode_outer(obs_sub_df1["userProfile"]["userLocations"])
)

obs_sub_df = obs_sub_df1.select(
   "status", 
   obs_sub_df1["entityExternalId"].alias("entity_externalId"),
   obs_sub_df1["entityId"].alias("entity_id"),
   obs_sub_df1["entityType"].alias("entity_type"),
   obs_sub_df1["createdBy"].alias("user_id"),
   obs_sub_df1["solutionId"].alias("solution_id"),
   obs_sub_df1["solutionExternalId"].alias("solution_externalId"),
   obs_sub_df1["_id"].alias("submission_id"),
   obs_sub_df1["entityInformation"]["name"].alias("entity_name"),
   "completedDate",
   obs_sub_df1["programId"].alias("program_id"),
   obs_sub_df1["programExternalId"].alias("program_externalId"),
   obs_sub_df1["app_name"],
   obs_sub_df1["private_program"],
   obs_sub_df1["solution_type"],
   obs_sub_df1["ecm_marked_na"],
   "updatedAt",
   obs_sub_df1["userRoleInformation"]["role"].alias("role_title"),
   obs_sub_df1["userProfile"]["rootOrgId"].alias("channel"),
   obs_sub_df1["parent_channel"],
   concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
   obs_sub_df1["exploded_orgInfo"]["orgId"].alias("organisation_id"),
   obs_sub_df1["exploded_orgInfo"]["orgName"].alias("organisation_name"),
   obs_sub_df1["observedData"]["observed_block_name"].alias("observed_block_name"),
   obs_sub_df1["observedData"]["observed_block_id"].alias("observed_block_id"),
   obs_sub_df1["observedData"]["observed_block_code"].alias("observed_block_code"),
   obs_sub_df1["observedData"]["observed_district_name"].alias("observed_district_name"),
   obs_sub_df1["observedData"]["observed_district_id"].alias("observed_district_id"),
   obs_sub_df1["observedData"]["observed_district_code"].alias("observed_district_code"),
   obs_sub_df1["observedData"]["observed_state_name"].alias("observed_state_name"),
   obs_sub_df1["observedData"]["observed_state_id"].alias("observed_state_id"),
   obs_sub_df1["observedData"]["observed_state_code"].alias("observed_state_code"),
   obs_sub_df1["observedData"]["observed_cluster_name"].alias("observed_cluster_name"),
   obs_sub_df1["observedData"]["observed_cluster_id"].alias("observed_cluster_id"),
   obs_sub_df1["observedData"]["observed_cluster_code"].alias("observed_cluster_code"),
   obs_sub_df1["observedData"]["observed_school_name"].alias("observed_school_name"),
   obs_sub_df1["observedData"]["observed_school_id"].alias("observed_school_id"),
   obs_sub_df1["observedData"]["observed_school_code"].alias("observed_school_code"),
   obs_sub_df1["themes"],obs_sub_df1["criteria"],
   concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type"),
   obs_sub_df1["isRubricDriven"],
   obs_sub_df1["criteriaLevelReport"]
)
obs_sub_rdd.unpersist()
obs_sub_df1.unpersist()
obs_sub_cursorMongo.close()

entities_df = melt(obs_sub_expl_ul,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
obs_sub_expl_ul.unpersist()
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
obs_sub_df_final = obs_sub_df.join(entities_df_res,obs_sub_df["submission_id"]==entities_df_res["_id"],how="left")\
        .drop(entities_df_res["_id"])
obs_sub_df.unpersist()
entities_df_res.unpersist()

#observation solution dataframe
obs_sol_cursorMongo = solutionCollec.aggregate(
   [
      {"$match": {"type":"observation"}},
      {"$project": {"_id": {"$toString": "$_id"}, "name":1}}
   ]
)

#schema for the observation solution dataframe
obs_sol_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
obs_soln_df = spark.createDataFrame(obs_soln_rdd,obs_sol_schema)
obs_soln_rdd.unpersist()
obs_sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_soln_df = obs_sub_df_final.join(
   obs_soln_df,
   obs_sub_df_final.solution_id==obs_soln_df._id,
   'inner'
).drop(obs_soln_df["_id"])
obs_soln_df.unpersist()
obs_sub_df_final.unpersist()
obs_sub_soln_df = obs_sub_soln_df.withColumnRenamed("name", "solution_name")

#observation program dataframe
obs_pgm_cursorMongo = programCollec.aggregate(
   [{"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
)

#schema for the observation program dataframe
obs_pgm_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo))
obs_pgm_df = spark.createDataFrame(obs_pgm_rdd,obs_pgm_schema)
obs_pgm_rdd.unpersist()
obs_pgm_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_pgm_df = obs_sub_soln_df.join(
   obs_pgm_df,
   obs_sub_soln_df.program_id==obs_pgm_df._id,
   'inner'
).drop(obs_pgm_df["_id"])
obs_pgm_df.unpersist()
obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")

obs_sub_soln_df.unpersist()

final_df = obs_sub_pgm_df.dropDuplicates()
obs_sub_pgm_df.unpersist()
final_df.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR", "observation_status")+"/"
)

final_df.unpersist()



for filename in os.listdir(config.get("OUTPUT_DIR", "observation_status")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "observation_status") + "/" + filename, 
         config.get("OUTPUT_DIR", "observation_status") + "/sl_observation_status.json"
      )

local_path = config.get("OUTPUT_DIR", "observation_status")
blob_path = config.get("COMMON", "observation_blob_path")


fileList = []

for files in os.listdir(local_path):
   if "sl_observation_status.json" in files:
      fileList.append("sl_observation_status.json")


# Uploading local file to cloud by calling upload_to_cloud fun.
uploadResponse = cloud_init.upload_to_cloud(filesList = fileList ,folderPathName = "observation_blob_path", local_Path = local_path )
successLogger.debug(
                    "cloud upload response : " + str(uploadResponse)
                  )

# if file uploading fails exiting the program
if uploadResponse['success'] == False:
   sys.exit() 


sl_status_spec = {}

successLogger.debug(
                    sl_status_spec["spec"]["ioConfig"]["inputSource"]["type"] + "\n" +
                    str(sl_status_spec["spec"]["ioConfig"]["inputSource"]["uris"]) + "\n" +
                    str(sl_status_spec)
                  )

datasources = json.loads(config.get("DRUID","observation_status_injestion_spec"))["spec"]["dataSchema"]["dataSource"]

ingestion_specs = json.loads(config.get("DRUID","observation_status_injestion_spec"))

druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}


for i,j in zip(datasources,ingestion_specs):
   druid_end_point = config.get("DRUID", "metadata_url") + i
   
   get_timestamp = requests.get(druid_end_point, headers=headers)
   successLogger.debug(get_timestamp)
   if get_timestamp.status_code == 200 :
      successLogger.debug("Successfully fetched time stamp of the datasource " + i)
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
      successLogger.debug(interval)

      time.sleep(50)

      disable_datasource = requests.delete(druid_end_point, headers=headers)
      if disable_datasource.status_code == 200:
         successLogger.debug("successfully disabled the datasource " + i)
         time.sleep(300)

         delete_segments = requests.delete(
            druid_end_point + "/intervals/" + interval, headers=headers
         )
         if delete_segments.status_code == 200:
            successLogger.debug("successfully deleted the segments " + i)
            time.sleep(600)

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 204 or enable_datasource.status_code == 200:
               time.sleep(600)
               successLogger.debug("successfully enabled the datasource " + i)

               start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
               successLogger.debug("ingest data")
               if start_supervisor.status_code == 200:
                  successLogger.debug(
                     "started the batch ingestion task sucessfully for the datasource " + i
                  )
                  time.sleep(50)
               else:
                  errorLogger.error("failed to start batch ingestion task" + i)
                  errorLogger.error(
                     "failed to start batch ingestion task" + str(start_supervisor.status_code)
                  ) 
                  errorLogger.error(start_supervisor.text)

            else:
               errorLogger.error("failed to enable the datasource " + i)
               errorLogger.error(
                    "failed to enable the datasource " + str(enable_datasource.status_code)
               )
               errorLogger.error(enable_datasource.text)
         else:
            errorLogger.error("failed to delete the segments of the datasource " + i)
            errorLogger.error(
                "failed to delete the segments of the datasource " + str(delete_segments.status_code)
            )
            errorLogger.error(delete_segments.text)
      else:
         errorLogger.error("failed to disable the datasource " + i)
         errorLogger.error(
                "failed to disable the datasource " + str(disable_datasource.status_code)
         )
         errorLogger.error(disable_datasource.text)

   elif get_timestamp.status_code == 204:
      start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
      if start_supervisor.status_code == 200:
         successLogger.debug(
            "started the batch ingestion task sucessfully for the datasource " + i
         )
         time.sleep(50)
      else:
         errorLogger.error("failed to start batch ingestion task" + i)
         errorLogger.error(
            "failed to start batch ingestion task" + str(start_supervisor.status_code)
         )
         errorLogger.error(start_supervisor.text)
   else:
      errorLogger.error("failed to get the timestamp of the datasource " + i)
      errorLogger.error(
                "failed to get the timestamp of the datasource " + str(get_timestamp.status_code)
      )
      errorLogger.error(get_timestamp.text)

