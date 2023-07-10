# -----------------------------------------------------------------
# PYSPARK - OBSERVATION data restoration
# -----------------------------------------------------------------

import os
import logging
import requests
import datetime
import json, sys, time, re

import pyspark.sql.functions as F

# from udf_func import *
from datetime import date
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

from typing import Iterable
from pymongo import MongoClient
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from azure.storage.blob import ContentSettings
from pyspark.sql.functions import element_at, split, col
from configparser import ConfigParser,ExtendedInterpolation
from azure.storage.blob import BlockBlobService, PublicAccess
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

# Gathering the config details	
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the Success message handler to the logger	
successHandler = RotatingFileHandler(config.get('LOGS','observation_status_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_status_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Add the Error message handler to the logger	
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS','observation_status_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_status_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)



try:
 def melt(df: DataFrame,id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:
    '''Function to explode a dataframe, separating bracketed columns into indivisual columns'''
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

# Catching the schema for Organisation
orgSchema = ArrayType(StructType([
    StructField("orgId", StringType(), False),
    StructField("orgName", StringType(), False)
]))

def orgName(val):
    '''Function to get organisation name based on a key'''
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

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
userRolesCollec = db[config.get("MONGO", 'user_roles_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]

# Querying the Mongo DB collection
obs_sub_cursorMongo = obsSubmissionsCollec.aggregate(
   [{
      "$project": {
         "_id": {"$toString": "$_id"},
         "entityId": {"$toString": "$entityId"},
         "status": 1,
         "entityExternalId": 1,
         "entityInformation": {"name": 1},
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

# Writing the schema for Pyspark	    
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
         'entityInformation', 
         StructType([StructField('name', StringType(), True)])
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

spark = SparkSession.builder.appName(
   "obs_sub_status"
).config(
   "spark.driver.memory", "50g"
).config(
   "spark.executor.memory", "100g"
).config(
   "spark.memory.offHeap.enabled", True
).config(
   "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc=spark.sparkContext

obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo))
obs_sub_df1 = spark.createDataFrame(obs_sub_rdd,obs_sub_schema)


#------------------- Writing logics for columns that requires logic -------------#	
obs_sub_df1 = obs_sub_df1.withColumn("status_code", 	
F.when(	
    obs_sub_df1["status"] == "started", 1).when(	
    obs_sub_df1["status"] == "inProgress", 3).when(	
    obs_sub_df1["status"] == "ratingPending", 5).when(	
    obs_sub_df1["status"] == "completed", 7).otherwise(0)	
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
      "observation_with_out_rubric"
   ).when(
      (obs_sub_df1["isRubricDriven"].isNotNull() == True) & 
      (obs_sub_df1["isRubricDriven"] == False), 
      "observation_with_out_rubric"
   ).otherwise("observation_with_out_rubric")
)

#------------------- END -- Writing logics for columns that requires logic -------------#	

obs_sub_df1 = obs_sub_df1.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
obs_sub_df1 = obs_sub_df1.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
obs_sub_df1 = obs_sub_df1.withColumn("parent_channel",F.lit("SHIKSHALOKAM"))

obs_sub_expl_ul = obs_sub_df1.withColumn(
   "exploded_userLocations",F.explode_outer(obs_sub_df1["userProfile"]["userLocations"])
)

obs_sub_df = obs_sub_df1.select(
   "status",
   obs_sub_df1["status_code"], 
   obs_sub_df1["createdBy"].alias("user_id"),
   obs_sub_df1["solutionId"].alias("solution_id"),
   obs_sub_df1["_id"].alias("submission_id"),
   obs_sub_df1["entityInformation"]["name"].alias("entity_name"),
   "completedDate",
   obs_sub_df1["programId"].alias("program_id"),
   obs_sub_df1["private_program"],
   obs_sub_df1["solution_type"],
   "updatedAt",
   obs_sub_df1["userRoleInformation"]["role"].alias("role_title"),
   obs_sub_df1["userProfile"]["rootOrgId"].alias("channel"),
   obs_sub_df1["parent_channel"],
   concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
   obs_sub_df1["exploded_orgInfo"]["orgId"].alias("organisation_id"),
   obs_sub_df1["exploded_orgInfo"]["orgName"].alias("organisation_name"),
   concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type")
)
obs_sub_rdd.unpersist()
obs_sub_df1.unpersist()
obs_sub_cursorMongo.close()


# Gathering data to merge them ino the final dataframe
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
   config.get("OUTPUT_DIR", "observation_status_rollup")+"/"
)

final_df.unpersist()

# Storing data in local
for filename in os.listdir(config.get("OUTPUT_DIR", "observation_status_rollup")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "observation_status_rollup") + "/" + filename, 
         config.get("OUTPUT_DIR", "observation_status_rollup") + "/observation_status_rollup.json"
      )

# Storing data in Azure
blob_service_client = BlockBlobService(
   account_name=config.get("AZURE", "account_name"), 
   sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "observation_status_rollup")
blob_path = config.get("AZURE", "observation_rollup_blob_path")

for files in os.listdir(local_path):
   if "observation_status_rollup.json" in files:
      blob_service_client.create_blob_from_path(
         container_name,
         os.path.join(blob_path,files),
         local_path + "/" + files
      )

sl_status_spec = {}
sl_status_spec = json.loads(config.get("DRUID","observation_status_rollup_injestion_spec"))
datasources = [sl_status_spec["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(sl_status_spec)]

druid_batch_end_point = config.get("DRUID", "batch_rollup_url")
headers = {'Content-Type': 'application/json'}

for i,j in zip(datasources,ingestion_specs):
   druid_end_point = config.get("DRUID", "metadata_rollup_url") + i
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
            time.sleep(300)

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 204:
               successLogger.debug("successfully enabled the datasource " + i)

               time.sleep(300)

               start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
               successLogger.debug("ingest data")
               if start_supervisor.status_code == 200:
                  successLogger.debug(
                     "started the batch ingestion task sucessfully for the datasource " + i
                  )
                  time.sleep(50)
               else:
                  errorLogger.error(
                     "failed to start batch ingestion task" + str(start_supervisor.status_code)
                  )    
            else:
               errorLogger.error("failed to enable the datasource " + i)
         else:
            errorLogger.error("failed to delete the segments of the datasource " + i)
      else:
         errorLogger.error("failed to disable the datasource " + i)

   elif get_timestamp.status_code == 204:
      start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
      if start_supervisor.status_code == 200:
         successLogger.debug(
            "started the batch ingestion task sucessfully for the datasource " + i
         )
         time.sleep(50)
      else:
         errorLogger.error(
            "failed to start batch ingestion task" + str(start_supervisor.status_code)
         )
         errorLogger.error(start_supervisor.json())
   else:
      errorLogger.error("failed to get the timestamp of the datasource " + i)

#observation submission distinct count
ml_distinctCnt_obs_status_spec = json.loads(config.get("DRUID","ml_distinctCnt_obs_status_spec"))
ml_distinctCnt_obs_status_datasource = ml_distinctCnt_obs_status_spec["spec"]["dataSchema"]["dataSource"]
distinctCnt_obs_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_obs_status_spec), headers=headers)
if distinctCnt_obs_start_supervisor.status_code == 200:
   successLogger.debug(
        "started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_obs_status_datasource
   )
   time.sleep(50)
else:
   errorLogger.error(
        "failed to start batch ingestion task" + str(distinctCnt_obs_start_supervisor.status_code)
   )
   errorLogger.error(distinctCnt_obs_start_supervisor.json())

#observation domain distinct count
ml_distinctCnt_obs_domain_spec = json.loads(config.get("DRUID","ml_distinctCnt_obs_domain_spec"))
ml_distinctCnt_obs_domain_datasource = ml_distinctCnt_obs_domain_spec["spec"]["dataSchema"]["dataSource"]
distinctCnt_obs_domain_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_obs_domain_spec), headers=headers)
if distinctCnt_obs_domain_start_supervisor.status_code == 200:
   successLogger.debug(
        "started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_obs_domain_datasource
   )
   time.sleep(50)
else:
   errorLogger.error(
        "failed to start batch ingestion task" + str(distinctCnt_obs_domain_start_supervisor.status_code)
   )
   errorLogger.error(distinctCnt_obs_domain_start_supervisor.json())

#observation domain criteria distinct count
ml_distinctCnt_obs_domain_criteria_spec = json.loads(config.get("DRUID","ml_distinctCnt_obs_domain_criteria_spec"))
ml_distinctCnt_obs_domain_criteria_datasource = ml_distinctCnt_obs_domain_criteria_spec["spec"]["dataSchema"]["dataSource"]
distinctCnt_obs_domain_criteria_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_obs_domain_criteria_spec), headers=headers)
if distinctCnt_obs_domain_criteria_start_supervisor.status_code == 200:
   successLogger.debug(
        "started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_obs_domain_criteria_datasource
   )
   time.sleep(50)
else:
   errorLogger.error(
        "failed to start batch ingestion task" + str(distinctCnt_obs_domain_criteria_start_supervisor.status_code)
   )
   errorLogger.error(distinctCnt_obs_domain_criteria_start_supervisor.json())
