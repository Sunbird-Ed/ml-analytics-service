# -----------------------------------------------------------------
# Name : pyspark_survey_batch.py
# Author : Ashwini E
# Description : Extracts the Status of the survey submissions 
#  either notStarted / In-Progress / Completed along with the users 
#  entity information
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time
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
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import ContentSettings
from typing import Iterable
from slackclient import SlackClient

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
bot = SlackClient(config.get("SLACK","token"))

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
   print(e)

bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** Survey Batch Ingestion STARTED AT: {datetime.datetime.now()} ***********\n")
clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
surveySubCollec = db[config.get('MONGO', 'survey_submissions_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]

survey_sub_cursorMongo = surveySubCollec.aggregate(
        [{
       "$project": {
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
final_df.coalesce(1).write.format("json").mode("overwrite").save(
        config.get("OUTPUT_DIR", "survey_status")+"/"
        )

for filename in os.listdir(config.get("OUTPUT_DIR", "survey_status")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "survey_status") + "/" + filename,
         config.get("OUTPUT_DIR", "survey_status") + "/sl_survey_status.json"
      )

blob_service_client = BlockBlobService(
   account_name=config.get("AZURE", "account_name"), 
   sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "survey_status")
blob_path = config.get("AZURE", "survey_blob_path")

for files in os.listdir(local_path):
   if "sl_survey_status.json" in files:
      blob_service_client.create_blob_from_path(
         container_name,
         os.path.join(blob_path,files),
         local_path + "/" + files
      )

ml_status_spec = {}
ml_status_spec = json.loads(config.get("DRUID","survey_status_injestion_spec"))
datasources = [ml_status_spec["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(ml_status_spec)]

druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

for i,j in zip(datasources,ingestion_specs):
   druid_end_point = config.get("DRUID", "metadata_url") + i
   get_timestamp = requests.get(druid_end_point, headers=headers)
   if get_timestamp.status_code == 200 :
      bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Fetched Timestamp of {i} | Waiting for 50s")
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
         bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Disabled the datasource of {i} | Waiting for 50s")
         time.sleep(300)

         delete_segments = requests.delete(
            druid_end_point + "/intervals/" + interval, headers=headers
         )
         if delete_segments.status_code == 200:
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Deleted the segments for {i}")
            time.sleep(300)

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 204:
               bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Enabled the datasource for {i}")
               time.sleep(300)

               start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
               if start_supervisor.status_code == 200:
                  bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Succesfully ingested the data in {i}")
                  time.sleep(50)
               else:
                  bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to ingested the data in {i}")
            else:
                bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to enable {i} | Error: {enable_datasource.status_code}")
         else:
             bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to delete the {i}")
      else:
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to disable the {i}")

   elif get_timestamp.status_code == 204:
      start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
      if start_supervisor.status_code == 200:
         time.sleep(50)
      else:
         bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to start batch ingestion task in {i}")
bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** Survey Batch Ingestion COMPLETED AT: {datetime.datetime.now()} ***********\n")	
