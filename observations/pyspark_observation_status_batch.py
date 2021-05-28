# -----------------------------------------------------------------
# Name : pyspark_observation_status_batch.py
# Author : Shakthieshwari.A
# Description : Extracts the Status of the observation submissions 
#  either notStarted / In-Progress / Completed along with the users 
#  entity information
# -----------------------------------------------------------------

import requests
import json, csv, sys, os, time, redis
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
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import databricks.koalas as ks
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import ContentSettings
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
   config.get('LOGS','observation_status_success_log_filename')
)
successBackuphandler = TimedRotatingFileHandler(
   config.get('LOGS','observation_status_success_log_filename'),
   when="w0",
   backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
   config.get('LOGS','observation_status_error_log_filename')
)
errorBackuphandler = TimedRotatingFileHandler(
   config.get('LOGS','observation_status_error_log_filename'),
   when="w0",
   backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

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


clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collec')]
solutionCollec = db[config.get('MONGO', 'solutions_collec')]
userRolesCollec = db[config.get("MONGO", 'user_roles_collection')]
programCollec = db[config.get("MONGO", 'programs_collec')]

# redis cache connection 
redis_connection = redis.ConnectionPool(
   host=config.get("REDIS", "host"), 
   decode_responses=True, 
   port=config.get("REDIS", "port"), 
   db=config.get("REDIS", "db_name")
)
datastore = redis.StrictRedis(connection_pool=redis_connection)

#observation submission dataframe
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
         }
      }
   }]
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
      StructField('ecm_marked_na', StringType(), True)
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

obs_sub_df1 = obs_sub_df1.withColumn(
   "date_time", to_timestamp(obs_sub_df1["updatedAt"], 'yyyy-MM-dd HH:mm:ss')
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "date", F.split(obs_sub_df1["date_time"], ' ')[0]
)
obs_sub_df1 = obs_sub_df1.withColumn(
   "time", F.split(obs_sub_df1["date_time"], ' ')[1]
)

obs_sub_df1 = obs_sub_df1.withColumn(
   "app_name", 
   F.when(
      obs_sub_df1["appInformation"]["appName"].isNull(), 
      F.lit(config.get("COMMON", "diksha_survey_app_name"))
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
   ).otherwise("false")
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

obs_sub_df1 =  obs_sub_df1.withColumn(
   "completedDate", 
   F.concat(F.col("date"), F.lit("T"), F.col("time"), F.lit(".000Z"))
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
   obs_sub_df1["ecm_marked_na"]
)
obs_sub_cursorMongo.close()

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
obs_sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_soln_df = obs_sub_df.join(
   obs_soln_df,
   obs_sub_df.solution_id==obs_soln_df._id,
   'inner'
).drop(obs_soln_df["_id"])
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
obs_pgm_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_pgm_df = obs_sub_soln_df.join(
   obs_pgm_df,
   obs_sub_soln_df.program_id==obs_pgm_df._id,
   'inner'
).drop(obs_pgm_df["_id"])
obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")

#user organisation dataframe
obs_sub_soln_userid_df = obs_sub_pgm_df.select("user_id")

userId_obs_status_df_before = []
userId_obs_status_df_after = []
userId_arr = []
uniqueuserId_arr = []
userId_obs_status_df_before = obs_sub_soln_userid_df.toJSON().map(lambda j: json.loads(j)).collect()

for uid in userId_obs_status_df_before:
   userId_arr.append(uid["user_id"])
uniqueuserId_arr = list(removeduplicate(userId_arr))
userIntegratedAppEntitiesArr = []
for ch in uniqueuserId_arr :
   userObj = {}
   userObj = datastore.hgetall("user:"+ch)
   if userObj :
      stateName = None
      blockName = None
      districtName = None
      clusterName = None
      rootOrgId = None
      userSubType = None
      userSchool = None
      userSchoolUDISE = None
      userSchoolName = None
      orgName = None
      try:
         userSchool = userObj["school"]
      except KeyError :
         userSchool = ''
      try:
         userSchoolUDISE = userObj["schooludisecode"]
      except KeyError :
         userSchoolUDISE = ''
      try:
         userSchoolName = userObj["schoolname"]
      except KeyError :
         userSchoolName = ''
      try:
         userSubType = userObj["usersubtype"]
      except KeyError :
         userSubType = ''
      try:
         stateName = userObj["state"]
      except KeyError :
         stateName = ''
      try:
         blockName = userObj["block"]
      except KeyError :
         blockName = ''
      try:
         districtName = userObj["district"]
      except KeyError :
         districtName = ''
      try:
         clusterName = userObj["cluster"]
      except KeyError :
         clusterName = ''
      try:
         rootOrgId = userObj["rootorgid"]
      except KeyError :
         rootOrgId = ''
      try:
         orgName = userObj["orgname"]
      except KeyError:
         orgName = ''
      userRelatedEntitiesObj = {}
      try :
         userRelatedEntitiesObj["state_name"] = stateName
         userRelatedEntitiesObj["block_name"] = blockName
         userRelatedEntitiesObj["district_name"] = districtName
         userRelatedEntitiesObj["cluster_name"] = clusterName
         userRelatedEntitiesObj["user_id"] = ch
         userRelatedEntitiesObj["role_title"] = userSubType
         userRelatedEntitiesObj["school_id"] = userSchool
         userRelatedEntitiesObj["school_name"] = userSchoolName
         userRelatedEntitiesObj["school_externalId"] = userSchoolUDISE
         userRelatedEntitiesObj["organisation_name"] = orgName
      except KeyError :
         pass
      if userRelatedEntitiesObj :
         userIntegratedAppEntitiesArr.append(userRelatedEntitiesObj)

      searchObj = {}
      searchObj["id"] = ch
      searchObj["channel"] = rootOrgId
      searchObj["parent_channel"] = "SHIKSHALOKAM"
      userId_obs_status_df_after.append(searchObj)

df_user_org = ks.DataFrame(userId_obs_status_df_after)
df_user_org = df_user_org.to_spark()

if len(userIntegratedAppEntitiesArr) > 0 :
   df_user_rel_entities = ks.DataFrame(userIntegratedAppEntitiesArr)
   df_user_rel_entities = df_user_rel_entities.to_spark()

# roles dataframe from mongodb
roles_cursorMongo = userRolesCollec.aggregate(
   [{"$project": {"_id": {"$toString": "$_id"}, "title": 1}}]
)

#schema for the observation solution dataframe
roles_schema = StructType([
   StructField('title', StringType(), True),
   StructField('_id', StringType(), True)
])

roles_rdd = spark.sparkContext.parallelize(list(roles_cursorMongo))
roles_df = spark.createDataFrame(roles_rdd, roles_schema)

roles_cursorMongo.close()

# user roles along with entity from elastic search
userEntityRoleArray = []

try:
   def elasticSearchJson(userEntityJson) :
      for user in userEntityJson :
         try:
            if len(user["_source"]["data"]["roles"]) > 0 :
               for roleObj in user["_source"]["data"]["roles"]:
                  try:
                     if len(roleObj["entities"]) > 0:
                        for ent in roleObj["entities"]:
                           entObj = {}
                           entObj["userId"] = user["_source"]["data"]["userId"]
                           entObj["roleId"] = roleObj["roleId"]
                           entObj["roleCode"] =roleObj["code"]
                           entObj["entityId"] = ent
                           userEntityRoleArray.append(entObj)
                     else :
                        entNoObj = {}
                        entNoObj["userId"] = user["_source"]["data"]["userId"]
                        entNoObj["roleId"] = roleObj["roleId"]
                        entNoObj["roleCode"] = roleObj["code"]
                        entNoObj["entityId"] = None
                        userEntityRoleArray.append(entNoObj)
                  except KeyError :
                     entNoEntObj = {}
                     entNoEntObj["userId"] = user["_source"]["data"]["userId"]
                     entNoEntObj["roleId"] = roleObj["roleId"]
                     entNoEntObj["roleCode"] = roleObj["code"]
                     entNoEntObj["entityId"] = None
                     userEntityRoleArray.append(entNoEntObj)
                     pass
         except KeyError :
            pass 
except Exception as e:
   errorLogger.error(e, exc_info=True)

headers_user = {'Content-Type': 'application/json'}
url_getuserinfo = config.get("ELASTICSEARCH", "url_user")
payload_user_elastic = {
   "size": 10000,
   "query": {
      "bool": {
         "must":[{"match":{"_type":"_doc"}}]
      }
   }
}
user_response = requests.post(
   url_getuserinfo , headers=headers_user, data=json.dumps(payload_user_elastic)
)
try:
   if user_response.status_code == 200:
      user_response = user_response.json()
      user_data = user_response['hits']['hits']
      elasticSearchJson(user_data)
      user_scroll_id = user_response['_scroll_id']
   else:
      errorLogger.error(user_response)
      errorLogger.error(user_response.text)
      errorLogger.error("Failure in getting User Data From Elastic Search")
except KeyError as e:
   user_hit = []
   user_scroll_id = None
   errorLogger.error("user scroll id error")

while user_data:
   user_scroll_payload = json.dumps({
      'scroll': '1m',
      'scroll_id': user_scroll_id
   })
   user_scroll_api_url = config.get("ELASTICSEARCH", "url_user_scroll")
   user_scroll_response = requests.post(
      user_scroll_api_url, headers=headers_user, data = user_scroll_payload
   )
   try:
      if user_scroll_response.status_code == 200:
         user_scroll_response = user_scroll_response.json()
         user_data = user_scroll_response['hits']['hits']
         if len(user_data) > 0 :
            elasticSearchJson(user_data)
         user_scroll_id = user_scroll_response['_scroll_id']
      else:
         errorLogger.error("Failure in getting User Data From Elastic Search")
   except KeyError :
      user_entity_data = []
      user_entity_scroll_id = None

#schema for the observation solution dataframe
user_roles_schema = StructType([
   StructField('roleId', StringType(), True),
   StructField('userId', StringType(), True),
   StructField('roleCode', StringType(), True),
   StructField('entityId', StringType(), True)
])

user_roles_rdd = spark.sparkContext.parallelize(list(userEntityRoleArray))
user_roles_df = spark.createDataFrame(user_roles_rdd, user_roles_schema)


# merge user_roles_df and roles_df to get role title 
user_roles_title_df = user_roles_df.join(
   roles_df, 
   user_roles_df.roleId==roles_df._id,
   'inner'
).drop(roles_df["_id"])
user_roles_title_df = user_roles_title_df.select(
   user_roles_title_df["userId"].alias("user_id"),
   user_roles_title_df["entityId"],
   user_roles_title_df["title"].alias("role_title")
)

#entity elastic search dataframe
entityArray = []

def entityElasticSearchJson(entityJsonData):
   for ent_data in entityJsonData :
      for tel in ent_data["_source"]["data"]["telemetry_entities"]:
         tel["entity_id"] = ent_data["_source"]["data"]["_id"]
         entityArray.append(tel)


headers_entity = {'Content-Type': 'application/json'}
url_getentityinfo = config.get("ELASTICSEARCH", "url_entity")
payload_entity_elastic = {
   "size": 10000,
   "query":{
      "bool": {
         "must": [{"match": {"_type": "_doc"}}]
      }
   }
}
entity_response = requests.post(
   url_getentityinfo , headers = headers_entity, data=json.dumps(payload_entity_elastic)
)
try:
   if entity_response.status_code == 200:
      entity_response = entity_response.json()
      entity_data = entity_response['hits']['hits']
      entityElasticSearchJson(entity_data)
      entity_scroll_id = entity_response['_scroll_id']
   else:
      errorLogger.error("Failure in getting Entity Data From Elastic Search")
except KeyError as e:
   entity_hit = []
   entity_scroll_id = None
   errorLogger.error("entity scroll id error")

while entity_data:
   entity_scroll_payload = json.dumps({
      'scroll': '1m',
      'scroll_id': entity_scroll_id
   })
   entity_scroll_api_url = config.get("ELASTICSEARCH", "url_user_scroll")
   entity_scroll_response = requests.post(
      entity_scroll_api_url, headers=headers_entity, data=entity_scroll_payload
   )
   try:
      if entity_scroll_response.status_code == 200:
         entity_scroll_response = entity_scroll_response.json()
         entity_data = entity_scroll_response['hits']['hits']
         if len(entity_data) > 0 :
            entityElasticSearchJson(entity_data)
         entity_scroll_id = entity_scroll_response['_scroll_id']
      else:
         errorLogger.error("Failure in getting Entity Data From Elastic Search")
   except KeyError :
      entity_entity_data = []
      entity_entity_scroll_id = None

entity_df = ks.DataFrame(entityArray)
entity_df = entity_df.to_spark()

# merge user role title dataframe and entity dataframe 
user_entity_info_df = user_roles_title_df.join(
   entity_df,
   user_roles_title_df.entityId==entity_df.entity_id,
   'inner'
).drop(user_roles_title_df["entityId"])

# merge user entity dataframe and user org dataframe
user_df = df_user_org.join(
   user_entity_info_df,
   df_user_org.id==user_entity_info_df.user_id,
   'left'
).drop(user_entity_info_df["user_id"]).drop(user_entity_info_df["entity_id"])

user_df_integrated_app = df_user_org.join(
   df_user_rel_entities,
   df_user_org.id==df_user_rel_entities.user_id,
   'left'
)
user_df_integrated_app = user_df_integrated_app.drop(user_df_integrated_app["user_id"])
obs_sub_cursorMongo = []
obs_sol_cursorMongo = []
user_org_rows = []
org_rows = []
roles_cursorMongo = []
userEntityRoleArray = []
entityArray = []

obs_sub_df1.cache()
obs_sub_df.cache()
obs_soln_df.cache()
df_user_org.cache()
roles_df.cache()
user_roles_df.cache()
entity_df.cache()
user_entity_info_df.cache()

# merge user dataframe and observation submission dataframe
obs_sub_status_df_survey = obs_sub_pgm_df.join(
   user_df,
   [
      obs_sub_pgm_df.user_id==user_df.id, 
      obs_sub_pgm_df.app_name==config.get("COMMON", "diksha_survey_app_name")
   ],
   'inner'
).drop(user_df["id"]).drop(user_df["entity_id"])

obs_sub_status_df_integrated_app = obs_sub_pgm_df.join(
   user_df_integrated_app,
   [
      obs_sub_pgm_df.user_id==user_df_integrated_app.id,
      obs_sub_pgm_df.app_name==config.get("COMMON", "diksha_integrated_app_name")
   ],
   'inner'
).drop(user_df_integrated_app["id"])

integrated_app_column_list = []
survey_app_column_list = []
integrated_app_column_list = obs_sub_status_df_integrated_app.columns
survey_app_column_list = obs_sub_status_df_survey.columns

missing_col_in_integrated_app_list = []
missing_col_in_integrated_app_list = list(
   set(integrated_app_column_list) - set(survey_app_column_list)
)
missing_col_in_survey_app_list = []
missing_col_in_survey_app_list = list(
   set(survey_app_column_list) - set(integrated_app_column_list)
)

if len(missing_col_in_survey_app_list) :
   for inte in missing_col_in_survey_app_list :
      obs_sub_status_df_integrated_app = obs_sub_status_df_integrated_app.withColumn(
         inte, lit(None).cast(StringType())
      )

if len(missing_col_in_integrated_app_list) :
   for sur in missing_col_in_integrated_app_list :
      obs_sub_status_df_survey = obs_sub_status_df_survey.withColumn(
         sur, lit(None).cast(StringType())
      )

final_df = obs_sub_status_df_integrated_app.unionByName(obs_sub_status_df_survey)
final_df = final_df.dropDuplicates()

final_df.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR", "observation_status_output_dir")+"/"
)

for filename in os.listdir(config.get("OUTPUT_DIR", "observation_status_output_dir")+"/"):
   if filename.endswith(".json"):
      os.rename(
         config.get("OUTPUT_DIR", "observation_status_output_dir") + "/" + filename, 
         config.get("OUTPUT_DIR", "observation_status_output_dir") + "/sl_observation_status.json"
      )

blob_service_client = BlockBlobService(
   account_name=config.get("AZURE", "account_name"), 
   sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "observation_status_output_dir")
blob_path = config.get("AZURE", "blob_path")

for files in os.listdir(local_path):
   if "sl_observation_status.json" in files:
      blob_service_client.create_blob_from_path(
         container_name,
         os.path.join(blob_path,files),
         local_path + "/" + files
      )

sl_status_spec = {}
sl_status_spec = json.loads(config.get("DRUID","observation_status_spec"))
datasources = [sl_status_spec["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(sl_status_spec)]

for i,j in zip(datasources,ingestion_specs):
   druid_end_point = config.get("DRUID", "druid_end_point") + i
   druid_batch_end_point = config.get("DRUID", "druid_batch_end_point")
   headers = {'Content-Type': 'application/json'}
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
            ruid_end_point + "/intervals/" + interval, headers=headers
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
