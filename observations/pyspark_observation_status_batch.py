# -----------------------------------------------------------------
# Name : pyspark_observation_status_batch.py
# Author : Shakthieshwari.A
# Description : Extracts the Status of the observation submissions 
#               either notStarted / In-Progress / Completed along with the users entity information

# -----------------------------------------------------------------
import requests
import json,csv,sys,os,time
import datetime
from datetime import date
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict,Counter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement,ConsistencyLevel
import databricks.koalas as ks
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import ContentSettings
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler


config_path = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(config.get('LOGS','observation_status_success_log_filename'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_status_success_log_filename'),
                                                when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(config.get('LOGS','observation_status_error_log_filename'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_status_error_log_filename'),
                                              when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
 def get_keyclock_accesstoken():
    url_getkeyclock = config.get("URL","url_getkeyclock")
    headers_getkeyclock = {'Content-Type': 'application/x-www-form-urlencoded'}
    body_getkeyclock = {"grant_type":config.get("API_HEADERS","grant_type"),
                        "client_id":config.get("API_HEADERS","client_id"),
                        "refresh_token":config.get("API_HEADERS","refresh_token")}

    responsegetkeyclock = requests.post(url_getkeyclock, data=body_getkeyclock,headers=headers_getkeyclock)
    if responsegetkeyclock.status_code == 200:
        successLogger.debug("getkeyclock api")
        return responsegetkeyclock.json()
    else:
        errorLogger.error("Failure in getkeyclock API")
        errorLogger.error(responsegetkeyclock)
        errorLogger.error(responsegetkeyclock.text)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def searchUser(accessToken,userId):
    queryStringReadUser = "?fields=completeness%2CmissingFields%2ClastLoginTime%2Ctopics%2Corganisations%2Croles%2Clocations%2Cdeclarations"
    urlReadUser = config.get("URL","sunbird_api_base_url_ip") + "/" + config.get("URL","sunbird_api_url_readuser") \
                  + "/" + str(userId) + queryStringReadUser
    headersReadUser ={
          'Content-Type': config.get("API_HEADERS","content_type"),
          'Authorization': "Bearer "+ config.get("API_HEADERS","authorization"),
          'X-authenticated-user-token': accessToken
    }

    try:
     responseReadUser = requests.get(urlReadUser, headers=headersReadUser)
     if responseReadUser.status_code == 200 :
        return responseReadUser.json()
     else:
        successLogger.debug("Failure in Search User API")
        successLogger.debug(responseReadUser.status_code)
        successLogger.debug(responseReadUser.json())
    except Exception as e :
      errorLogger.error("Search User API Failed")
      errorLogger.error(e)
      errorLogger.error(e,exc_info=True)
except Exception as e:
  errorLogger.error(e,exc_info=True)

get_keycloak_obj = get_keyclock_accesstoken()

try:
 def removeduplicate(it):
     seen = []
     for x in it:
        if x not in seen:
             yield x
             seen.append(x)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
  errorLogger.error(e,exc_info=True)


clientProd = MongoClient(config.get('MONGO','mongo_url'))
dbProd = clientProd[config.get('MONGO','database_name')]

obsSubmissionsCollec = dbProd[config.get('MONGO','observation_submissions_collec')]

solutionCollec = dbProd[config.get('MONGO','solution_collec')]

userRolesCollec = dbProd[config.get("MONGO","user_roles_collection")]

programCollec = dbProd[config.get("MONGO","program_collec")]

#observation submission dataframe
obs_sub_cursorMongo = obsSubmissionsCollec.aggregate([{"$project": {"_id": {"$toString": "$_id"},
                                                                    "entityId":{"$toString": "$entityId"},"status":1,
                                                                    "entityExternalId":1,"entityInformation":{"name":1},
                                                                    "entityType":1,"createdBy":1,
                                                                    "solutionId":{"$toString": "$solutionId"},
                                                                    "solutionExternalId":1,"updatedAt":1,
                                                                    "programId":{"$toString": "$programId"},
                                                                    "programExternalId":1,
                                                                    "appInformation":{"appName":1}
                                                                    }
                                                       }
                                                      ]
                                                     )

#schema for the observation submission dataframe
obs_sub_schema = StructType([
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
                              StructField('entityInformtion',StructType([
                                                            StructField('name', StringType(), True)
                                                                      ])),
                              StructField('appInformation',StructType([
                                                           StructField('appName', StringType(), True)
                                                                     ]))
                              ])
spark = SparkSession.builder.appName("obs_sub_status").config("spark.driver.memory", "50g")\
    .config("spark.executor.memory","100g")\
    .config("spark.memory.offHeap.enabled",True)\
    .config("spark.memory.offHeap.size","32g").getOrCreate()

sc=spark.sparkContext

obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo));
obs_sub_df1 = spark.createDataFrame(obs_sub_rdd,obs_sub_schema);

obs_sub_df1 = obs_sub_df1.withColumn("date_time", to_timestamp(obs_sub_df1["updatedAt"], 'yyyy-MM-dd HH:mm:ss'))

obs_sub_df1 = obs_sub_df1.withColumn("date",F.split(obs_sub_df1["date_time"], ' ')[0])
obs_sub_df1 = obs_sub_df1.withColumn("time",F.split(obs_sub_df1["date_time"], ' ')[1])

obs_sub_df1 = obs_sub_df1.withColumn("app_name",\
                                               F.when(obs_sub_df1["appInformation"]["appName"].isNull(),
                                                      F.lit(config.get("COMMON","diksha_survey_app_name")))
                                                .otherwise(lower(obs_sub_df1["appInformation"]["appName"])))

obs_sub_df1 =  obs_sub_df1.withColumn("timestamp",F.concat(F.col("date"),F.lit("T"),F.col("time"),F.lit(".000Z")))
obs_sub_df = obs_sub_df1.select("status",obs_sub_df1["entityExternalId"].alias("entity_externalId"),
                                obs_sub_df1["entityId"].alias("entity_id"),
                                obs_sub_df1["entityType"].alias("entity_type"),
                                obs_sub_df1["createdBy"].alias("user_id"),
                                obs_sub_df1["solutionId"].alias("solution_id"),
                                obs_sub_df1["solutionExternalId"].alias("solution_externalId"),
                                obs_sub_df1["_id"].alias("submission_id"),
                                obs_sub_df1["entityInformation"]["name"].alias("entity_name"),
                                "timestamp",obs_sub_df1["programId"].alias("program_id"),
                                obs_sub_df1["programExternalId"].alias("program_externalId"),
                                obs_sub_df1["app_name"])
obs_sub_cursorMongo.close()

#observation solution dataframe
obs_sol_cursorMongo = solutionCollec.aggregate([{"$match":{"type":"observation"}},
                                                {"$project": {"_id": {"$toString": "$_id"},"name":1}}])

#schema for the observation solution dataframe
obs_sol_schema = StructType([
    StructField('name', StringType(), True),
    StructField('_id', StringType(), True)
])

obs_soln_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo));
obs_soln_df = spark.createDataFrame(obs_soln_rdd,obs_sol_schema);
obs_sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_soln_df = obs_sub_df.join(obs_soln_df,obs_sub_df.solution_id==obs_soln_df._id,'inner').drop(obs_soln_df["_id"])
obs_sub_soln_df = obs_sub_soln_df.withColumnRenamed("name","solution_name")

#observation program dataframe
obs_pgm_cursorMongo = programCollec.aggregate([{"$project": {"_id": {"$toString": "$_id"},"name":1}}])

#schema for the observation program dataframe
obs_pgm_schema = StructType([
    StructField('name', StringType(), True),
    StructField('_id', StringType(), True)
])

obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo));
obs_pgm_df = spark.createDataFrame(obs_pgm_rdd,obs_pgm_schema);
obs_pgm_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_pgm_df = obs_sub_soln_df.join(obs_pgm_df,obs_sub_soln_df.program_id==obs_pgm_df._id,'inner')\
    .drop(obs_pgm_df["_id"])
obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name","program_name")

#user organisation dataframe
obs_sub_soln_userid_df = obs_sub_pgm_df.select("user_id")

userId_obs_status_df_before = []
userId_obs_status_df_after = []
userId_arr = []
uniqueuserId_arr = []
userId_obs_status_df_before = obs_sub_soln_userid_df.toJSON().map(lambda j: json.loads(j)).collect()
for uid in userId_obs_status_df_before :
    userId_arr.append(uid["user_id"])

uniqueuserId_arr = list(removeduplicate(userId_arr))
userIntegratedAppEntitiesArr = []
for ch in uniqueuserId_arr :
   searchUserObj = {}
   searchUserObj = searchUser(get_keycloak_obj["access_token"],ch)
   if searchUserObj:
    searchResult = False
    searchResult = "result" in searchUserObj
    if searchResult == True :
       searchResponse = False
       searchResponse = "response" in searchUserObj["result"]
       if searchResponse == True :
          userRelatedEntitiesObj = {}
          userRoles = None
          try :
            userRoles = searchUserObj["result"]["response"]["userSubType"]
          except KeyError :
            userRoles = ''
          try :
            for usrLoc in searchUserObj["result"]["response"]["userLocations"]:
                userRelatedEntitiesObj[usrLoc["type"]+'_name'] = usrLoc["name"]
                userRelatedEntitiesObj[usrLoc["type"]+'_id'] = usrLoc["id"]
                userRelatedEntitiesObj[usrLoc["type"]+'_externalId'] = usrLoc["code"]
                userRelatedEntitiesObj["user_id"] = searchUserObj["result"]["response"]["id"]
                if userRoles :
                   userRelatedEntitiesObj["role_id"] = "integrated_app"
                   userRelatedEntitiesObj["role_externalId"] = "integrated_app"
                   userRelatedEntitiesObj["role_title"] = userRoles
          except KeyError :
            pass
          if userRelatedEntitiesObj :
             userIntegratedAppEntitiesArr.append(userRelatedEntitiesObj)

          for usOg in searchUserObj["result"]["response"]["organisations"]:
            searchObj = {}
            searchObj["id"] = searchUserObj["result"]["response"]["id"]
            searchObj["user_name"] = searchUserObj["result"]["response"]["userName"]
            searchObj["first_name"] = searchUserObj["result"]["response"]["firstName"]
            searchObj["channel"] = searchUserObj["result"]["response"]["rootOrgId"]
            searchObj["parent_channel"] = "SHIKSHALOKAM"
            try:
              searchObj["organisation_id"] = usOg["organisationId"]
            except KeyError :
              searchObj["organisation_id"] = None
            userId_obs_status_df_after.append(searchObj)

df_user_org = ks.DataFrame(userId_obs_status_df_after);
df_user_org = df_user_org.to_spark()

if len(userIntegratedAppEntitiesArr) > 0 :
 df_user_rel_entities = ks.DataFrame(userIntegratedAppEntitiesArr)
 df_user_rel_entities = df_user_rel_entities.to_spark()

# roles dataframe from mongodb
roles_cursorMongo = userRolesCollec.aggregate([{"$project": {"_id": {"$toString": "$_id"},"title":1}}])

#schema for the observation solution dataframe
roles_schema = StructType([
    StructField('title', StringType(), True),
    StructField('_id', StringType(), True)
])

roles_rdd = spark.sparkContext.parallelize(list(roles_cursorMongo));
roles_df = spark.createDataFrame(roles_rdd,roles_schema);

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
  errorLogger.error(e,exc_info=True)

headers_user = {'Content-Type': 'application/json'}
url_getuserinfo = config.get("ELASTICSEARCH","url_user")
payload_user_elastic = {"size": 10000,"query":{"bool":{"must":[{"match":{"_type":"_doc"}}]}}}
user_response = requests.post(url_getuserinfo , headers = headers_user,data=json.dumps(payload_user_elastic))
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
    user_scroll_api_url = config.get("ELASTICSEARCH","url_user_scroll")
    user_scroll_response = requests.post(user_scroll_api_url,headers=headers_user,data = user_scroll_payload)
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

user_roles_rdd = spark.sparkContext.parallelize(list(userEntityRoleArray));
user_roles_df = spark.createDataFrame(user_roles_rdd,user_roles_schema);


# merge user_roles_df and roles_df to get role title 
user_roles_title_df = user_roles_df.join(roles_df,user_roles_df.roleId==roles_df._id,'inner').drop(roles_df["_id"])
user_roles_title_df = user_roles_title_df.select(user_roles_title_df["roleId"].alias("role_id"),
                                                 user_roles_title_df["userId"].alias("user_id"),
                                                 user_roles_title_df["roleCode"].alias("role_externalId"),
                                                 user_roles_title_df["entityId"],
                                                 user_roles_title_df["title"].alias("role_title"))

#entity elastic search dataframe
entityArray = []

def entityElasticSearchJson(entityJsonData):
    for ent_data in entityJsonData :
        for tel in ent_data["_source"]["data"]["telemetry_entities"]:
            tel["entity_id"] = ent_data["_source"]["data"]["_id"]
            entityArray.append(tel)
headers_entity = {'Content-Type': 'application/json'}
url_getentityinfo = config.get("ELASTICSEARCH","url_entity")
payload_entity_elastic = {"size": 10000,"query":{"bool":{"must":[{"match":{"_type":"_doc"}}]}}}
entity_response = requests.post(url_getentityinfo , headers = headers_entity,data=json.dumps(payload_entity_elastic))
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
    entity_scroll_api_url = config.get("ELASTICSEARCH","url_user_scroll")
    entity_scroll_response = requests.post(entity_scroll_api_url,headers=headers_entity,data = entity_scroll_payload)
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


entity_df = ks.DataFrame(entityArray);
entity_df = entity_df.to_spark()

# merge user role title dataframe and entity dataframe 
user_entity_info_df = user_roles_title_df.join(entity_df,user_roles_title_df.entityId==entity_df.entity_id,'inner')\
    .drop(user_roles_title_df["entityId"])

# merge user entity dataframe and user org dataframe
user_df = df_user_org.join(user_entity_info_df,df_user_org.id==user_entity_info_df.user_id,'left')\
    .drop(user_entity_info_df["user_id"]).drop(user_entity_info_df["entity_id"])

user_df_integrated_app = df_user_org.join(df_user_rel_entities,df_user_org.id==df_user_rel_entities.user_id,'left')
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
obs_sub_status_df_survey = obs_sub_pgm_df\
    .join(user_df,[obs_sub_pgm_df.user_id==user_df.id,
                   obs_sub_pgm_df.app_name==config.get("COMMON","diksha_survey_app_name")],'inner')\
    .drop(user_df["id"]).drop(user_df["entity_id"])

obs_sub_status_df_integrated_app = obs_sub_pgm_df\
    .join(user_df_integrated_app,[obs_sub_pgm_df.user_id==user_df_integrated_app.id,
                                  obs_sub_pgm_df.app_name==config.get("COMMON","diksha_integrated_app_name")],'inner')\
    .drop(user_df_integrated_app["id"])

integrated_app_column_list = []
survey_app_column_list = []
integrated_app_column_list = obs_sub_status_df_integrated_app.columns
survey_app_column_list = obs_sub_status_df_survey.columns

missing_col_in_integrated_app_list = []
missing_col_in_integrated_app_list = list(set(integrated_app_column_list) - set(survey_app_column_list))
missing_col_in_survey_app_list = []
missing_col_in_survey_app_list = list(set(survey_app_column_list) - set(integrated_app_column_list))

if len(missing_col_in_survey_app_list) :
 for inte in missing_col_in_survey_app_list :
    obs_sub_status_df_integrated_app = obs_sub_status_df_integrated_app.withColumn(inte, lit(None).cast(StringType()))

if len(missing_col_in_integrated_app_list) :
 for sur in missing_col_in_integrated_app_list :
    obs_sub_status_df_survey = obs_sub_status_df_survey.withColumn(sur, lit(None).cast(StringType()))

final_df = obs_sub_status_df_integrated_app.unionByName(obs_sub_status_df_survey)
final_df = final_df.dropDuplicates()
final_df.coalesce(1).write.format("json").mode("overwrite") \
   .save(config.get("COMMON","observation_status_output_dir")+"/")

for filename in os.listdir(config.get("COMMON","observation_status_output_dir")+"/"):
        if filename.endswith(".json"):
           os.rename(config.get("COMMON","observation_status_output_dir")+"/"+filename,
                     config.get("COMMON","observation_status_output_dir")+"/sl_observation_status.json")
blob_service_client = BlockBlobService(account_name=config.get("AZURE","account_name"),
                                       sas_token=config.get("AZURE","sas_token"))
container_name = config.get("AZURE","container_name")
local_path = config.get("COMMON","observation_status_output_dir")
blob_path = config.get("AZURE","blob_path")

for files in os.listdir(local_path):
    if "sl_observation_status.json" in files:
     blob_service_client.create_blob_from_path(container_name,os.path.join(blob_path,files),local_path + "/" + files)

datasources = ["sl-observation-status"]

sl_status_spec = config.get("DRUID","sl_observation_status_spec")

ingestion_specs = [sl_status_spec]

for i,j in zip(datasources,ingestion_specs):

  druid_end_point = config.get("DRUID","druid_end_point")+i

  druid_batch_end_point = config.get("DRUID","druid_batch_end_point")

  headers = {'Content-Type' : 'application/json'}

  get_timestamp = requests.get(druid_end_point, headers=headers)

  successLogger.debug(get_timestamp)
  if get_timestamp.status_code == 200 :
     successLogger.debug("Successfully fetched time stamp of the datasource " + i )
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

        delete_segments = requests.delete(druid_end_point + "/intervals/" + interval, headers=headers)
        if delete_segments.status_code == 200:
           successLogger.debug("successfully deleted the segments " + i)
           time.sleep(300)

           enable_datasource = requests.get(druid_end_point, headers=headers)
           if enable_datasource.status_code == 204:
              successLogger.debug("successfully enabled the datasource " + i)

              time.sleep(300)

              start_supervisor = requests.post(druid_batch_end_point,data=j, headers=headers)
              successLogger.debug("ingest data")
              if start_supervisor.status_code == 200:
                 successLogger.debug("started the batch ingestion task sucessfully for the datasource " + i)
                 time.sleep(50)
              else:
                 errorLogger.error("failed to start batch ingestion task" + str(start_supervisor.status_code))
                 
           else:
              errorLogger.error("failed to enable the datasource " + i)
        else:
           errorLogger.error("failed to delete the segments of the datasource " + i)
     else:
        errorLogger.error("failed to disable the datasource " + i)



  elif get_timestamp.status_code == 204:
           start_supervisor = requests.post(druid_batch_end_point,data=j, headers=headers)
           if start_supervisor.status_code == 200:
              successLogger.debug("started the batch ingestion task sucessfully for the datasource " + i)
              time.sleep(50)
           else:
              errorLogger.error("failed to start batch ingestion task" + str(start_supervisor.status_code))
              errorLogger.error(start_supervisor.json())
  else:
     errorLogger.error("failed to get the timestamp of the datasource " + i)
