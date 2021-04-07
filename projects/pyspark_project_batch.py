# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author :
# Description :
#
# -----------------------------------------------------------------
import json, sys, time
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict
import databricks.koalas as ks
from azure.storage.blob import BlockBlobService, PublicAccess
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import datetime

config_path = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('FILE_PATHS', 'project_success_log_filename')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('FILE_PATHS', 'project_success_log_filename'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('FILE_PATHS', 'project_error_log_filename')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('FILE_PATHS', 'project_error_log_filename'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    def convert_to_row(d: dict) -> Row:
        return Row(**OrderedDict(sorted(d.items())))
except Exception as e:
    errorLogger.error(e, exc_info=True)

spark = SparkSession.builder.appName("projects").config("spark.driver.memory", "25g").getOrCreate()

clientProd = MongoClient(config.get('MONGO', 'url'))

dbProd = clientProd[config.get('MONGO', 'db')]

projectsCollec = dbProd[config.get('MONGO', 'collection')]

# getKeyclock api to  generate authentication token
try:
    def get_keyclock_accesstoken():
        url_getkeyclock = config.get("KEYCLOAK", "url")
        headers_getkeyclock = {'Content-Type': 'application/x-www-form-urlencoded'}
        body_getkeyclock = {
            "grant_type": config.get("KEYCLOAK", "grant_type"),
            "client_id": config.get("KEYCLOAK", "client_id"),
            "refresh_token": config.get("KEYCLOAK", "refresh_token")
        }

        responsegetkeyclock = requests.post(
            url_getkeyclock, data=body_getkeyclock, headers=headers_getkeyclock
        )
        if responsegetkeyclock.status_code == 200:
            successLogger.debug("getkeyclock api")
            return responsegetkeyclock.json()
        else:
            errorLogger.error(" Failure in getkeyclock API ")
            errorLogger.error(responsegetkeyclock)
            errorLogger.error(responsegetkeyclock.text)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def readUser(userId, accessToken):
        queryStringReadUser = "?fields=completeness%2CmissingFields%2ClastLoginTime%2Ctopics%2Corganisations%2Croles%2Clocations%2Cdeclarations"
        urlReadUser = config.get("ENDPOINTS", "read_user") + "/" + str(userId) + queryStringReadUser
        headersReadUser = {
            'Content-Type': config.get("API", "content_type"),
            'Authorization': config.get("API", "authorization"),
            'X-authenticated-user-token': accessToken
        }
        responseReadUser = requests.get(urlReadUser, headers=headersReadUser)
        if responseReadUser.status_code == 200:
            successLogger.debug("read user api")
            responseReadUser = responseReadUser.json()
            return responseReadUser
        else:
            errorLogger.error("read user api failed")
            errorLogger.error(responseReadUser)
            errorLogger.error(responseReadUser.text)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def removeduplicate(it):
        seen = []
        for x in it:
            if x not in seen:
                yield x
                seen.append(x)
except Exception as e:
    errorLogger.error(e, exc_info=True)

spark = SparkSession.builder.appName("projects").config(
    "spark.driver.memory", "50g"
).config("spark.executor.memory", "100g").config(
    "spark.memory.offHeap.enabled", True
).config("spark.memory.offHeap.size", "32g").getOrCreate()

sc = spark.sparkContext

projects_cursorMongo = projectsCollec.aggregate(
    [
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "projectTemplateId": {"$toString": "$projectTemplateId"},
                "solutionInformation": {"name": 1}, "title": 1,
                "programId": {"$toString": "$programId"},
                "programInformation": {"name": 1},
                "metaInformation": {"duration": 1}, "syncedAt": 1,
                "updatedAt": 1, "isDeleted": 1, "categories": 1,
                "tasks": 1, "status": 1, "userId": 1, "description": 1,
                "createdAt": 1
            }
        }
    ]
)

projects_schema = StructType(
    [
        StructField('_id', StringType(), True),
        StructField('projectTemplateId', StringType(), True),
        StructField(
            'solutionInformation', StructType(
                [StructField('name', StringType(), True)]
            )
        ),
        StructField('title', StringType(), True),
        StructField('programId', StringType(), True),
        StructField(
            'programInformation', StructType(
                [StructField('name', StringType(), True)]
            )
        ),
        StructField(
            'metaInformation', StructType(
                [StructField('duration', StringType(), True)]
            )
        ),
        StructField('updatedAt', TimestampType(), True),
        StructField('syncedAt', TimestampType(), True),
        StructField('isDeleted', BooleanType(), True),
        StructField('status', StringType(), True),
        StructField('userId', StringType(), True),
        StructField('description', StringType(), True),
        StructField('createdAt', TimestampType(), True),
        StructField(
            'categories', ArrayType(
                StructType(
                    [StructField('name', StringType(), True)]
                )
            ), True
        ),
        StructField(
            'tasks', ArrayType(
                StructType(
                    [
                        StructField('_id', StringType(), True),
                        StructField('name', StringType(), True),
                        StructField('assignee', StringType(), True),
                        StructField(
                            'attachments', ArrayType(
                                StructType([StructField('sourcePath', StringType(), True)])
                            )
                        ),
                        StructField('startDate', StringType(), True),
                        StructField('endDate', StringType(), True),
                        StructField('syncedAt', TimestampType(), True),
                        StructField('status', StringType(), True),
                        StructField('children', ArrayType(
                            StructType([
                                StructField('_id', StringType(), True),
                                StructField('name', StringType(), True),
                                StructField('startDate', StringType(), True),
                                StructField('endDate', StringType(), True),
                                StructField('syncedAt', TimestampType(), True),
                                StructField('status', StringType(), True)
                            ])
                        )),
                    ]
                )
            ), True
        )
    ]
)

projects_rdd = spark.sparkContext.parallelize(list(projects_cursorMongo))
projects_df = spark.createDataFrame(projects_rdd, projects_schema)

projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True, "project imported from library"
    ).otherwise("user created project")
)

projects_df = projects_df.withColumn(
    "project_title", F.when(
        projects_df["solutionInformation"]["name"].isNotNull() == True,
        projects_df["solutionInformation"]["name"]
    ).otherwise(projects_df["title"])
)

projects_df = projects_df.withColumn(
    "date_time", to_timestamp(projects_df["updatedAt"], 'yyyy-MM-dd HH:mm:ss')
)

projects_df = projects_df.withColumn("date", F.split(projects_df["date_time"], ' ')[0])
projects_df = projects_df.withColumn("time", F.split(projects_df["date_time"], ' ')[1])

projects_df = projects_df.withColumn(
    "project_updated_date", F.concat(
        F.col("date"), F.lit("T"), F.col("time"), F.lit(".000Z")
    )
)

projects_df = projects_df.withColumn(
    "deleted_flag", F.when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == True), "true"
    ).when(
        (projects_df["isDeleted"].isNotNull() == True) &
        (projects_df["isDeleted"] == False), "false"
    ).otherwise("false")
)

projects_df = projects_df.withColumn("exploded_categories", F.explode_outer(F.col("categories")))

projects_df = projects_df.withColumn("parent_channel", F.lit("SHIKSHALOKAM"))

projects_df = projects_df.withColumn("exploded_tasks", F.explode_outer(F.col("tasks")))

projects_df = projects_df.withColumn(
    "exploded_tasks_attachments", F.explode_outer(projects_df["exploded_tasks"]["attachments"])
)

projects_df = projects_df.withColumn(
    "task_evidence_status", F.when(
        projects_df["exploded_tasks_attachments"]["sourcePath"].isNotNull() == True, True
    ).otherwise(False)
)

projects_df = projects_df.withColumn(
    "task_evidence", F.when(
        projects_df["exploded_tasks_attachments"]["sourcePath"].isNotNull() == True,
        F.concat(
            F.lit("https://samikshaprod.blob.core.windows.net/samiksha/"),
            projects_df["exploded_tasks_attachments"]["sourcePath"]
        )
    )
)

projects_df = projects_df.withColumn("exploded_sub_tasks", F.explode_outer(projects_df["exploded_tasks"]["children"]))

projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"), projects_df["project_created_type"],
    projects_df["project_title"],
    projects_df["title"].alias("project_title_editable"),
    projects_df["programId"].alias("program_id"),
    projects_df["programInformation"]["name"].alias("program_name"),
    projects_df["metaInformation"]["duration"].alias("project_duration"),
    projects_df["syncedAt"].alias("project_last_sync"),
    projects_df["project_updated_date"], projects_df["deleted_flag"],
    projects_df["exploded_categories"]["name"].alias("area_of_improvement"),
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["description"].alias("project_goal"), projects_df["parent_channel"],
    projects_df["createdAt"].alias("project_created_date"),
    projects_df["exploded_tasks"]["_id"].alias("task_id"),
    projects_df["exploded_tasks"]["name"].alias("tasks"),
    projects_df["exploded_tasks"]["assignee"].alias("task_assigned_to"),
    projects_df["exploded_tasks"]["startDate"].alias("task_start_date"),
    projects_df["exploded_tasks"]["endDate"].alias("task_end_date"),
    projects_df["exploded_tasks"]["syncedAt"].alias("tasks_date"),
    projects_df["exploded_tasks"]["status"].alias("tasks_status"),
    projects_df["task_evidence"], projects_df["task_evidence_status"],
    projects_df["exploded_sub_tasks"]["_id"].alias("sub_task_id"),
    projects_df["exploded_sub_tasks"]["name"].alias("sub_task"),
    projects_df["exploded_sub_tasks"]["status"].alias("sub_task_status"),
    projects_df["exploded_sub_tasks"]["syncedAt"].alias("sub_task_date"),
    projects_df["exploded_sub_tasks"]["startDate"].alias("sub_task_start_date"),
    projects_df["exploded_sub_tasks"]["endDate"].alias("sub_task_end_date")
)

projects_df_cols = projects_df_cols.dropDuplicates()

projects_userid_df = projects_df_cols.select("createdBy")

userId_projects_df_after = []
userId_arr = []
uniqueuserId_arr = []
userId_projects_df_before = projects_userid_df.toJSON().map(lambda j: json.loads(j)).collect()
for uid in userId_projects_df_before:
    userId_arr.append(uid["createdBy"])
uniqueuserId_arr = list(removeduplicate(userId_arr))

get_keycloak_obj = get_keyclock_accesstoken()

user_info_arr = []
entitiesArr = []
for usr in uniqueuserId_arr:
    readUserObj = {}
    readUserObj = readUser(usr, get_keycloak_obj["access_token"])
    if readUserObj:
        readResult = False
        readResult = "result" in readUserObj
        if readResult == True:
            readResponse = False
            readResponse = "response" in readUserObj["result"]
            if readResponse == True:
                userEntitiesArr = []
                userObj = {}
                try:
                    if len(readUserObj["result"]["response"]["userLocations"]) > 0:
                        for usrLoc in readUserObj["result"]["response"]["userLocations"]:
                            userObj[usrLoc["type"] + '_name'] = usrLoc["name"]
                            userObj[usrLoc["type"] + '_id'] = usrLoc["id"]
                            userObj[usrLoc["type"] + '_externalId'] = usrLoc["code"]
                except KeyError:
                    pass
                userEntitiesArr = list(userObj.keys())
                entitiesArr.extend(userEntitiesArr)
                userObj["id"] = readUserObj["result"]["response"]["id"]
                userObj["user_id"] = readUserObj["result"]["response"]["userName"]
                userObj["user_full_name"] = readUserObj["result"]["response"]["firstName"]
                userObj["channel"] = readUserObj["result"]["response"]["rootOrgId"]
                userRoles = None
                try:
                    userRoles = readUserObj["result"]["response"]["userSubType"]
                except KeyError:
                    userRoles = ''
                try:
                    if userRoles:
                        userObj["designation"] = userRoles
                except KeyError:
                    pass
                user_info_arr.append(userObj)

user_df = ks.DataFrame(user_info_arr)
user_df = user_df.to_spark()

final_projects_df = projects_df_cols.join(
    user_df, projects_df_cols["createdBy"] == user_df["id"], "inner"
).drop(user_df["id"])

final_projects_df = final_projects_df.dropDuplicates()

final_projects_df.coalesce(1).write.format("json").mode("overwrite").save(
    config.get("FILE_PATHS", "projects_output_dir") + "/"
)

for filename in os.listdir(config.get("FILE_PATHS", "projects_output_dir") + "/"):
    if filename.endswith(".json"):
        os.rename(
            config.get("FILE_PATHS", "projects_output_dir") + "/" + filename,
            config.get("FILE_PATHS", "projects_output_dir") + "/sl_projects.json"
        )

blob_service_client = BlockBlobService(
    account_name=config.get("AZURE", "account_name"),
    sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("FILE_PATHS", "projects_output_dir")
blob_path = config.get("AZURE", "blob_path")

for files in os.listdir(local_path):
    if "sl_projects.json" in files:
        blob_service_client.create_blob_from_path(
            container_name,
            os.path.join(blob_path, files),
            local_path + "/" + files
        )

os.remove(config.get("FILE_PATHS", "projects_output_dir") + "/sl_projects.json")

dimensionsArr = []
dimensionsArr = list(set(entitiesArr))

submissionReportColumnNamesArr = [
    'user_id', 'user_full_name', 'project_title', 'project_goal', 'project_created_date',
    'project_last_sync', 'area_of_improvement', 'status_of_project', 'tasks',
    'tasks_date', 'tasks_status', 'sub_task', 'sub_task_status', 'sub_task_date',
    "task_start_date", "task_end_date", "sub_task_start_date", "sub_task_end_date",
    "designation", "deleted_flag", "task_evidence", "task_evidence_status", "project_id",
    "task_id", "sub_task_id", "project_created_type", "task_assigned_to", 'channel',
    'parent_channel', 'program_id', 'program_name', 'project_updated_date', 'createdBy',
    'project_title_editable', 'project_duration'
]

dimensionsArr.extend(submissionReportColumnNamesArr)

datasources = ["sl-project"]
payload = json.loads(config.get("SPECS", "sl_general_unnati_spec"))
payload["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = dimensionsArr
ingestion_specs = [json.dumps(payload)]

for i, j in zip(datasources, ingestion_specs):

    druid_end_point = config.get("DRUID", "url") + config.get("ENDPOINTS", "coordinator_v1_ds") + i

    druid_batch_end_point = config.get("DRUID", "url") + config.get("ENDPOINTS", "indexer_v1_task")

    headers = {'Content-Type': 'application/json'}

    get_timestamp = requests.get(druid_end_point, headers=headers)

    if get_timestamp.status_code == 200:
        successLogger.debug("Successfully fetched time stamp of the datasource " + i)
        timestamp = get_timestamp.json()
        # calculating interval from druid get api
        minTime = timestamp["segments"]["minTime"]
        maxTime = timestamp["segments"]["maxTime"]
        min1 = datetime.datetime.strptime(minTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        max1 = datetime.datetime.strptime(maxTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        new_format = "%Y-%m-%d"
        min1.strftime(new_format)
        max1.strftime(new_format)
        minmonth = "{:02d}".format(min1.month)
        maxmonth = "{:02d}".format(max1.month)
        min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
        max2 = str(max1.year) + "-" + maxmonth + "-" + str(max1.day)
        interval = min2 + "_" + max2
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

                    start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
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
        start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
        if start_supervisor.status_code == 200:
            successLogger.debug("started the batch ingestion task sucessfully for the datasource " + i)
            time.sleep(50)
        else:
            errorLogger.error(start_supervisor.text)
            errorLogger.error("failed to start batch ingestion task" + str(start_supervisor.status_code))
