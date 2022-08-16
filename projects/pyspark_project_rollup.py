# --------------------------------------
# Rollup Datasource old data refreshment 
# --------------------------------------


import os
import logging
import requests
import datetime
import json, sys, time

import pyspark.sql.functions as F

from udf_func import *
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
successHandler = RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Add the Error message handler to the logger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS', 'project_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'project_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

# Querying the Mongo DB
clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]

try:
    def removeduplicate(it):
        '''Remove duplicates from a dataframe'''
        seen = []
        for x in it:
            if x not in seen:
                yield x
                seen.append(x)
except Exception as e:
    errorLogger.error(e, exc_info=True)


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

# Schema for organisation
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
   
# Initiating Spark session
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

# Querying the Mongo DB collection
projects_cursorMongo = projectsCollec.aggregate(
    [{"$match":{"isDeleted":False}},
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
            "tasks": 1,
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

# Writing the schema for Pyspark
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
              StructField('state', StringType(), True),
              StructField('block', StringType(), True),
              StructField('district', StringType(), True),
              StructField('cluster', StringType(), True),
              StructField('school', StringType(), True),
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
             )
          ])
    ),
    StructField(
        'taskarr',
         ArrayType(
             StructType([
                  StructField('tasks', StringType(), True),
                  StructField('_id', StringType(), True),
                  StructField('remarks',StringType(), True),
                  StructField('assignee',StringType(), True),
                  StructField('startDate',StringType(), True),
                  StructField('endDate',StringType(), True),
                  StructField('syncedAt',TimestampType(), True),
                  StructField('status',StringType(), True),
                  StructField('task_evidence_status',StringType(), True),
                  StructField('deleted_flag',StringType(), True),
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
            StructField('issuedOn', StringType(), True)	
        ])	
    ),
    StructField(
        'attachments',
        ArrayType(
            StructType([StructField('sourcePath', StringType(), True)])
        ), True
    )
])


func_return = recreate_task_data(projects_cursorMongo)
prj_rdd = spark.sparkContext.parallelize(list(func_return))
projects_df = spark.createDataFrame(prj_rdd,projects_schema)
prj_rdd.unpersist()

#------------------- Writing logics for columns that requires logic -------------#

projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True ,
        "project imported from library"
    ).otherwise("user created project")
)

projects_df = projects_df.withColumn("status_code", 
    F.when(
        projects_df["status"] == "started", 1).when(
        projects_df["status"] == "inProgress", 3).when(
        projects_df["status"] == "submitted", 5).otherwise(0)
)

projects_df = projects_df.withColumn(
    "project_title",
    F.when(
        projects_df["solutionInformation"]["name"].isNotNull() == True,
        regexp_replace(projects_df["solutionInformation"]["name"], "\n", " ")
    ).otherwise(regexp_replace(projects_df["title"], "\n", " "))
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
                      size(F.col("attachments"))>=1,True
                 ).otherwise(False)
)


projects_df = projects_df.withColumn(
    "exploded_categories", F.explode_outer(F.col("categories"))
)

category_df = projects_df.groupby('_id').agg(collect_list('exploded_categories.name').alias("category_name"))
category_df = category_df.withColumn("categories_name", concat_ws(", ", "category_name"))

projects_df = projects_df.join(category_df, "_id", how = "left")
category_df.unpersist()
projects_df = projects_df.withColumn("parent_channel", F.lit("SHIKSHALOKAM"))

projects_df = projects_df.withColumn(
    "exploded_taskarr", F.explode_outer(projects_df["taskarr"])
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
    "project_evidence",F.when(
    (projects_df["exploded_taskarr"]["prj_evidence"].isNotNull() == True) &
    (projects_df["exploded_taskarr"]["prjEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["prj_evidence"]
        )
    ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
)

projects_df = projects_df.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
projects_df = projects_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))

projects_df = projects_df.withColumn("area_of_improvement",regexp_replace(F.col("categories_name"), "\n", " "))
projects_df = projects_df.withColumn("tasks_name",regexp_replace(F.col("exploded_taskarr.tasks"), "\n", " "))
projects_df = projects_df.withColumn("program_name",regexp_replace(F.col("programInformation.name"), "\n", " "))

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


#------------------- END -- Writing logics for columns that requires logic -------------#


# Selecting the important ones to create a dataframe
projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["project_created_type"],
    projects_df["project_title"],
    projects_df["status_code"],
    projects_df["programId"].alias("program_id"),
    projects_df["programExternalId"].alias("program_externalId"),
    projects_df["program_name"],
    projects_df["updatedAt"].alias("project_updated_date"),
    projects_df["area_of_improvement"],
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["parent_channel"],
    projects_df["tasks_name"],
    projects_df["exploded_taskarr"]["_id"].alias("task_id"),
    projects_df["exploded_taskarr"]["status"].alias("tasks_status"),
    projects_df["exploded_taskarr"]["task_evidence_status"].alias("task_evidence_status"),
    projects_df["private_program"],
    projects_df["project_terms_and_condition"],
    projects_df["solutionInformation"]["_id"].alias("solution_id"),
    projects_df["userRoleInformation"]["role"].alias("designation"),
    projects_df["userRoleInformation"]["state"].alias("state_externalId"),
    projects_df["userRoleInformation"]["block"].alias("block_externalId"),
    projects_df["userRoleInformation"]["district"].alias("district_externalId"),
    projects_df["userRoleInformation"]["cluster"].alias("cluster_externalId"),
    projects_df["userRoleInformation"]["school"].alias("school_externalId"),
    projects_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
    projects_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
    concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
    projects_df["evidence_status"]    
)

projects_df.unpersist()
projects_df_cols = projects_df_cols.dropDuplicates()
projects_userid_df = projects_df_cols.select("createdBy")

projects_entities_id_df = projects_df_cols.select("state_externalId","block_externalId","district_externalId","cluster_externalId","school_externalId")
entitiesId_projects_df_before = []
entitiesId_arr = []
uniqueEntitiesId_arr = []
entitiesId_projects_df_before = projects_entities_id_df.toJSON().map(lambda j: json.loads(j)).collect()
projects_entities_id_df.unpersist()
for eid in entitiesId_projects_df_before:
   try:
    entitiesId_arr.append(eid["state_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["block_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["district_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["cluster_externalId"])
   except KeyError :
    pass
   try:
    entitiesId_arr.append(eid["school_externalId"])
   except KeyError :
    pass

uniqueEntitiesId_arr = list(removeduplicate(entitiesId_arr))

# Querying Mongo for entities Collection
ent_cursorMongo = entitiesCollec.aggregate(
   [{"$match": {"$or":[{"registryDetails.locationId":{"$in":uniqueEntitiesId_arr}},{"registryDetails.code":{"$in":uniqueEntitiesId_arr}}]}},
    {
      "$project": {
         "_id": {"$toString": "$_id"},
         "entityType": 1,
         "metaInformation": {"name": 1},
         "registryDetails": 1
      }
    }
   ])

# Writing the seperate schema for entities   
ent_schema = StructType(
        [
            StructField("_id", StringType(), True),
            StructField("entityType", StringType(), True),
            StructField("metaInformation",
                StructType([StructField('name', StringType(), True)])
            ),
            StructField("registryDetails",
                StructType([StructField('locationId', StringType(), True),
                            StructField('code',StringType(), True)
                        ])
            )
        ]
    )
entities_rdd = spark.sparkContext.parallelize(list(ent_cursorMongo))
entities_df = spark.createDataFrame(entities_rdd,ent_schema)
entities_rdd.unpersist()
entities_df = melt(entities_df,
        id_vars=["_id","entityType","metaInformation.name"],
        value_vars=["registryDetails.locationId", "registryDetails.code"]
    ).select("_id","entityType","name","value"
            ).dropDuplicates()
entities_df = entities_df.withColumn("variable",F.concat(F.col("entityType"),F.lit("_externalId")))

# Necessary rows for project cluster
projects_df_melt = melt(projects_df_cols,
        id_vars=["project_id", "project_created_type", "project_title", "program_id", "program_externalId", "program_name", "project_updated_date", "tasks_name",
        "area_of_improvement", "status_of_project", "createdBy", "parent_channel", "task_id", "tasks_status", "task_evidence_status", "private_program", 
        "project_terms_and_condition", "solution_id", "designation","board_name","organisation_name","organisation_id","evidence_status", "status_code"],
        value_vars=["state_externalId", "block_externalId", "district_externalId", "cluster_externalId", "school_externalId"]
)
projects_ent_df_melt = projects_df_melt.join(entities_df,["variable","value"],how="left").select(projects_df_melt["*"],entities_df["name"],entities_df["_id"].alias("entity_ids"))
entities_df.unpersist()
projects_df_melt.unpersist()
projects_ent_df_melt = projects_ent_df_melt.withColumn("flag",F.regexp_replace(F.col("variable"),"_externalId","_name"))
projects_ent_df_melt = projects_ent_df_melt.groupBy(["project_id"]).pivot("flag").agg(first(F.col("name")))
projects_df_final = projects_df_cols.join(projects_ent_df_melt,["project_id"],how="left")
projects_ent_df_melt.unpersist()
projects_df_cols.unpersist()
final_projects_df = projects_df_final.dropDuplicates()
projects_df_final.unpersist()
final_projects_df.coalesce(1).write.format("json").mode("overwrite").save(config.get("OUTPUT_DIR", "project_rollup") + "/")

# Creating a file for project
for filename in os.listdir(config.get("OUTPUT_DIR", "project_rollup")+"/"):
    if filename.endswith(".json"):
       os.rename(
           config.get("OUTPUT_DIR", "project_rollup") + "/" + filename,
           config.get("OUTPUT_DIR", "project_rollup") + "/projects_rollup.json"
        )

# Storing the data in Azure
blob_service_client = BlockBlobService(
    account_name=config.get("AZURE", "account_name"), 
    sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "project_rollup")
blob_path = config.get("AZURE", "projects_rollup_blob_path")


for files in os.listdir(local_path):
    if "projects_rollup.json" in files:
        blob_service_client.create_blob_from_path(
            container_name,
            os.path.join(blob_path,files),
            local_path + "/" + files
        )

os.remove(config.get("OUTPUT_DIR", "project_rollup") + "/sl_projects_rollup.json")


# Preparing for the druid ingestion
druid_batch_end_point = config.get("DRUID", "batch_rollup_url")
headers = {'Content-Type': 'application/json'}


payload = {}
payload = json.loads(config.get("DRUID","project_rollup_injestion_spec"))
datasources = [payload["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(payload)]

for i, j in zip(datasources,ingestion_specs):
    druid_end_point = config.get("DRUID", "metadata_rollup_url") + i
    get_timestamp = requests.get(druid_end_point, headers=headers)
    if get_timestamp.status_code == 200:
        successLogger.debug("Successfully fetched time stamp of the datasource " + i )
        timestamp = get_timestamp.json()
        #calculating interval from druid get api
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
        max2 = str(max1.year) + "-" + maxmonth  + "-" + str(max1.day)
        interval = min2 + "_" + max2
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

                    start_supervisor = requests.post(
                        druid_batch_end_point, data=j, headers=headers
                    )
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
        start_supervisor = requests.post(
            druid_batch_end_point, data=j, headers=headers
        )
        if start_supervisor.status_code == 200:
            successLogger.debug(
                "started the batch ingestion task sucessfully for the datasource " + i
            )
            time.sleep(50)
        else:
            errorLogger.error(start_supervisor.text)
            errorLogger.error(
                "failed to start batch ingestion task" + str(start_supervisor.status_code)
            )
