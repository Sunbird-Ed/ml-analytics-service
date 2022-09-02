# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author :Shakthiehswari, Ashwini
# Description : Extracts the Status of the Project submissions 
#  either Started / In-Progress / Submitted along with the users 
#  entity information
# -----------------------------------------------------------------

import json, sys, time
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import databricks.koalas as ks
import datetime
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import element_at, split, col
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import boto3
import glob

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'project_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS','project_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'project_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_error'),
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

spark = SparkSession.builder.appName("nvsk").config(
    "spark.driver.memory", "50g"
).config(
    "spark.executor.memory", "100g"
).config(
    "spark.memory.offHeap.enabled", True
).config(
    "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc = spark.sparkContext

clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]


projects_cursorMongo = projectsCollec.aggregate(
      [{"$match":{"isAPrivateProgram":False,"isDeleted":False}},
{
        "$project": {
            "_id": {"$toString": "$_id"},
            "status": 1,
            "attachments":1,
            "tasks": 1,
            "userRoleInformation": 1
        }
    }]
)

projects_schema = StructType([
    StructField('_id', StringType(), True),
    StructField('status', StringType(), True),
    StructField(
        'attachments',
        ArrayType(
            StructType([StructField('sourcePath', StringType(), True)])
        ), True
    ),
    StructField(
        'tasks',
        ArrayType(
            StructType([StructField('_id', StringType(), True),
                       StructField('attachments',
                                    ArrayType(
                                        StructType([StructField('sourcePath', StringType(), True)])
        ), True)])
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
    )
])

projects_df = spark.createDataFrame(projects_cursorMongo,projects_schema)
projects_df = projects_df.withColumn(
                 "project_evidence_status",
                 F.when(
                      size(F.col("attachments"))>=1,True
                 ).otherwise(False)
              )
projects_df = projects_df.withColumn("exploded_tasks", F.explode_outer(F.col("tasks")))

projects_df = projects_df.withColumn(
                 "task_evidence_status",
                 F.when(
                      size(projects_df["exploded_tasks"]["attachments"])>=1,True
                 ).otherwise(False)
              )

projects_df = projects_df.withColumn(
                 "evidence_status",
                F.when(
                      (projects_df["project_evidence_status"]== False) & (projects_df["task_evidence_status"]==False),False
                 ).otherwise(True)
              )
projects_final_df = projects_df.select(
              projects_df["_id"].alias("project_id"),
              projects_df["status"],
              projects_df["evidence_status"],
              projects_df["userRoleInformation"]["state"].alias("state_externalId"),
              projects_df["userRoleInformation"]["block"].alias("block_externalId"),
              projects_df["userRoleInformation"]["district"].alias("district_externalId"),
              projects_df["userRoleInformation"]["cluster"].alias("cluster_externalId"),
              projects_df["userRoleInformation"]["school"].alias("school_externalId"),
           )
projects_final_df = projects_final_df.dropDuplicates()
projects_final_df = projects_final_df.filter(F.col("status") != "null")

projects_entities_id_df = projects_final_df.select("state_externalId","block_externalId","district_externalId","cluster_externalId","school_externalId")
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
projects_df_melt = melt(projects_final_df,
        id_vars=["project_id","status","evidence_status"],
        value_vars=["state_externalId", "block_externalId", "district_externalId", "cluster_externalId", "school_externalId"]
        )
projects_ent_df_melt = projects_df_melt\
                 .join(entities_df,["variable","value"],how="left")\
                 .select(projects_df_melt["*"],entities_df["name"],entities_df["_id"].alias("entity_ids"))
entities_df.unpersist()
projects_df_melt.unpersist()
projects_ent_df_melt = projects_ent_df_melt.withColumn("flag",F.regexp_replace(F.col("variable"),"_externalId","_name"))
projects_ent_df_melt = projects_ent_df_melt.groupBy(["project_id"])\
                               .pivot("flag").agg(first(F.col("name")))
projects_df_final = projects_final_df.join(projects_ent_df_melt,["project_id"],how="left")

district_final_df = projects_df_final.groupBy("state_name","district_name").agg(countDistinct(F.col("project_id")).alias("Total_Micro_Improvement_Projects"),countDistinct(when(F.col("status") == "started",True),F.col("project_id")).alias("Total_Micro_Improvement_Started"),countDistinct(when(F.col("status") == "inProgress",True),F.col("project_id")).alias("Total_Micro_Improvement_InProgress"),countDistinct(when(F.col("status") == "submitted",True),F.col("project_id")).alias("Total_Micro_Improvement_Submitted"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status") == "submitted"),True),F.col("project_id")).alias("Total_Micro_Improvement_Submitted_With_Evidence")).sort("state_name","district_name")


# DF To file
OUTPUT_PATH = "/opt/sparkjobs/ml-analytics-service/urgent_data_metrics/output/"
district_final_df.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save(OUTPUT_PATH)
district_final_df.unpersist()


# Renaming a file
path = OUTPUT_PATH
extension = 'csv'
os.chdir(path)
result = glob.glob(f'*.{extension}')
os.rename(f'{path}' + f'{result[0]}', f'{path}' + 'data.csv')


# Uploading file to AWS-s3
s3 = boto3.client('s3')

s3 = boto3.resource(
    service_name = config.get('CLOUD_STORAGE', 'service_name'),
    aws_access_key_id = config.get('CLOUD_STORAGE', 'access_key'),
    aws_secret_access_key = config.get('CLOUD_STORAGE', 'secret_access_key'),
    region_name = config.get('CLOUD_STORAGE', 'region_name'),
)

bucket_name = config.get('CLOUD_STORAGE', 'bucket_name')

s3.Bucket(f'{bucket_name}').upload_file(Filename=f'{OUTPUT_PATH}' + 'data.csv', Key='Manage_Learn_Data/micro_improvement/data.csv')

print("file got uploaded to AWS")
print("DONE")
