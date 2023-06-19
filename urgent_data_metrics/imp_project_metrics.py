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
import datetime
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import element_at, split, col
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import glob

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
sys.path.append(config.get("COMMON", "cloud_module_path"))

from cloud import MultiCloud
cloud_init = MultiCloud()


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
for file_name in os.listdir(file_path_for_output_and_debug_log):
    file_path = os.path.join(file_path_for_output_and_debug_log, file_name)
    if os.path.isfile(file_path):
        file_date = file_name.split('.')[0]
        date = file_date.split('-')[0] + '-' + file_date.split('-')[1] + '-' + file_date.split('-')[2]
        if date < number_of_days_logs_kept:
            os.remove(file_path)


formatter = logging.Formatter('%(asctime)s - %(levelname)s')
# handler for output log
output_logHandler = RotatingFileHandler(f"{file_name_for_output_log}")
output_logBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}", when="w0",backupCount=1)
output_logHandler.setFormatter(formatter)

#handler for debug log
debug_logHandler = RotatingFileHandler(f"{file_name_for_debug_log}")
debug_logBackuphandler = TimedRotatingFileHandler(f"{file_name_for_debug_log}",when="w0",backupCount=1)
debug_logHandler.setFormatter(formatter)

# Add the successLoger
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successLogger.addHandler(output_logHandler)
successLogger.addHandler(output_logBackuphandler)

#add the Errorloger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorLogger.addHandler(output_logHandler)
successLogger.addHandler(output_logBackuphandler)

#add the Infologer
infoLogger = logging.getLogger('info log')
infoLogger.setLevel(logging.INFO)
infoLogger.addHandler(debug_logHandler)
infoLogger.addHandler(debug_logHandler)

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

projects_cursorMongo = projectsCollec.aggregate(
      [{"$match":{"isAPrivateProgram":False,"isDeleted":False,"programInformation.name":{"$regex": "^((?!(?i)(test)).)*$"}}},
{
        "$project": {
            "_id": {"$toString": "$_id"},
            "status": 1,
            "attachments":1,
            "tasks": {"attachments":1,"_id": {"$toString": "$_id"}},
            "userProfile": 1
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
          'userProfile',
          StructType([
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

projects_df = projects_df.withColumn(
   "exploded_userLocations",F.explode_outer(projects_df["userProfile"]["userLocations"])
)
entities_df = melt(projects_df,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId")))
entities_df = entities_df.withColumn("variable1",F.concat(F.col("type"),F.lit("_name")))
entities_df = entities_df.withColumn("variable2",F.concat(F.col("type"),F.lit("_code")))

entities_df_id=entities_df.groupBy("_id").pivot("variable").agg(first("id"))

entities_df_name=entities_df.groupBy("_id").pivot("variable1").agg(first("name"))

entities_df_value=entities_df.groupBy("_id").pivot("variable2").agg(first("value"))

entities_df_med=entities_df_id.join(entities_df_name,["_id"],how='outer')
entities_df_res=entities_df_med.join(entities_df_value,["_id"],how='outer')
entities_df_res=entities_df_res.drop('null')

projects_df = projects_df.join(entities_df_res,projects_df["_id"]==entities_df_res["_id"],how='left')\
        .drop(entities_df_res["_id"])
projects_df = projects_df.filter(F.col("status") != "null")
entities_df.unpersist()
projects_df_final = projects_df.select(
              projects_df["_id"].alias("project_id"),
              projects_df["status"],
              projects_df["evidence_status"],
              projects_df["school_name"],
              projects_df["school_externalId"],
              projects_df["school_code"],
              projects_df["block_name"],
              projects_df["block_externalId"],
              projects_df["block_code"],
              projects_df["state_name"],
              projects_df["state_externalId"],
              projects_df["state_code"],
              projects_df["district_name"],
              projects_df["district_externalId"],
              projects_df["district_code"]
           )
projects_df_final = projects_df_final.dropDuplicates()

district_final_df = projects_df_final.groupBy("state_name","district_name").agg(countDistinct(F.col("project_id")).alias("Total_Micro_Improvement_Projects"),countDistinct(when(F.col("status") == "started",True),F.col("project_id")).alias("Total_Micro_Improvement_Started"),countDistinct(when(F.col("status") == "inProgress",True),F.col("project_id")).alias("Total_Micro_Improvement_InProgress"),countDistinct(when(F.col("status") == "submitted",True),F.col("project_id")).alias("Total_Micro_Improvement_Submitted"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status") == "submitted"),True),F.col("project_id")).alias("Total_Micro_Improvement_Submitted_With_Evidence")).sort("state_name","district_name")

state_final_df = projects_df_final.groupBy("state_name").agg(countDistinct(F.col("project_id")).alias("Total_Micro_Improvement_Projects")).sort("state_name")


# DF To file
local_path = config.get("COMMON", "nvsk_imp_projects_data_local_path")
blob_path = config.get("COMMON", "nvsk_imp_projects_data_blob_path")
district_final_df.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save(local_path)
district_final_df.unpersist()


# Renaming a file
path = local_path
extension = 'csv'
os.chdir(path)
result = glob.glob(f'*.{extension}')
os.rename(f'{path}' + f'{result[0]}', f'{path}' + 'data.csv')


# Uploading file to Cloud
cloud_init.upload_to_cloud(blob_Path = blob_path, local_Path = local_path, file_Name = 'data.csv')

# DF To file - State
state_local_path = config.get("COMMON", "nvsk_imp_projects_state_data_local_path")
state_blob_path = config.get("COMMON", "nvsk_imp_projects_state_data_blob_path")
state_final_df.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save(state_local_path)
state_final_df.unpersist()


# Renaming a file - State
state_path = state_local_path
extension = 'csv'
os.chdir(state_path)
state_result = glob.glob(f'*.{extension}')
os.rename(f'{state_path}' + f'{state_result[0]}', f'{state_path}' + 'state_data.csv')


# Uploading file to Cloud - State
cloud_init.upload_to_cloud(blob_Path = state_blob_path, local_Path = state_local_path, file_Name = 'state_data.csv')

print("file got uploaded to AWS")
print("DONE")
