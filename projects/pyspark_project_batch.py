# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author :Shakthiehswari, Ashwini, Snehangsu
# Description : Extracts the Status of the Project submissions 
#  either Started / In-Progress / Submitted along with the users 
#  entity information
# -----------------------------------------------------------------

import json, sys, time
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import os
import requests
import pyspark.sql.utils as ut
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import datetime
from slackclient import SlackClient
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from udf_func import *
from pyspark.sql.functions import element_at, split, col

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
bot = SlackClient(config.get("SLACK","token"))
sys.path.append(config.get("COMMON", "cloud_module_path"))

from multi_cloud import MultiCloud


cloud_init = MultiCloud()
formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'), when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS', 'project_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'project_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)


clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]


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

successLogger.debug(
        "Program started  " + str(datetime.datetime.now())
   )	   
bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** STARTED AT: {datetime.datetime.now()} ***********\n")
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

successLogger.debug(
        "Mongo Query start time  " + str(datetime.datetime.now())
   )
projects_cursorMongo = projectsCollec.aggregate(
    [{"$match": {"$and":[
         {"isAPrivateProgram": False},
         {"isDeleted":False}]}},
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

successLogger.debug(
        "Mongo Query end time  " + str(datetime.datetime.now())
   )
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
        'taskarr',
         ArrayType(
             StructType([
                  StructField('tasks', StringType(), True),
                  StructField('_id', StringType(), True),
                  StructField('task_sequence', IntegerType(), True),
                  StructField('sub_task_id', StringType(), True),
                  StructField('sub_task', StringType(), True),
                  StructField('sub_task_date',TimestampType(), True),
                  StructField('sub_task_status', StringType(), True),
                  StructField('sub_task_end_date', StringType(), True),
                  StructField('sub_task_deleted_flag', BooleanType(), True),
                  StructField('task_evidence',StringType(), True),
                  StructField('remarks',StringType(), True),
                  StructField('assignee',StringType(), True),
                  StructField('startDate',StringType(), True),
                  StructField('endDate',StringType(), True),
                  StructField('syncedAt',TimestampType(), True),
                  StructField('status',StringType(), True),
                  StructField('task_evidence_status',StringType(), True),
                  StructField('deleted_flag',StringType(), True),
                  StructField('sub_task_start_date',StringType(), True),
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

successLogger.debug(
        "Function call start time  " + str(datetime.datetime.now())
   )
func_return = recreate_task_data(projects_cursorMongo)
successLogger.debug(
        "Function return end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "RDD converstion start time  " + str(datetime.datetime.now())
   ) 
prj_rdd = spark.sparkContext.parallelize(list(func_return))
successLogger.debug(
        "RDD converstion end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "RDD to Dataframe conversion start time  " + str(datetime.datetime.now())
   )
projects_df = spark.createDataFrame(prj_rdd,projects_schema)
successLogger.debug(
        "RDD to Dataframe conversion end time  " + str(datetime.datetime.now())
   )
prj_rdd.unpersist()

successLogger.debug(
        "Flattening data start time  " + str(datetime.datetime.now())
   )
projects_df = projects_df.withColumn(
    "project_created_type",
    F.when(
        projects_df["projectTemplateId"].isNotNull() == True ,
        "project imported from library"
    ).otherwise("user created project")
)

projects_df = projects_df.withColumn(
    "project_title",
    F.when(
        projects_df["solutionInformation"]["name"].isNotNull() == True,
        regexp_replace(projects_df["solutionInformation"]["name"], "\n", " ")
    ).otherwise(regexp_replace(projects_df["title"], "\n", " "))
)

projects_df = projects_df.withColumn(
    "project_deleted_flag",
    F.when(
        (projects_df["isDeleted"].isNotNull() == True) & 
        (projects_df["isDeleted"] == True),
        "true"
    ).when(
        (projects_df["isDeleted"].isNotNull() == True) & 
        (projects_df["isDeleted"] == False),
        "false"
    ).otherwise("false")
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
    "project_completed_date",
    F.when(
        projects_df["status"] == "submitted",
        projects_df["updatedAt"]
    ).otherwise(None)
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
    "task_evidence",F.when(
    (projects_df["exploded_taskarr"]["task_evidence"].isNotNull() == True) &
    (projects_df["exploded_taskarr"]["taskEvi_type"] != "link"),
        F.concat(
            F.lit(config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
            projects_df["exploded_taskarr"]["task_evidence"]
        )
    ).when(
        (projects_df["exploded_taskarr"]["task_evidence"].isNotNull() == True) & 
        (projects_df["exploded_taskarr"]["task_evidence"]!="") &
        (projects_df["exploded_taskarr"]["taskEvi_type"] == "link"),
            F.concat(
                F.lit("'"),
                regexp_replace(projects_df["exploded_taskarr"]["task_evidence"], "\n", " "),
                F.lit("'")
            )
    ).when(
        (projects_df["exploded_taskarr"]["task_evidence"].isNull() == True),
        F.lit("unknown")
    ).otherwise(projects_df["exploded_taskarr"]["task_evidence"])
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
    "sub_task_deleted_flag",
    F.when((
        projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == True),
        "true"
    ).when(
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"].isNotNull() == True) &
        (projects_df["exploded_taskarr"]["sub_task_deleted_flag"] == False),
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
    ).when(
        (projects_df["exploded_taskarr"]["prj_evidence"].isNotNull() == True) & 
        (projects_df["exploded_taskarr"]["prj_evidence"]!="") &
        (projects_df["exploded_taskarr"]["prjEvi_type"] == "link"),
            F.concat(
                F.lit("'"),
                regexp_replace(projects_df["exploded_taskarr"]["prj_evidence"], "\n", " "),
                F.lit("'")
            )
    ).when(
        (projects_df["exploded_taskarr"]["prj_evidence"].isNull() == True),
        F.lit("unknown")
    ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
)

successLogger.debug(
        "Organisation logic start time  " + str(datetime.datetime.now())
   )
projects_df = projects_df.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
projects_df = projects_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
successLogger.debug(
        "Organisation logic end time  " + str(datetime.datetime.now())
   )
   
projects_df = projects_df.withColumn("project_goal",regexp_replace(F.col("metaInformation.goal"), "\n", " "))
projects_df = projects_df.withColumn("area_of_improvement",F.when((F.col("categories_name").isNotNull()) & (F.col("categories_name")!=""),F.concat(F.lit("'"),regexp_replace(F.col("categories_name"), "\n", " "),F.lit("'"))).otherwise(F.lit("unknown")))
projects_df = projects_df.withColumn("tasks",F.when((F.col("exploded_taskarr.tasks").isNotNull()) & (F.col("exploded_taskarr.tasks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.tasks"), "\n", " "),F.lit("'"))).otherwise(F.lit("unknown")))
projects_df = projects_df.withColumn("sub_task",F.when((F.col("exploded_taskarr.sub_task").isNotNull()) & (F.col("exploded_taskarr.sub_task")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.sub_task"), "\n", " "),F.lit("'"))).otherwise(F.lit("unknown")))	
projects_df = projects_df.withColumn("program_name",regexp_replace(F.col("programInformation.name"), "\n", " "))
projects_df = projects_df.withColumn("task_remarks",F.when((F.col("exploded_taskarr.remarks").isNotNull()) & (F.col("exploded_taskarr.remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.remarks"), "\n", " "),F.lit("'"))).otherwise(F.lit("unknown")))
projects_df = projects_df.withColumn("project_remarks",F.when((F.col("exploded_taskarr.prj_remarks").isNotNull()) & (F.col("exploded_taskarr.prj_remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.prj_remarks"), "\n", " "),F.lit("'"))).otherwise(F.lit("unknown")))

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

prj_df_expl_ul = projects_df.withColumn(
   "exploded_userLocations",F.explode_outer(projects_df["userProfile"]["userLocations"])
)

projects_df = projects_df.withColumn(
    "project_title_editable", F.when((F.col("title").isNotNull()) & (F.col("title")!=""),F.concat(F.lit("'"),F.col("title"),F.lit("'"))).otherwise(F.lit("unknown"))
)

projects_df = projects_df.withColumn(
    "project_description", F.when((F.col("description").isNotNull()) & (F.col("description")!=""),F.concat(F.lit("'"),F.col("description"),F.lit("'"))).otherwise(F.lit("unknown"))
)

projects_df_cols = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["project_created_type"],
    projects_df["project_title"],
    projects_df["project_title_editable"],
    projects_df["programId"].alias("program_id"),
    projects_df["programExternalId"].alias("program_externalId"),
    projects_df["program_name"],
    projects_df["metaInformation"]["duration"].alias("project_duration"),
    projects_df["syncedAt"].alias("project_last_sync"),
    projects_df["updatedAt"].alias("project_updated_date"),
    projects_df["project_deleted_flag"],
    projects_df["area_of_improvement"],
    projects_df["status"].alias("status_of_project"),
    projects_df["userId"].alias("createdBy"),
    projects_df["project_description"],
    projects_df["project_goal"],projects_df["project_evidence"],
    projects_df["parent_channel"],
    projects_df["createdAt"].alias("project_created_date"),
    projects_df["exploded_taskarr"]["_id"].alias("task_id"),
    projects_df["tasks"],projects_df["project_remarks"],
    projects_df["exploded_taskarr"]["assignee"].alias("task_assigned_to"),
    projects_df["exploded_taskarr"]["startDate"].alias("task_start_date"),
    projects_df["exploded_taskarr"]["endDate"].alias("task_end_date"),
    projects_df["exploded_taskarr"]["syncedAt"].alias("tasks_date"),projects_df["exploded_taskarr"]["status"].alias("tasks_status"),
    projects_df["task_evidence"],
    projects_df["exploded_taskarr"]["task_evidence_status"].alias("task_evidence_status"),
    projects_df["exploded_taskarr"]["sub_task_id"].alias("sub_task_id"),
    projects_df["sub_task"],
    projects_df["exploded_taskarr"]["sub_task_status"].alias("sub_task_status"),
    projects_df["exploded_taskarr"]["sub_task_date"].alias("sub_task_date"),
    projects_df["exploded_taskarr"]["sub_task_start_date"].alias("sub_task_start_date"),
    projects_df["exploded_taskarr"]["sub_task_end_date"].alias("sub_task_end_date"),
    projects_df["private_program"],
    projects_df["task_deleted_flag"],projects_df["sub_task_deleted_flag"],
    projects_df["project_terms_and_condition"],
    projects_df["task_remarks"],projects_df["exploded_taskarr"]["task_sequence"].alias("task_sequence"),
    projects_df["project_completed_date"],
    projects_df["solutionInformation"]["_id"].alias("solution_id"),
    projects_df["userRoleInformation"]["role"].alias("designation"),
    projects_df["userProfile"]["rootOrgId"].alias("channel"),
    projects_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
    projects_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
    projects_df["certificate"]["osid"].alias("certificate_id"),	
    projects_df["certificate"]["status"].alias("certificate_status"),	
    projects_df["certificate"]["issuedOn"].alias("certificate_date"),
    concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
    concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type"),
    projects_df["evidence_status"]    
)

projects_task_cnt = projects_df_cols.groupBy("project_id").agg(countDistinct(F.col("task_id")).alias("task_count"))

projects_prj_evi= projects_df_cols.groupBy("project_id").agg(countDistinct("project_evidence").alias("project_evidence_count"))
projects_dff = projects_task_cnt.join(projects_prj_evi,["project_id"],"left")


projects_tsk_evi = projects_df_cols.groupBy("project_id").agg(countDistinct("task_evidence").alias("task_evidence_count"))

projects_df_cols = projects_df_cols.join(projects_dff,["project_id"],"left")
projects_df_cols = projects_df_cols.join(projects_tsk_evi,["project_id"],"left")

successLogger.debug(
        "Flattening data end time  " + str(datetime.datetime.now())
   )
projects_df.unpersist()
projects_prj_evi.unpersist()
projects_task_cnt.unpersist()
projects_dff.unpersist()
projects_task_cnt.unpersist()
projects_tsk_evi.unpersist()
projects_df_cols = projects_df_cols.dropDuplicates()

successLogger.debug(
        "Get Entities start time  " + str(datetime.datetime.now())
   )
entities_df = melt(prj_df_expl_ul,
        id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
        value_vars=["exploded_userLocations.code"]
    ).select("_id","name","value","type","id").dropDuplicates()
prj_df_expl_ul.unpersist()
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
successLogger.debug(
        "Get Entities end time  " + str(datetime.datetime.now())
   )
   
successLogger.debug(
        "Final Dataframe start time  " + str(datetime.datetime.now())
   )
projects_df_final = projects_df_cols.join(entities_df_res,projects_df_cols["project_id"]==entities_df_res["_id"],how='left')\
        .drop(entities_df_res["_id"])
successLogger.debug(
        "Final Dataframe end time  " + str(datetime.datetime.now())
   )
entities_df_res.unpersist()
projects_df_cols.unpersist()
final_projects_df = projects_df_final.dropDuplicates()
projects_df_final.unpersist()
successLogger.debug(
        "sl-project Json file generation start time  " + str(datetime.datetime.now())
   )
final_projects_df.coalesce(1).write.format("json").mode("overwrite").save(
    config.get("OUTPUT_DIR", "project") + "/"
)
successLogger.debug(
        "sl-project Json file generation end time  " + str(datetime.datetime.now())
   )
#projects submission distinct count
# try:
successLogger.debug(
        "Logic for submission distinct count start time  " + str(datetime.datetime.now())
   )
final_projects_tasks_distinctCnt_df = final_projects_df.groupBy("program_name","program_id","project_title","solution_id","status_of_project","state_name","state_externalId",
                                                                        "district_name","district_externalId","block_name","block_externalId","organisation_name","organisation_id","private_program","project_created_type",
                                                                        "parent_channel").agg(countDistinct(when(F.col("certificate_status") == "active",True),F.col("project_id")).alias("no_of_certificate_issued"),countDistinct(F.col("project_id")).alias("unique_projects"),countDistinct(F.col("solution_id")).alias("unique_solution"),countDistinct(F.col("createdBy")).alias("unique_users"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "submitted"),True),F.col("project_id")).alias("no_of_imp_with_evidence"))
final_projects_tasks_distinctCnt_df = final_projects_tasks_distinctCnt_df.withColumn("time_stamp", current_timestamp())
final_projects_tasks_distinctCnt_df = final_projects_tasks_distinctCnt_df.dropDuplicates()
final_projects_tasks_distinctCnt_df.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR","projects_distinctCount") + "/"
)
successLogger.debug(
        "Logic for submission distinct count end time  " + str(datetime.datetime.now())
   )
final_projects_tasks_distinctCnt_df.unpersist()
# except ut.AnalysisException:
#    pass

# projects submission distinct count by program level
# try:
successLogger.debug(
        "Logic for submission distinct count by program level start time  " + str(datetime.datetime.now())
   )
final_projects_tasks_distinctCnt_prgmlevel = final_projects_df.groupBy("program_name", "program_id","status_of_project", "state_name","state_externalId","private_program","project_created_type","parent_channel").agg(countDistinct(when(F.col("certificate_status") == "active",True),F.col("project_id")).alias("no_of_certificate_issued"), countDistinct(F.col("project_id")).alias("unique_projects"),countDistinct(F.col("createdBy")).alias("unique_users"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "submitted"),True),F.col("project_id")).alias("no_of_imp_with_evidence"))
final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.withColumn("time_stamp", current_timestamp())
final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.dropDuplicates()
final_projects_tasks_distinctCnt_prgmlevel.coalesce(1).write.format("json").mode("overwrite").save(
   config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/"
)
successLogger.debug(
        "Logic for submission distinct count by program level end time  " + str(datetime.datetime.now())
   )
final_projects_df.unpersist()
final_projects_tasks_distinctCnt_prgmlevel.unpersist()
# except ut.AnalysisException:
#    pass
successLogger.debug(
        "Renaming file start time  " + str(datetime.datetime.now())
   )
for filename in os.listdir(config.get("OUTPUT_DIR", "project")+"/"):
    if filename.endswith(".json"):
       os.rename(
           config.get("OUTPUT_DIR", "project") + "/" + filename,
           config.get("OUTPUT_DIR", "project") + "/sl_projects.json"
        )

#projects submission distinct count
for filename in os.listdir(config.get("OUTPUT_DIR", "projects_distinctCount")+"/"):
    if filename.endswith(".json"):
       os.rename(
           config.get("OUTPUT_DIR", "projects_distinctCount") + "/" + filename,
           config.get("OUTPUT_DIR", "projects_distinctCount") + "/ml_projects_distinctCount.json"
        )

#projects submission distinct count by program level
for filename in os.listdir(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")+"/"):
    if filename.endswith(".json"):
       os.rename(
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/" + filename,
           config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json"
        )
successLogger.debug("Renaming file end time  " + str(datetime.datetime.now())) 
  
successLogger.debug("Uploading to Azure start time  " + str(datetime.datetime.now()))

local_path = config.get("OUTPUT_DIR", "project")
blob_path = config.get("COMMON", "projects_blob_path")

#projects submission distinct count
local_distinctCnt_path = config.get("OUTPUT_DIR", "projects_distinctCount")
blob_distinctCnt_path = config.get("COMMON", "projects_distinctCnt_blob_path")

#projects submission distinct count program level
local_distinctCnt_prgmlevel_path = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")
blob_distinctCnt_prgmlevel_path = config.get("COMMON", "projects_distinctCnt_prgmlevel_blob_path")

for files in os.listdir(local_path):
    if "sl_projects.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_path, local_Path = local_path, file_Name = files)

#projects submission distinct count
for files in os.listdir(local_distinctCnt_path):
    if "ml_projects_distinctCount.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_distinctCnt_path, local_Path = local_distinctCnt_path, file_Name = files)

#projects submission distinct count program level
for files in os.listdir(local_distinctCnt_prgmlevel_path):
    if "ml_projects_distinctCount_prgmlevel.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_distinctCnt_prgmlevel_path, local_Path = local_distinctCnt_prgmlevel_path, file_Name = files)

successLogger.debug("Uploading to azure end time  " + str(datetime.datetime.now()))
successLogger.debug("Removing file start time  " + str(datetime.datetime.now()))  
os.remove(config.get("OUTPUT_DIR", "project") + "/sl_projects.json")

#projects submission distinct count
os.remove(config.get("OUTPUT_DIR", "projects_distinctCount") + "/ml_projects_distinctCount.json")

#projects submission distinct count program level
os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json")

successLogger.debug("Removing file end time  " + str(datetime.datetime.now()))
druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

successLogger.debug("Ingestion start time  " + str(datetime.datetime.now()))

#projects submission distinct count
ml_distinctCnt_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_projects_status_spec"))
ml_distinctCnt_projects_datasource = ml_distinctCnt_projects_spec["spec"]["dataSchema"]["dataSource"]
# #local
# ml_distinctCnt_projects_spec["spec"]["ioConfig"]["inputSource"]["filter"] = "ml_projects_distinctCount.json"
distinctCnt_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_projects_spec), headers=headers)
if distinctCnt_projects_start_supervisor.status_code == 200:
   bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Successfully Ingested for {ml_distinctCnt_projects_datasource}")
   successLogger.debug("started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_projects_datasource)
   time.sleep(50)
else:
   errorLogger.error("failed to start batch ingestion task of ml-project-status " + str(distinctCnt_projects_start_supervisor.status_code))
   errorLogger.error(distinctCnt_projects_start_supervisor.text)

#projects submission distinct count program level
ml_distinctCnt_prgmlevel_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec"))
ml_distinctCnt_prgmlevel_projects_datasource = ml_distinctCnt_prgmlevel_projects_spec["spec"]["dataSchema"]["dataSource"]
# #local
# ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["filter"] = "ml_projects_distinctCount_prgmlevel.json"
distinctCnt_prgmlevel_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_prgmlevel_projects_spec), headers=headers)
if distinctCnt_prgmlevel_projects_start_supervisor.status_code == 200:
   bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Successfully Ingested for {ml_distinctCnt_prgmlevel_projects_datasource}")
   successLogger.debug("started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_prgmlevel_projects_datasource)
   time.sleep(50)
else:
   errorLogger.error(
        "failed to start batch ingestion task of ml-project-programLevel-status " + str(distinctCnt_prgmlevel_projects_start_supervisor.status_code)
   )
   errorLogger.error(distinctCnt_prgmlevel_projects_start_supervisor.text)


dimensionsArr = []
entitiesArr = ["state_externalId", "block_externalId", "district_externalId", "cluster_externalId", "school_externalId",\
              "state_name","block_name","district_name","cluster_name","school_name","board_name","state_code", \
              "block_code", "district_code", "cluster_code", "school_code"]
dimensionsArr = list(set(entitiesArr))

submissionReportColumnNamesArr = [
    'project_title', 'project_goal', 'project_created_date', 'project_last_sync',
    'area_of_improvement', 'status_of_project', 'tasks', 'tasks_date', 'tasks_status',
    'sub_task', 'sub_task_status', 'sub_task_date', 'task_start_date', 'task_end_date',
    'sub_task_start_date', 'sub_task_end_date', 'designation', 'project_deleted_flag',
    'task_evidence', 'task_evidence_status', 'project_id', 'task_id', 'sub_task_id',
    'project_created_type', 'task_assigned_to', 'channel', 'parent_channel', 'program_id',
    'program_name', 'project_updated_date', 'createdBy', 'project_title_editable', 
    'project_duration', 'program_externalId', 'private_program', 'task_deleted_flag',
    'sub_task_deleted_flag', 'project_terms_and_condition','task_remarks',
    'organisation_name','project_description','project_completed_date','solution_id',
    'project_remarks','project_evidence','organisation_id','user_type', 'certificate_id', 
    'certificate_status','certificate_date',{"type":"long","name":"task_count"},{"type":"long","name":"task_evidence_count"},{"type":"long","name":"project_evidence_count"},{"type":"long","name":"task_sequence"}
]

dimensionsArr.extend(submissionReportColumnNamesArr)

payload = {}
payload = json.loads(config.get("DRUID","project_injestion_spec"))
# #local
# payload["spec"]["ioConfig"]["inputSource"]["filter"] = "sl_projects.json"
payload["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = dimensionsArr
datasources = [payload["spec"]["dataSchema"]["dataSource"]]
ingestion_specs = [json.dumps(payload)]

for i, j in zip(datasources,ingestion_specs):
    druid_end_point = config.get("DRUID", "metadata_url") + i
    get_timestamp = requests.get(druid_end_point, headers=headers)
    if get_timestamp.status_code == 200:
        bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Fetched Timestamp of {i} | Waiting for 50s")
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
                bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Deletion check successfull for {i}")
                time.sleep(300)

                enable_datasource = requests.get(druid_end_point, headers=headers)
                if enable_datasource.status_code == 200:
                   time.sleep(300)
                   enable_datasource_repeat = requests.get(druid_end_point, headers=headers)
                if (enable_datasource.status_code == 204) | (enable_datasource_repeat.status_code == 204):
                    successLogger.debug("successfully enabled the datasource " + i)
                    
                    time.sleep(300)
                    
                    print(j)
                    start_supervisor = requests.post(
                        druid_batch_end_point, data=j, headers=headers
                    )
                    successLogger.debug("ingest data")
                    if start_supervisor.status_code == 200:
                        bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Succesfully ingested the data in {i}")
                        successLogger.debug(
                            "started the batch ingestion task sucessfully for the datasource " + i
                        )
                        time.sleep(50)
                    else:
                        errorLogger.error("failed to start batch ingestion task" + i)
                        errorLogger.error(
                         "failed to start batch ingestion task " + str(start_supervisor.status_code)
                        )
                        errorLogger.error(start_supervisor.text)
                        bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to ingested the data in {i}")
                else:
                    errorLogger.error("failed to enable the datasource " + i)
                    errorLogger.error(
                    "failed to enable the datasource " + str(enable_datasource.status_code)
                    )
                    errorLogger.error(enable_datasource.text)
                    bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to enable {i} | Error: {enable_datasource.status_code}")
            else:
                errorLogger.error("failed to delete the segments of the datasource " + i)
                errorLogger.error(
                "failed to delete the segments of the datasource " + str(delete_segments.status_code)
                )
                errorLogger.error(delete_segments.text)
                bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to delete the {i}")
        else:
            errorLogger.error("failed to disable the datasource " + i)
            errorLogger.error(
                "failed to disable the datasource " + str(disable_datasource.status_code)
            )
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to disable the {i}")
            errorLogger.error(disable_datasource.text)

    elif get_timestamp.status_code == 204:
        start_supervisor = requests.post(
            druid_batch_end_point, data=j, headers=headers)
        if start_supervisor.status_code == 200:
            successLogger.debug("started the batch ingestion task sucessfully for the datasource " + i)
            time.sleep(50)
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Succesfully started the ingestion in {i}")
        else:
            errorLogger.error("failed to start batch ingestion task" + i)
            errorLogger.error("failed to start batch ingestion task" + str(start_supervisor.status_code))
            errorLogger.error(start_supervisor.text)
            bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to start batch ingestion task in {i}")
    else:
      errorLogger.error("failed to get the timestamp of the datasource " + i)
      errorLogger.error("failed to get the timestamp of the datasource " + str(get_timestamp.status_code))
      errorLogger.error(get_timestamp.text)
successLogger.debug(
        "Ingestion end time  " + str(datetime.datetime.now())
   )
successLogger.debug(
        "Program completed  " + str(datetime.datetime.now())
   )
bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** COMPLETED AT: {datetime.datetime.now()} ***********\n")
