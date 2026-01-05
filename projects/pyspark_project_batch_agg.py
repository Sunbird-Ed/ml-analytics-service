# -----------------------------------------------------------------
# Name : pyspark_project_batch_agg.py
# Author : Vivek M , Prashant G
# Description : Generates aggregated metrics for projects using MongoDB
# aggregation pipelines directly via Spark MongoDB Connector
# -----------------------------------------------------------------

import sys
import os
import json
import argparse
import logging
import datetime
import re
import time
import requests
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from typing import Iterable
from itertools import zip_longest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
config = ConfigParser(interpolation=ExtendedInterpolation())
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config.read(os.path.join(base_path, "config.ini"))
NATURE_OF_UPLOAD = "cloud" # "local" or "cloud"
CLOUD_MODULE_PATH = config.get("COMMON", "cloud_module_path")
MONGO_URL = config.get('MONGO', 'url')
MONGO_DATABASE_NAME = config.get('MONGO', 'database_name')
SUCCESS_LOG = config.get('LOGS', 'project_agg_success')
ERROR_LOG = config.get('LOGS', 'project_agg_error')
ML_DISTINCT_CNT_PROJECTS_STATUS_SPEC = config.get("DRUID","ml_distinctCnt_projects_status_spec")
ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_SPEC = config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec")
ML_DISTINCT_CNT_PROJECTS_STATUS_LOCAL_SPEC = config.get("DRUID", "ml_distinctCnt_projects_status_local_spec_agg", fallback=None)
ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_LOCAL_SPEC = config.get("DRUID", "ml_distinctCnt_prglevel_projects_status_local_spec_agg", fallback=None)
PROJECTS_DISTINCT_CNT_OUTPUT_DIR = config.get("OUTPUT_DIR", "projects_distinctCount")
PROJECTS_DISTINCT_CNT_OUTPUT_DIR_PRGM_LEVEL = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")
PROJECTS_DISTINCT_CNT_BLOB_PATH = config.get("COMMON", "projects_distinctCnt_blob_path")
PROJECTS_DISTINCT_CNT_BLOB_PATH_PRGM_LEVEL = config.get("COMMON", "projects_distinctCnt_prgmlevel_blob_path")
PROJECTS_COLLECTION = config.get('MONGO', 'projects_collection')
DRUID_BATCH_URL = config.get("DRUID", "batch_url")

class ConfigManager:
    def __init__(self):
        self.config = config
        self.success_logger = None
        self.error_logger = None
        self._setup_logging()
        self._setup_cloud_path()

    def _setup_cloud_path(self):
        sys.path.append(CLOUD_MODULE_PATH)

    def _setup_logging(self):
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Success Logger
        self.success_logger = logging.getLogger('success log')
        self.success_logger.setLevel(logging.INFO)
        if not self.success_logger.handlers:
            successHandler = TimedRotatingFileHandler(SUCCESS_LOG, when="w0", backupCount=1)
            successHandler.setFormatter(formatter)
            self.success_logger.addHandler(successHandler)
        self.success_logger.propagate = False

        # Error Logger
        self.error_logger = logging.getLogger('error log')
        self.error_logger.setLevel(logging.ERROR)
        if not self.error_logger.handlers:
            errorHandler = TimedRotatingFileHandler(ERROR_LOG, when="w0", backupCount=1)
            errorHandler.setFormatter(formatter)
            self.error_logger.addHandler(errorHandler)
        self.error_logger.propagate = False

    def get(self, section, option, fallback=None):
        return self.config.get(section, option, fallback=fallback)
    
    def get_logger(self):
        return self.success_logger, self.error_logger


config_manager = ConfigManager()
from cloud import MultiCloud 

# ---------------------------------------------------------------------------
# Spark Setup
# ---------------------------------------------------------------------------
def init_spark_session(app_name="pyspark_project_batch_agg"):
    return SparkSession.builder.appName(app_name).config(
        "spark.driver.memory", "50g"
    ).config(
        "spark.executor.memory", "100g"
    ).config(
        "spark.memory.offHeap.enabled", True
    ).config(
        "spark.memory.offHeap.size", "32g"
    ).config(
        "spark.eventLog.enabled", False
    ).getOrCreate()

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
class Utils:
    def __init__(self):
        pass

    def melt(self, df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str],
            var_name: str="variable", value_name: str="value") -> DataFrame:
        try:
            _vars_and_vals = array(*(
                struct(lit(c).alias(var_name), col(c).alias(value_name))
                for c in value_vars))

            # Add to the DataFrame and explode
            _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

            cols = id_vars + [
                    col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
            return _tmp.select(*cols)
        except Exception as e:
            config_manager.error_logger.error(e, exc_info=True)
            raise e

    def get_org_name_udf(self):
        orgSchema = ArrayType(StructType([
            StructField("orgId", StringType(), True),
            StructField("orgName", StringType(), True)
        ]))

        def orgName(val):
            return [
                {'orgId': org['organisationId'], 'orgName': org["orgName"]}
                for org in (val or []) if org and (org["isSchool"] == False)
            ]
        return udf(orgName, orgSchema)

# ---------------------------------------------------------------------------
# Partition Processing
# ---------------------------------------------------------------------------
def process_project_partition(partition_iterator):
    def task_detail(task,del_flg,cntr):
      if (type(task)==dict) :
       taskObj = {}
       taskObj["_id"] = task["_id"]
       taskObj["tasks"] = task["name"]
       taskObj["task_sequence"] = cntr
       taskObj["deleted_flag"] = del_flg

       try:
          if len(task["attachments"]) > 0:
              taskObj["task_evidence_status"] = True
          else:
              taskObj["task_evidence_status"] = False
       except:
          taskObj["task_evidence_status"] =  False

       try: 
         taskObj["assignee"] = task["assignee"]
       except KeyError:
         taskObj["assignee"] =''

       try:
         taskObj["startDate"] = task["startDate"]
       except KeyError:
         taskObj["startDate"] = ''

       try:
         taskObj["endDate"] = task["endDate"]
       except KeyError:
         taskObj["endDate"] = ''

       taskObj["syncedAt"] = task["syncedAt"]
       try:
         taskObj["status"] = task["status"]
       except:
         taskObj["status"] = ''

       return taskObj

    prjarr = []
    for prj in partition_iterator:
        # Filters to match pyspark_project_batch.py source query
        if prj.get("isAPrivateProgram") is not False:
            continue
        if prj.get("isDeleted") is not False:
            continue

        prjinfo = []
        ## creating project level remarks and evidence obj to avoid repetition
        try:
          if prj["attachments"]:
            for cnt in range(len(prj["attachments"])):
                prjObj = {}
                if cnt == 0:
                    try :
                        prjObj["prj_remarks"] = prj["remarks"]
                    except :
                        KeyError
                try:
                    prjObj["prjEvi_type"] = prj["attachments"][cnt]["type"]
                    if prjObj["prjEvi_type"] == "link":
                      prjObj["prj_evidence"] = prj["attachments"][cnt]["name"]
                    else:
                      prjObj["prj_evidence"] = prj["attachments"][cnt]["sourcePath"]
                except KeyError:
                    pass
                prjinfo.append(prjObj)
        except KeyError:
          try :
            if prj["remarks"]:
              prjObj = {}
              prjObj["prj_remarks"] = prj["remarks"]
              prjinfo.append(prjObj)
          except KeyError:
            pass
        
        taskarr = []
        cntr = 1
        for  task in prj.get("tasks", []):        
            arr_len = 0
            try :
              attachLen = len(task["attachments"])
            except:
              attachLen = 0
            try :
              sub_tskLen = len(task["children"])
            except:
              sub_tskLen = 0
            try:
              del_flg = task["isDeleted"]
            except:
              del_flg = False
            ## To get greater length b/w evidence and subtask
            if attachLen > sub_tskLen:
             arr_len = attachLen        
            elif sub_tskLen > attachLen:
             arr_len = sub_tskLen        
            elif ((sub_tskLen == attachLen) & (sub_tskLen == 0)):
              if del_flg == False:
                taskObj = task_detail(task,del_flg,cntr)
             
             ## add remarks value when arrlen is 0
                try:
                   taskObj["remarks"] = task["remarks"]
                except Exception as e:
                   pass

                taskarr.append(taskObj)
                arr_len = sub_tskLen  
            elif (sub_tskLen == attachLen):
             arr_len = sub_tskLen
           
            ## creating task level remarks and evidence obj to avoid repetition
            for index in range(arr_len):
              if del_flg == False:
               taskObj = task_detail(task,del_flg,cntr)
               try :
                 taskObj["taskEvi_type"] = task["attachments"][index]["type"]
                 if taskObj["taskEvi_type"] == "link":
                     taskObj["task_evidence"] = task["attachments"][index]["name"]
                 else:
                     taskObj["task_evidence"] = task["attachments"][index]["sourcePath"]
               except :
                   pass
               try:
                 taskObj["sub_task"] = task["children"][index]["name"]
               except :
                 pass
               if index == 0:
                 try:
                   taskObj["remarks"] = task["remarks"]
                 except :
                   pass
               
               ## Sub task data    
               try :
                 if "children":
                    try:
                      sub_del_flg = task["children"][index]["isDeleted"]
                    except:
                      sub_del_flg = False                  
                    if sub_del_flg == False:
                      taskObj["sub_task_date"] = task["children"][index]["syncedAt"]
                      taskObj["sub_task_id"] = task["children"][index]["_id"]
                      taskObj["sub_task_status"] = task["children"][index]["status"]
                      taskObj["sub_task_deleted_flag"] = sub_del_flg
                      try:
                        taskObj["sub_task_start_date"] = task["children"][index]["startDate"]
                      except KeyError:
                        taskObj["sub_task_start_date"] = ''
                      try:
                        taskObj["sub_task_end_date"] = task["children"][index]["endDate"]
                      except KeyError:
                        pass
                      taskarr.append(taskObj)
                    else:
                      taskarr.append(taskObj) 
               except IndexError:
                taskarr.append(taskObj)
            cntr = cntr + 1
        

        ## Formatting project level remarks and evidence
        prjinfo_len = len(prjinfo)
        taskarr_len = len(taskarr)
        if ((taskarr_len > prjinfo_len) & (prjinfo_len !=0)) | ((taskarr_len == prjinfo_len) & (prjinfo_len !=0)):
          for ind in range(len(prjinfo)):
            taskarr[ind].update(prjinfo[ind])
        elif (taskarr_len < prjinfo_len):
          try:
            for ind in range(len(prjinfo)):
              prjinfo[ind].update(taskarr[ind])
              del((taskarr[ind]))
              taskarr.append(prjinfo[ind])        
          except IndexError:
            while(ind < prjinfo_len):
              taskarr.append(prjinfo[ind])
              ind = ind + 1
        prj["taskarr"] = taskarr
        
        ## delete unwanted keys 
        del_keys = ["tasks"]
        for key in del_keys:
          try:
            del prj[key]         
          except KeyError:
            pass
        prjarr.append(prj)
    
    return iter(prjarr)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def get_projects_schema():
    return StructType([
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
                StructField('issuedOn', StringType(), True),
                StructField('templateUrl', StringType(),True),
                StructField('eligible',BooleanType(), True)		
            ])	
        ),
        StructField(
            'attachments',
            ArrayType(
                StructType([StructField('sourcePath', StringType(), True)])
            ), True
        )
    ])

# ---------------------------------------------------------------------------
# Ingestion Manager
# ---------------------------------------------------------------------------
class IngestionManager:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.clientProd = MongoClient(MONGO_URL)
        self.db = self.clientProd[MONGO_DATABASE_NAME]
        self.projectsCollec = self.db[PROJECTS_COLLECTION]
        self.cloud_init = MultiCloud()
        self.success_logger, self.error_logger = self.config_manager.get_logger()

    def fetch_projects(self, program_unique_id=None):
        project_query = {"$match": {"$and":[{"isAPrivateProgram": False},{"isDeleted":False}]}}
        if program_unique_id:
             project_query["$match"]["$and"].append({"programId": program_unique_id})

        self.success_logger.info("Mongo Query start time")
        cursor = self.projectsCollec.aggregate(
            [project_query,
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
        self.success_logger.info("Mongo Query end time")
        return cursor
    
    def _process_file_operation(self, output_dir, blob_path, file_suffix, program_unique_id):
        """Helper to Rename, Upload, and Remove files."""
        for filename in os.listdir(output_dir + "/"):
            if filename.endswith(".json"):
                target_name = f"{file_suffix}_{program_unique_id}.json" if program_unique_id else f"{file_suffix}.json"
                os.rename(os.path.join(output_dir, filename), os.path.join(output_dir, target_name))
        
        for files in os.listdir(output_dir):
            target_name = f"{file_suffix}_{program_unique_id}.json" if program_unique_id else f"{file_suffix}.json"
            if target_name in files:
                if NATURE_OF_UPLOAD != "local":
                    self.cloud_init.upload_to_cloud(blob_Path=blob_path, local_Path=output_dir, file_Name=files)
                    os.remove(os.path.join(output_dir, files))

    def upload_and_trigger(self, program_unique_id):
        self.success_logger.info("Renaming, Uploading, Removing files start time")
        
        # Projects Submission Distinct Count
        self._process_file_operation(PROJECTS_DISTINCT_CNT_OUTPUT_DIR, PROJECTS_DISTINCT_CNT_BLOB_PATH, "ml_projects_distinctCount", program_unique_id)

        # Projects Submission Distinct Count Program Level
        self._process_file_operation(PROJECTS_DISTINCT_CNT_OUTPUT_DIR_PRGM_LEVEL, PROJECTS_DISTINCT_CNT_BLOB_PATH_PRGM_LEVEL, "ml_projects_distinctCount_prgmlevel", program_unique_id)

        self.success_logger.info("Renaming, Uploading, Removing files end time")
        self._trigger_druid(program_unique_id)

    def _submit_druid_task(self, cloud_spec_str, local_spec_str, program_unique_id, output_dir=None, file_prefix=None):
        """Helper to submit a Druid task."""
        
        if NATURE_OF_UPLOAD == "local":
            druid_spec = local_spec_str
            spec = json.loads(druid_spec)
            if program_unique_id and output_dir and file_prefix:
                base_dir_path = output_dir
                file_name_final = f"{file_prefix}_{program_unique_id}.json"
                
                spec["spec"]["ioConfig"]["inputSource"]["baseDir"] = base_dir_path
                spec["spec"]["ioConfig"]["inputSource"]["filter"] = file_name_final
                spec["spec"]["ioConfig"]["appendToExisting"] = True
        else:
            druid_spec = cloud_spec_str
            spec = json.loads(druid_spec)
            if program_unique_id:
                input_source = spec["spec"]["ioConfig"]["inputSource"]
                current_cloud, uri = re.split("://+", input_source["uris"][0])
                edited_uri = re.split(".json", uri)[0]
                input_source["uris"][0] = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
                spec["spec"]["ioConfig"]["appendToExisting"] = True

        datasource = spec["spec"]["dataSchema"]["dataSource"]
        response = requests.post(DRUID_BATCH_URL, data=json.dumps(spec), headers={'Content-Type': 'application/json'})
        
        if response.status_code == 200:
            self.success_logger.debug(f"started the batch ingestion task sucessfully for the datasource {datasource}")
        else:
            self.error_logger.error(f"failed to start batch ingestion task of {datasource} {response.status_code}")
            self.error_logger.error(response.text)

    def _trigger_druid(self, program_unique_id):
        self.success_logger.info("Ingestion start time")
        self._submit_druid_task(
            ML_DISTINCT_CNT_PROJECTS_STATUS_SPEC, 
            ML_DISTINCT_CNT_PROJECTS_STATUS_LOCAL_SPEC,
            program_unique_id, 
            PROJECTS_DISTINCT_CNT_OUTPUT_DIR, 
            "ml_projects_distinctCount"
        )
        self._submit_druid_task(
            ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_SPEC, 
            ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_LOCAL_SPEC,
            program_unique_id, 
            PROJECTS_DISTINCT_CNT_OUTPUT_DIR_PRGM_LEVEL, 
            "ml_projects_distinctCount_prgmlevel"
        )


# ---------------------------------------------------------------------------
# Data Transformations
# ---------------------------------------------------------------------------
def transform_projects_df(projects_df, utils):
    # Flattening data
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
            regexp_replace(projects_df["solutionInformation"]["name"], "\n|\"", "")
        ).otherwise(regexp_replace(projects_df["title"], "\n|\"", ""))
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
                    regexp_replace(projects_df["exploded_taskarr"]["task_evidence"], "\n|\"", ""),
                    F.lit("'")
                )
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
                    regexp_replace(projects_df["exploded_taskarr"]["prj_evidence"], "\n|\"", ""),
                    F.lit("'")
                )
        ).otherwise(projects_df["exploded_taskarr"]["prj_evidence"])
    )

    config_manager.success_logger.info(
            "Organisation logic start time"
       )
    projects_df = projects_df.withColumn("orgData",utils.get_org_name_udf()(F.col("userProfile.organisations")))
    projects_df = projects_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
    config_manager.success_logger.info(
            "Organisation logic end time"
       )
       
    projects_df = projects_df.withColumn("project_goal",regexp_replace(F.col("metaInformation.goal"), "\n|\"", ""))
    projects_df = projects_df.withColumn("area_of_improvement",F.when((F.col("categories_name").isNotNull()) & (F.col("categories_name")!=""),F.concat(F.lit("'"),regexp_replace(F.col("categories_name"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("categories_name")))
    projects_df = projects_df.withColumn("tasks",F.when((F.col("exploded_taskarr.tasks").isNotNull()) & (F.col("exploded_taskarr.tasks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.tasks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.tasks")))
    projects_df = projects_df.withColumn("sub_task",F.when((F.col("exploded_taskarr.sub_task").isNotNull()) & (F.col("exploded_taskarr.sub_task")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.sub_task"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.sub_task")))	
    projects_df = projects_df.withColumn("program_name",regexp_replace(F.col("programInformation.name"), "\n|\"", ""))
    projects_df = projects_df.withColumn("task_remarks",F.when((F.col("exploded_taskarr.remarks").isNotNull()) & (F.col("exploded_taskarr.remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.remarks")))
    projects_df = projects_df.withColumn("project_remarks",F.when((F.col("exploded_taskarr.prj_remarks").isNotNull()) & (F.col("exploded_taskarr.prj_remarks")!=""),F.concat(F.lit("'"),regexp_replace(F.col("exploded_taskarr.prj_remarks"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("exploded_taskarr.prj_remarks")))

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
        "project_title_editable", F.when((F.col("title").isNotNull()) & (F.col("title")!=""),F.concat(F.lit("'"),regexp_replace(F.col("title"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("title"))
    )
    projects_df = projects_df.withColumn(
        "project_description", F.when((F.col("description").isNotNull()) & (F.col("description")!=""),F.concat(F.lit("'"),regexp_replace(F.col("description"), "\n|\"", ""),F.lit("'"))).otherwise(F.col("description"))
    )

    pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'
    projects_df = projects_df.withColumn('certificate_issued_on', F.regexp_replace(F.col("certificate.issuedOn"), pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))

    projects_df = projects_df.withColumn('certificate_status_customised', F.when(((F.col("certificate.eligible").isNotNull()) & (F.col("certificate.eligible") == True) & (F.col("certificate.osid").isNotNull())),F.lit("Issued")).otherwise(F.lit("")))

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
        projects_df["certificate_status_customised"],		
        projects_df["certificate_issued_on"],
        projects_df["certificate"]["templateUrl"].alias("certificate_template_url"),
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

    config_manager.success_logger.info(
            "Flattening data end time"
       )
    projects_df.unpersist()
    projects_prj_evi.unpersist()
    projects_task_cnt.unpersist()
    projects_dff.unpersist()
    projects_task_cnt.unpersist()
    projects_tsk_evi.unpersist()
    projects_df_cols = projects_df_cols.dropDuplicates()

    config_manager.success_logger.info(
            "Get Entities start time"
       )
    entities_df = utils.melt(prj_df_expl_ul,
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
    config_manager.success_logger.info(
            "Get Entities end time"
       )
       
    config_manager.success_logger.info(
            "Final Dataframe start time"
       )
    projects_df_final = projects_df_cols.join(entities_df_res,projects_df_cols["project_id"]==entities_df_res["_id"],how='left')\
            .drop(entities_df_res["_id"])
    config_manager.success_logger.info(
            "Final Dataframe end time"
       )
    entities_df_res.unpersist()
    projects_df_cols.unpersist()
    final_projects_df = projects_df_final.dropDuplicates()

    necessary_columns = ["state_name","state_externalId","district_name","district_externalId","block_name",
                        "block_externalId","organisation_name","organisation_id"]
    final_df_columns = final_projects_df.columns
    for miss_cols in necessary_columns:
        if miss_cols not in final_df_columns:
            config_manager.success_logger.debug(f"MISSED: {miss_cols}")
            final_projects_df = final_projects_df.withColumn(miss_cols, lit(None).cast(StringType()))
            config_manager.success_logger.debug(f"UPDATED: {final_projects_df.columns}")

    return final_projects_df

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
def main():
    details = argparse.ArgumentParser(description='Pass the ProgramID')
    details.add_argument('--program_id',metavar='--program_id', type=str, help='Program IDs', required=False)
    args = details.parse_args()
    
    program_unique_id = ObjectId(args.program_id) if args.program_id else None

    utils = Utils()
    ingestion_manager = IngestionManager(config_manager)

    config_manager.success_logger.info(
        "Program started"
    )	  

    spark = init_spark_session()
    
    # Mongo Fetch
    projects_cursorMongo = ingestion_manager.fetch_projects(program_unique_id)
    
    # Processing partitions
    func_return = process_project_partition(projects_cursorMongo)
    
    prj_rdd = spark.sparkContext.parallelize(list(func_return)) 
    
    projects_df = spark.createDataFrame(prj_rdd, get_projects_schema())
    
    prj_rdd.unpersist()

    final_projects_df = transform_projects_df(projects_df, utils)
    final_projects_df.cache()
    final_projects_tasks_distinctCnt_df = final_projects_df.groupBy("program_name","program_id","project_title","solution_id","status_of_project","state_name","state_externalId",
                                                                            "district_name","district_externalId","block_name","block_externalId","organisation_name","organisation_id","private_program","project_created_type",
                                                                            "parent_channel").agg(countDistinct(when(F.col("certificate_status_customised") == "Issued",True),F.col("project_id")).alias("no_of_certificate_issued"),countDistinct(F.col("project_id")).alias("unique_projects"),countDistinct(F.col("solution_id")).alias("unique_solution"),countDistinct(F.col("createdBy")).alias("unique_users"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "submitted"),True),F.col("project_id")).alias("no_of_imp_with_evidence"))
    final_projects_tasks_distinctCnt_df = final_projects_tasks_distinctCnt_df.withColumn("time_stamp", current_timestamp())
    final_projects_tasks_distinctCnt_df = final_projects_tasks_distinctCnt_df.dropDuplicates()
    final_projects_tasks_distinctCnt_df.coalesce(1).write.format("json").mode("overwrite").save(config_manager.get("OUTPUT_DIR","projects_distinctCount") + "/")
    final_projects_tasks_distinctCnt_df.unpersist()

    final_projects_tasks_distinctCnt_prgmlevel = final_projects_df.groupBy("program_name", "program_id","status_of_project", "state_name","state_externalId","private_program","project_created_type","parent_channel").agg(countDistinct(when(F.col("certificate_status_customised") == "Issued",True),F.col("project_id")).alias("no_of_certificate_issued"), countDistinct(F.col("project_id")).alias("unique_projects"),countDistinct(F.col("createdBy")).alias("unique_users"),countDistinct(when((F.col("evidence_status") == True)&(F.col("status_of_project") == "submitted"),True),F.col("project_id")).alias("no_of_imp_with_evidence"))
    final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.withColumn("time_stamp", current_timestamp())
    final_projects_tasks_distinctCnt_prgmlevel = final_projects_tasks_distinctCnt_prgmlevel.dropDuplicates()
    final_projects_tasks_distinctCnt_prgmlevel.coalesce(1).write.format("json").mode("overwrite").save(config_manager.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/")
    final_projects_df.unpersist()
    final_projects_tasks_distinctCnt_prgmlevel.unpersist()

    ingestion_manager.upload_and_trigger(program_unique_id)

if __name__ == "__main__":
    main()