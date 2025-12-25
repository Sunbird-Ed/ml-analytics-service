# -----------------------------------------------------------------
# Name : pyspark_project_batch_agg_modified.py
# Author : Shakthiehswari, Ashwini, Snehangsu
# Description : Extracts the Status of the Project submissions 
#  either Started / In-Progress / Submitted along with the users 
#  entity information
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
SUCCESS_LOG = config.get('LOGS', 'project_success')
ERROR_LOG = config.get('LOGS', 'project_error')
ML_DISTINCT_CNT_PROJECTS_STATUS_SPEC = config.get("DRUID","ml_distinctCnt_projects_status_spec")
ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_SPEC = config.get("DRUID","ml_distinctCnt_prgmlevel_projects_status_spec")
ML_DISTINCT_CNT_PROJECTS_STATUS_LOCAL_SPEC = config.get("DRUID", "ml_distinctCnt_projects_status_local_spec", fallback=None)
ML_DISTINCT_CNT_PRGMLEVEL_PROJECTS_STATUS_LOCAL_SPEC = config.get("DRUID", "ml_distinctCnt_prgmlevel_projects_status_local_spec", fallback=None)
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
        formatter = logging.Formatter('%(asctime)s - %(levelname)s')
        
        # Success Logger
        self.success_logger = logging.getLogger('success log')
        self.success_logger.setLevel(logging.DEBUG)
        if not self.success_logger.handlers:
            successHandler = RotatingFileHandler(SUCCESS_LOG)
            successBackuphandler = TimedRotatingFileHandler(SUCCESS_LOG, when="w0",backupCount=1)
            successHandler.setFormatter(formatter)
            self.success_logger.addHandler(successHandler)
            self.success_logger.addHandler(successBackuphandler)

        # Error Logger
        self.error_logger = logging.getLogger('error log')
        self.error_logger.setLevel(logging.ERROR)
        if not self.error_logger.handlers:
            errorHandler = RotatingFileHandler(ERROR_LOG)
            errorBackuphandler = TimedRotatingFileHandler(ERROR_LOG,when="w0",backupCount=1)
            errorHandler.setFormatter(formatter)
            self.error_logger.addHandler(errorHandler)
            self.error_logger.addHandler(errorBackuphandler)

    def get(self, section, option, fallback=None):
        return self.config.get(section, option, fallback=fallback)
    
    def get_logger(self):
        return self.success_logger, self.error_logger


config_manager = ConfigManager()
from cloud import MultiCloud 

# ---------------------------------------------------------------------------
# Spark Setup
# ---------------------------------------------------------------------------
def init_spark_session(app_name="projects"):
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
                for org in (val or []) if org and (org["isSchool"] == False or org["isSchool"] is None)
            ]
        return udf(orgName, orgSchema)

# ---------------------------------------------------------------------------
# Partition Processing
# ---------------------------------------------------------------------------
def process_project_partition(partition_iterator):
    """Flattens project data, handling tasks and subtasks."""
    
    def get_defaults(d, key, default=None):
        return d.get(key) if d.get(key) is not None else default

    def task_detail(task, del_flg, cntr):
        if not isinstance(task, dict): return None
        
        # Ternary operator for task evidence status
        has_evidence = len(task.get("attachments", [])) > 0
        
        return {
            "_id": task.get("_id"),
            "tasks": task.get("name"),
            "task_sequence": cntr,
            "deleted_flag": del_flg,
            "task_evidence_status": True if has_evidence else False,
            "assignee": task.get("assignee", ""),
            "startDate": task.get("startDate", ""),
            "endDate": task.get("endDate", ""),
            "syncedAt": task.get("syncedAt"),
            "status": task.get("status", "")
        }

    processed_projects = []
    
    for prj in partition_iterator:
        prjinfo = []
        project_attachments = prj.get("attachments", [])
        
        # Process Project Attachments/Remarks
        if project_attachments:
            for cnt, attachment in enumerate(project_attachments):
                prjObj = {}
                if cnt == 0 and "remarks" in prj:
                    prjObj["prj_remarks"] = prj["remarks"]
                
                attach_type = attachment.get("type")
                prjObj["prjEvi_type"] = attach_type
                # Ternary for evidence link/path
                prjObj["prj_evidence"] = attachment.get("name") if attach_type == "link" else attachment.get("sourcePath")
                prjinfo.append(prjObj)
        elif prj.get("remarks"):
            prjinfo.append({"prj_remarks": prj["remarks"]})

        taskarr = []
        cntr = 1
        
        for task in prj.get("tasks", []):
            if not isinstance(task, dict): continue

            attachments = task.get("attachments", [])
            children = task.get("children", [])
            
            try:
                attachLen = len(attachments)
            except:
                attachLen = 0
                
            try:
                sub_tskLen = len(children)
            except:
                sub_tskLen = 0
            
            try:
                del_flg = task["isDeleted"]
            except:
                del_flg = False
                
            # Determine max length for looping
            arr_len = 0
            if attachLen > sub_tskLen:
                arr_len = attachLen
            elif sub_tskLen > attachLen:
                arr_len = sub_tskLen
            elif ((sub_tskLen == attachLen) and (sub_tskLen == 0)):
                if del_flg == False:
                    taskObj = task_detail(task, del_flg, cntr)
                    if "remarks" in task: taskObj["remarks"] = task["remarks"]
                    taskarr.append(taskObj)
                arr_len = sub_tskLen # should be 0
            elif (sub_tskLen == attachLen):
                arr_len = sub_tskLen

            for index in range(arr_len):
                if del_flg == False:
                    taskObj = task_detail(task, del_flg, cntr)
                    
                    # Task Evidence
                    try:
                        att = attachments[index]
                        e_type = att.get("type", "")
                        taskObj["taskEvi_type"] = e_type
                        if e_type == "link":
                                taskObj["task_evidence"] = att.get("name", "")
                        else:
                                taskObj["task_evidence"] = att.get("sourcePath", "")
                    except:
                        pass
                    
                    # Sub Task Name
                    try:
                        taskObj["sub_task"] = children[index].get("name", "")
                    except:
                        pass

                    # Remarks
                    if index == 0:
                        try:
                            taskObj["remarks"] = task["remarks"]
                        except:
                            pass

                    # Sub Task Details
                    try:
                        if children:
                            children_list = children # Fixed variable reference
                            try:
                                sub_del_flg = children_list[index]["isDeleted"]
                            except:
                                sub_del_flg = False
                            
                            if sub_del_flg == False:
                                    sub_task = children_list[index]
                                    taskObj.update({
                                        "sub_task_date": sub_task.get("syncedAt"),
                                        "sub_task_id": sub_task.get("_id"),
                                        "sub_task_status": sub_task.get("status"),
                                        "sub_task_deleted_flag": sub_del_flg,
                                        "sub_task_start_date": sub_task.get("startDate", ""),
                                        "sub_task_end_date": sub_task.get("endDate", None)
                                    })
                                    taskarr.append(taskObj)
                            else:
                                    taskarr.append(taskObj)
                    except IndexError:
                        taskarr.append(taskObj)
                    except:
                        taskarr.append(taskObj)
            
            cntr += 1

        # Flatten logic: Combine Project Info and Task Info
        prjinfo_len = len(prjinfo)
        taskarr_len = len(taskarr)

        if taskarr_len == 0 and prjinfo_len > 0:
            taskarr.extend(prjinfo)
        elif taskarr_len >= prjinfo_len and prjinfo_len > 0:
                for ind in range(prjinfo_len):
                    taskarr[ind].update(prjinfo[ind])
        elif taskarr_len < prjinfo_len:
                for ind in range(taskarr_len):
                    taskarr[ind].update(prjinfo[ind])
                taskarr.extend(prjinfo[taskarr_len:])
        
        prj["taskarr"] = taskarr
        if "tasks" in prj: del prj["tasks"]
        
        processed_projects.append(prj)

    return iter(processed_projects)

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

        self.success_logger.debug("Mongo Query start time  " + str(datetime.datetime.now()))
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
        self.success_logger.debug("Mongo Query end time  " + str(datetime.datetime.now()))
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
        self.success_logger.debug("Renaming, Uploading, Removing files start time  " + str(datetime.datetime.now()))
        
        # Projects Submission Distinct Count
        self._process_file_operation(PROJECTS_DISTINCT_CNT_OUTPUT_DIR, PROJECTS_DISTINCT_CNT_BLOB_PATH, "ml_projects_distinctCount", program_unique_id)

        # Projects Submission Distinct Count Program Level
        self._process_file_operation(PROJECTS_DISTINCT_CNT_OUTPUT_DIR_PRGM_LEVEL, PROJECTS_DISTINCT_CNT_BLOB_PATH_PRGM_LEVEL, "ml_projects_distinctCount_prgmlevel", program_unique_id)

        self.success_logger.debug("Renaming, Uploading, Removing files end time  " + str(datetime.datetime.now()))
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
        self.success_logger.debug("Ingestion start time  " + str(datetime.datetime.now()))
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
    # Helpers
    def clean_str(col_name):
        """Replaces newlines and quotes with empty string."""
        return F.regexp_replace(F.col(col_name), "\n|\"", "")

    def bool_to_str(cond):
        """Converts boolean condition to 'true'/'false' string."""
        return F.when(cond, "true").otherwise("false")

    def evidence_url_builder(evidence_col, type_col):
        """Builds evidence URL or string based on type."""
        return F.when(
            (evidence_col.isNotNull()) & (type_col != "link"),
            F.concat(F.lit(config_manager.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')), evidence_col)
        ).when(
            (evidence_col.isNotNull()) & (evidence_col != "") & (type_col == "link"),
            F.concat(F.lit("'"), F.regexp_replace(evidence_col, "\n|\"", ""), F.lit("'"))
        ).otherwise(evidence_col)
    
    # 1. Project Info Transformations
    projects_df = projects_df.withColumn(
        "project_created_type",
        F.when(F.col("projectTemplateId").isNotNull(), "project imported from library")
         .otherwise("user created project")
    ).withColumn(
        "project_title",
        F.when(F.col("solutionInformation.name").isNotNull(), 
               F.regexp_replace(F.col("solutionInformation.name"), "\n|\"", ""))
         .otherwise(clean_str("title"))
    ).withColumn(
        "project_deleted_flag", bool_to_str(F.col("isDeleted") == True)
    ).withColumn(
        "private_program", bool_to_str(F.col("isAPrivateProgram") != False) # Logic matched: if true->true, if false->false, else->true
    ).withColumn(
        "project_terms_and_condition", bool_to_str(F.col("hasAcceptedTAndC") == True)
    ).withColumn(
        "project_evidence_status", F.size(F.col("attachments")) >= 1
    ).withColumn(
        "project_completed_date",
        F.when(F.col("status") == "submitted", F.col("updatedAt")).otherwise(None)
    )

    # 2. Category Explosion
    projects_df = projects_df.withColumn("exploded_categories", F.explode_outer(F.col("categories")))
    category_df = projects_df.groupby('_id').agg(collect_list('exploded_categories.name').alias("category_name"))
    category_df = category_df.withColumn("categories_name", concat_ws(", ", "category_name"))
    projects_df = projects_df.join(category_df, "_id", how = "left")
    category_df.unpersist()

    projects_df = projects_df.withColumn("parent_channel", F.lit("SHIKSHALOKAM"))

    # 3. Task Explosion and Evidence
    projects_df = projects_df.withColumn("exploded_taskarr", F.explode_outer(projects_df["taskarr"]))

    projects_df = projects_df.withColumn(
        "task_evidence", 
        evidence_url_builder(F.col("exploded_taskarr.task_evidence"), F.col("exploded_taskarr.taskEvi_type"))
    ).withColumn(
        "project_evidence",
        evidence_url_builder(F.col("exploded_taskarr.prj_evidence"), F.col("exploded_taskarr.prjEvi_type"))
    )

    projects_df = projects_df.withColumn(
        "task_deleted_flag", bool_to_str(F.col("exploded_taskarr.deleted_flag") == True)
    ).withColumn(
        "sub_task_deleted_flag", bool_to_str(F.col("exploded_taskarr.sub_task_deleted_flag") == True)
    )

    # 4. Organization Logic
    config_manager.success_logger.debug("Organisation logic start time  " + str(datetime.datetime.now()))
    projects_df = projects_df.withColumn("orgData", utils.get_org_name_udf()(F.col("userProfile.organisations")))
    projects_df = projects_df.withColumn("exploded_orgInfo", F.explode_outer(F.col("orgData")))
    config_manager.success_logger.debug("Organisation logic end time  " + str(datetime.datetime.now()))
    
    # 5. Text Cleanup
    text_cols = {
        "project_goal": "metaInformation.goal",
        "program_name": "programInformation.name"
    }
    for new_col, src_col in text_cols.items():
        projects_df = projects_df.withColumn(new_col, clean_str(src_col))

    quoted_text_cols = {
        "area_of_improvement": "categories_name",
        "tasks": "exploded_taskarr.tasks",
        "sub_task": "exploded_taskarr.sub_task",
        "task_remarks": "exploded_taskarr.remarks",
        "project_remarks": "exploded_taskarr.prj_remarks",
        "project_title_editable": "title",
        "project_description": "description"
    }
    
    for new_col, src_col in quoted_text_cols.items():
        projects_df = projects_df.withColumn(
            new_col,
            F.when(
                (F.col(src_col).isNotNull()) & (F.col(src_col) != ""),
                F.concat(F.lit("'"), clean_str(src_col), F.lit("'"))
            ).otherwise(F.col(src_col))
        )

    # 6. Evidence Status Aggregation
    projects_df = projects_df.withColumn(
         "evidence_status",
         (F.col("project_evidence_status") == True) | 
         ((F.col("project_evidence_status") == False) & (F.col("exploded_taskarr.task_evidence_status") == True))
         # Optimized logic covering the combinations
    )

    # 7. User Locations
    prj_df_expl_ul = projects_df.withColumn(
        "exploded_userLocations", F.explode_outer(projects_df["userProfile"]["userLocations"])
    )

    # 8. Certificate
    pattern = r'(?:.*)YEAR=(\d+).+?MONTH=(\d+).+?DAY_OF_MONTH=(\d+).+?HOUR=(\d+).+?MINUTE=(\d+).+?SECOND=(\d+).+'
    projects_df = projects_df.withColumn('certificate_issued_on', F.regexp_replace(F.col("certificate.issuedOn"), pattern, '$1-$2-$3 $4:$5:$6').cast('timestamp'))
    projects_df = projects_df.withColumn(
        'certificate_status_customised', 
        F.when(
            (F.col("certificate.eligible") == True) & (F.col("certificate.osid").isNotNull()), 
            F.lit("Issued")
        ).otherwise(F.lit(""))
    )

    # 9. Selection
    projects_df_cols = projects_df.select(
        projects_df["_id"].alias("project_id"),
        "project_created_type", "project_title", "project_title_editable",
        projects_df["programId"].alias("program_id"),
        projects_df["programExternalId"].alias("program_externalId"),
        "program_name",
        projects_df["metaInformation"]["duration"].alias("project_duration"),
        projects_df["syncedAt"].alias("project_last_sync"),
        projects_df["updatedAt"].alias("project_updated_date"),
        "project_deleted_flag", "area_of_improvement",
        projects_df["status"].alias("status_of_project"),
        projects_df["userId"].alias("createdBy"),
        "project_description", "project_goal", "project_evidence", "parent_channel",
        projects_df["createdAt"].alias("project_created_date"),
        projects_df["exploded_taskarr"]["_id"].alias("task_id"),
        "tasks", "project_remarks",
        projects_df["exploded_taskarr"]["assignee"].alias("task_assigned_to"),
        projects_df["exploded_taskarr"]["startDate"].alias("task_start_date"),
        projects_df["exploded_taskarr"]["endDate"].alias("task_end_date"),
        projects_df["exploded_taskarr"]["syncedAt"].alias("tasks_date"),
        projects_df["exploded_taskarr"]["status"].alias("tasks_status"),
        "task_evidence",
        projects_df["exploded_taskarr"]["task_evidence_status"].alias("task_evidence_status"),
        projects_df["exploded_taskarr"]["sub_task_id"].alias("sub_task_id"),
        "sub_task",
        projects_df["exploded_taskarr"]["sub_task_status"].alias("sub_task_status"),
        projects_df["exploded_taskarr"]["sub_task_date"].alias("sub_task_date"),
        projects_df["exploded_taskarr"]["sub_task_start_date"].alias("sub_task_start_date"),
        projects_df["exploded_taskarr"]["sub_task_end_date"].alias("sub_task_end_date"),
        "private_program", "task_deleted_flag", "sub_task_deleted_flag",
        "project_terms_and_condition", "task_remarks",
        projects_df["exploded_taskarr"]["task_sequence"].alias("task_sequence"),
        "project_completed_date",
        projects_df["solutionInformation"]["_id"].alias("solution_id"),
        projects_df["userRoleInformation"]["role"].alias("designation"),
        projects_df["userProfile"]["rootOrgId"].alias("channel"),
        projects_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
        projects_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
        projects_df["certificate"]["osid"].alias("certificate_id"),	
        projects_df["certificate"]["status"].alias("certificate_status"),
        "certificate_status_customised", "certificate_issued_on",
        projects_df["certificate"]["templateUrl"].alias("certificate_template_url"),
        concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
        concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type"),
        "evidence_status"    
    )

    # 10. Aggregations on IDs
    projects_task_cnt = projects_df_cols.groupBy("project_id").agg(countDistinct(F.col("task_id")).alias("task_count"))
    projects_prj_evi = projects_df_cols.groupBy("project_id").agg(countDistinct("project_evidence").alias("project_evidence_count"))
    projects_tsch_evi = projects_df_cols.groupBy("project_id").agg(countDistinct("task_evidence").alias("task_evidence_count"))
    
    # Joins
    projects_df_cols = projects_df_cols.join(projects_task_cnt, ["project_id"], "left") \
                                     .join(projects_prj_evi, ["project_id"], "left") \
                                     .join(projects_tsch_evi, ["project_id"], "left")

    config_manager.success_logger.debug("Flattening data end time  " + str(datetime.datetime.now()))
    
    projects_df.unpersist()
    projects_task_cnt.unpersist()
    projects_prj_evi.unpersist()
    projects_tsch_evi.unpersist()
    
    projects_df_cols = projects_df_cols.dropDuplicates()

    # 11. Location Entities (Melt)
    config_manager.success_logger.debug("Get Entities start time  " + str(datetime.datetime.now()))
    entities_df = utils.melt(prj_df_expl_ul,
            id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
            value_vars=["exploded_userLocations.code"]).select("_id","name","value","type","id").dropDuplicates()
    prj_df_expl_ul.unpersist()
    
    entities_df = entities_df.withColumn("variable",F.concat(F.col("type"),F.lit("_externalId"))) \
                             .withColumn("variable1",F.concat(F.col("type"),F.lit("_name"))) \
                             .withColumn("variable2",F.concat(F.col("type"),F.lit("_code")))

    entities_df_id = entities_df.groupBy("_id").pivot("variable").agg(first("id"))
    entities_df_name = entities_df.groupBy("_id").pivot("variable1").agg(first("name"))
    entities_df_value = entities_df.groupBy("_id").pivot("variable2").agg(first("value"))

    entities_df_res = entities_df_id.join(entities_df_name,["_id"],how='outer') \
                                    .join(entities_df_value,["_id"],how='outer') \
                                    .drop('null')

    entities_df.unpersist()
    config_manager.success_logger.debug("Get Entities end time  " + str(datetime.datetime.now()))
    
    # 12. Final Join
    config_manager.success_logger.debug("Final Dataframe start time  " + str(datetime.datetime.now()))
    projects_df_final = projects_df_cols.join(entities_df_res, projects_df_cols["project_id"]==entities_df_res["_id"], how='left').drop(entities_df_res["_id"])
    config_manager.success_logger.debug("Final Dataframe end time  " + str(datetime.datetime.now()))
    
    entities_df_res.unpersist()
    projects_df_cols.unpersist()
    final_projects_df = projects_df_final.dropDuplicates()

    # 13. Backfill Nulls
    necessary_columns = ["state_name","state_externalId","district_name","district_externalId","block_name",
                        "block_externalId","organisation_name","organisation_id"]
    final_df_columns = set(final_projects_df.columns)
    missing_cols = [c for c in necessary_columns if c not in final_df_columns]
    
    if missing_cols:
         final_projects_df = final_projects_df.select("*", *(lit(None).cast(StringType()).alias(c) for c in missing_cols))

    projects_df_final.unpersist()
    
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

    config_manager.success_logger.debug(
        "Program started  " + str(datetime.datetime.now())
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