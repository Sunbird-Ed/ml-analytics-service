import os
import sys
import glob
import shutil
import argparse
import logging
import json
import re
import time
import requests
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import datetime, timedelta, timezone
from typing import Iterable
from pymongo import MongoClient
from bson import ObjectId
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, array, struct, explode, udf, regexp_replace, collect_list, concat_ws, size, countDistinct, array_distinct, first
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    BooleanType, TimestampType, ArrayType
)
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
config = ConfigParser(interpolation=ExtendedInterpolation())
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config.read(os.path.join(base_path, "config.ini"))
NATURE_OF_UPLOAD = "cloud" # "local" or "cloud"
MONGO_URL = config.get('MONGO', 'url')
MONGO_DATABASE_NAME = config.get('MONGO', 'database_name')
MONGO_PROJECTS_COLLECTION = config.get('MONGO', 'projects_collection')
MONGO_SOLUTIONS_COLLECTION = config.get('MONGO', 'solutions_collection')
MONGO_PROGRAMACTIVITYLOG_COLLECTION = config.get('MONGO', 'programActivityLog_collection')
SUCCESS_LOG_PATH = config.get('LOGS', 'project_raw_success')
ERROR_LOG_PATH = config.get('LOGS', 'project_raw_error')
DRUID_BATCH_URL = config.get("DRUID", "batch_url")
SL_PROJECT_LOCAL_INGESTION_SPEC = config.get("DRUID","project_local_ingestion_spec", fallback=None)
SL_PROJECT_CLOUD_INGESTION_SPEC = config.get("DRUID","project_injestion_spec")
SL_PROJECT_BLOB_PATH = config.get("COMMON", "projects_blob_path")
SL_PROJECT_OUTPUT_DIR = config.get("OUTPUT_DIR", "project")
CLOUD_MODULE_PATH = config.get("COMMON", "cloud_module_path")

# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Is it first time running the Ingestion job?"
    )

    parser.add_argument(
        "--is_first_time",
        required=False,
        type=bool,
        help="pass True if it is first time running the Ingestion job else False",
    )
    return parser.parse_args()

args = parse_args()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
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
        self.success_logger.setLevel(logging.DEBUG)
        if not self.success_logger.handlers:
            successHandler = TimedRotatingFileHandler(SUCCESS_LOG_PATH, when="w0", backupCount=1)
            successHandler.setFormatter(formatter)
            self.success_logger.addHandler(successHandler)
        self.success_logger.propagate = False

        # Error Logger
        self.error_logger = logging.getLogger('error log')
        self.error_logger.setLevel(logging.ERROR)
        if not self.error_logger.handlers:
            errorHandler = TimedRotatingFileHandler(ERROR_LOG_PATH, when="w0", backupCount=1)
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
def init_spark_session(app_name="sl-project"):
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
        self.config = config
    
    def melt(self, df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str], var_name: str="variable", value_name: str="value") -> DataFrame:
        """Unpivot DataFrame from wide to long format."""
        _vars_and_vals: Iterable[str] = array(*(
            struct(lit(c).alias(var_name), col(c).alias(value_name))
            for c in value_vars))

        _tmp: DataFrame = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
        cols: Iterable[str] = id_vars + [col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
        return _tmp.select(*cols)

    def get_org_name_udf(self):
        """Returns the UDF for processing organization names."""
        orgSchema: ArrayType = ArrayType(StructType([
            StructField("orgId", StringType(), False),
            StructField("orgName", StringType(), False)
        ]))

        def orgName(val):
            orgarr: list = []
            if val is not None:
                for org in val:
                    if org and org["isSchool"] == False: 
                        orgarr.append({
                            'orgId': org['organisationId'],
                            'orgName': org["orgName"]
                        })
            return orgarr
        return udf(orgName, orgSchema)

    def segement_deletion(self, created_at: str, datasources: str) -> None:
        """
        Delete Druid segments for a given created_at timestamp and datasource.
        """
        created_at_utc = self.to_utc(created_at)
        min_time = created_at_utc.replace(microsecond=0)
        max_time = min_time + timedelta(seconds=1)
        min_iso = min_time.isoformat().replace("+00:00", "Z")
        max_iso = max_time.isoformat().replace("+00:00", "Z")

        interval = min_iso + "/" + max_iso
        headers: dict = {'Content-Type': 'application/json'}
        
        # ---- Druid config ----
        indexer_url: str = DRUID_BATCH_URL

        payload: dict = {
            "type": "kill",
            "dataSource": datasources,
            "interval": interval,
            "markAsUnused": True
        }

        response: requests.Response = requests.post(indexer_url, headers=headers, json=payload)

        if config_manager and config_manager.success_logger:
            config_manager.success_logger.info(f"Status Code: {response.status_code}")
            config_manager.success_logger.info(f"Response: {response.json()}")
        # time.sleep(120)


    def to_utc(self, dt) -> datetime:
        """
        Convert datetime to UTC.
        - If timezone-aware → convert properly
        - If naive → assume UTC (MongoDB default)
        """
        if dt.tzinfo is None:
            # Naive datetime → assume UTC
            return dt.replace(tzinfo=timezone.utc)
        else:
            # Aware datetime → convert to UTC
            return dt.astimezone(timezone.utc)
        
    def delete_entire_datasource(self, datasource: str) -> None:
        """
        Delete entire Druid datasource.
        """
        headers: dict = {'Content-Type': 'application/json'}
        
        # ---- Druid config ----
        indexer_url: str = DRUID_BATCH_URL

        payload: dict = {
            "type": "kill",
            "dataSource": datasource,
            "interval": "1000-01-01T00:00:00Z/3000-01-01T00:00:00Z",
            "markAsUnused": True
        }

        # ---- Submit kill task ----
        response: requests.Response = requests.post(indexer_url, headers=headers, json=payload)
        
        if config_manager and config_manager.success_logger:
            config_manager.success_logger.info(f"Status Code: {response.status_code}")
            config_manager.success_logger.info(f"Response: {response.json()}")
        # time.sleep(600)

    def delete_local_output_file(self, solution_id: str) -> None:
        if solution_id:
            os.remove(SL_PROJECT_OUTPUT_DIR + f"/sl_project_{solution_id}.json")
        else:
            os.remove(SL_PROJECT_OUTPUT_DIR + "/sl_project.json")


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
def get_projects_schema():
    return StructType([
        StructField('_id', StringType(), True),
        StructField('projectTemplateId', StringType(), True),
        StructField('solutionInformation', StructType([
            StructField('name', StringType(), True), 
            StructField('_id', StringType(), True)
        ])),
        StructField('title', StringType(), True),
        StructField('programId', StringType(), True),
        StructField('programExternalId', StringType(), True),
        StructField('programInformation', StructType([StructField('name', StringType(), True)])),
        StructField('metaInformation', StructType([
            StructField('duration', StringType(), True), 
            StructField('goal', StringType(), True)
        ])),
        StructField('updatedAt', TimestampType(), True),
        StructField('syncedAt', TimestampType(), True),
        StructField('isDeleted', BooleanType(), True),
        StructField('status', StringType(), True),
        StructField('userId', StringType(), True),
        StructField('description', StringType(), True),
        StructField('createdAt', TimestampType(), True),
        StructField('isAPrivateProgram', BooleanType(), True),
        StructField('hasAcceptedTAndC', BooleanType(), True),
        StructField('categories', ArrayType(StructType([StructField('name', StringType(), True)])), True),
        StructField('userRoleInformation', StructType([StructField('role', StringType(), True)])),
        StructField('userProfile', StructType([
            StructField('rootOrgId', StringType(), True),
            StructField('framework', StructType([StructField('board',ArrayType(StringType()), True)])),
            StructField('organisations',ArrayType(StructType([
                StructField('organisationId', StringType(), True),
                StructField('orgName', StringType(), True),
                StructField('isSchool', BooleanType(), True)
            ]), True)),
            StructField('profileUserTypes',ArrayType(StructType([StructField('type', StringType(), True)]), True)),
            StructField('userLocations', ArrayType(StructType([
                StructField('name', StringType(), True),
                StructField('type', StringType(), True),
                StructField('id', StringType(), True),
                StructField('code', StringType(), True)
            ]),True))
        ])),
        StructField('taskarr', ArrayType(StructType([
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
        ])),True),
        StructField('remarks', StringType(), True),
        StructField('certificate', StructType([
            StructField('osid', StringType(), True),
            StructField('status', StringType(), True),
            StructField('issuedOn', StringType(), True),
            StructField('templateUrl', StringType(),True),
            StructField('eligible',BooleanType(), True)
        ])),
        StructField('attachments', ArrayType(StructType([
            StructField('sourcePath', StringType(), True),
            StructField('name', StringType(), True),
            StructField('type', StringType(), True)
        ])), True)
    ])

def get_solution_schema():
    return StructType([
        StructField('_id', StringType(), True),
        StructField('createdAt', TimestampType(), True)
    ])


# ---------------------------------------------------------------------------
# Ingestion Manager Class
# ---------------------------------------------------------------------------
class IngestionManager:
    def __init__(self, config: ConfigManager):
        self.config = config
        self.client = MongoClient(MONGO_URL)
        self.db = self.client[MONGO_DATABASE_NAME]
        self.projects_collection = self.db[MONGO_PROJECTS_COLLECTION]
        self.solutions_collection = self.db[MONGO_SOLUTIONS_COLLECTION]
        self.programActivityLog_collection = self.db[MONGO_PROGRAMACTIVITYLOG_COLLECTION]
        self.success_logger, self.error_logger = config.get_logger()

    def get_all_solution_ids(self)-> list:
        """Fetches distinct solution IDs to be processed."""
        query: dict = {"isAPrivateProgram": False, "isDeleted": False}
        solution_ids: list = self.projects_collection.distinct("solutionId", query)
        valid_ids: list = [str(sid) for sid in solution_ids if str(sid) != 'None']
        with open("solution_ids.txt", "w") as f:
            for sid in valid_ids:
                f.write(f"{sid}\n")
        return valid_ids

    def fetch_solution_details(self, solution_id):
        solution_doc = self.solutions_collection.find_one(
            {"_id": ObjectId(solution_id)},
            {"_id": 1, "createdAt": 1}
        )
        if solution_doc:
            solution_doc['_id'] = str(solution_doc['_id'])
        return solution_doc

    def fetch_recent_updated_solution_ids(self) -> dict:
        today = datetime.now().date()
        three_days_ago = today - timedelta(days=3)
        query = {
            "date": {
                "$gte": three_days_ago.strftime("%Y-%m-%d"),
                "$lte": today.strftime("%Y-%m-%d")
            },
            "improvementProjectStatus.completedAt": {
                "$exists": False
            }
        }
        records = list(self.programActivityLog_collection.find(query))   
        records_obj = {}
        for record in records:
            unique_solution_ids = set()
            for proj in record.get("activity", {}).get("improvementProject", []):
                unique_solution_ids.update(proj.get("solutionIds", []))

            records_obj[str(record.get("_id"))] = list(unique_solution_ids)  
            if self.success_logger:    
                self.success_logger.info(f"records_obj: {records_obj}")
        return records_obj

    def update_program_activity_status_start(self, log_id: str):
        """Updates the startedAt timestamp for improvementProjectStatus."""
        try:
            self.programActivityLog_collection.update_one(
                {"_id": ObjectId(log_id)},
                {"$set": {"improvementProjectStatus.startedAt": datetime.now()}}
            )
            if self.success_logger:
                self.success_logger.info(f"Updated startedAt key at programActivityLog collection for the log_id: {log_id}")
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Error updating startedAt key at programActivityLog collection for log_id {log_id}: {e}")

    def update_program_activity_progress(self, log_id: str, solution_id: str, total_count: int):
        """Pushes progress data to improvementProjectStatus."""
        try:
            progress_entry = {
                "processedSolutionId": solution_id,
                "totalProjectCount": total_count,
                "processedAt": datetime.now()
            }
            self.programActivityLog_collection.update_one(
                {"_id": ObjectId(log_id)},
                {"$push": {"improvementProjectStatus.progressData": progress_entry}}
            )
            if self.success_logger:
                self.success_logger.info(f"Updated progressData key at programActivityLog collection for log_id: {log_id}, solution: {solution_id}")
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Error updating progressData key at programActivityLog collection for log_id {log_id}: {e}")

    def update_program_activity_completed(self, log_id: str, total_processed_projects: int):
        """Updates the completedAt timestamp and total count for improvementProjectStatus."""
        try:
            self.programActivityLog_collection.update_one(
                {"_id": ObjectId(log_id)},
                {"$set": {
                    "improvementProjectStatus.completedAt": datetime.now(),
                    "improvementProjectStatus.total_processed_projects": total_processed_projects
                }}
            )
            if self.success_logger:
                self.success_logger.info(f"Updated completedAt key at programActivityLog collection for log_id: {log_id}")
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Error updating completedAt key at programActivityLog collection for log_id {log_id}: {e}")
        

    def fetch_projects_for_solution(self, solution_id):
        """Fetches projects for a given solution using aggregation pipeline."""
        base_match = [
            {"isAPrivateProgram": False}, 
            {"isDeleted": False},
            {"solutionId": ObjectId(solution_id)},
        ]
        
        project_query = {"$match": {"$and": base_match}}

        pipeline = [
            project_query,
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "projectTemplateId": {"$toString": "$projectTemplateId"},
                    "solutionInformation": {"name": 1, "_id": {"$toString": "$solutionInformation._id"}},
                    "title": { "$reduce": { "input": { "$split": ["$title", "\n"] }, "initialValue": "", "in": { "$concat": ["$$value", " ", "$$this"] } } },
                    "remarks": 1, "attachments": 1, "status": 1, "userId": 1, "createdAt": 1,
                    "programId": {"$toString": "$programId"},
                    "programInformation": {"name": 1},
                    "metaInformation": {"duration": 1, "goal": 1},
                    "syncedAt": 1, "updatedAt": 1, "isDeleted": 1, "categories": 1, "tasks": 1,
                    "description": { "$reduce": { "input": { "$split": ["$description", "\n"] }, "initialValue": "", "in": { "$concat": ["$$value", " ", "$$this"] } } },
                    "programExternalId": 1, "isAPrivateProgram": 1, "hasAcceptedTAndC": 1,
                    "userRoleInformation": 1, "userProfile": 1, "certificate": 1
                }
            }
        ]
        
        try:
            cursor = self.projects_collection.aggregate(pipeline)
            projects_list = list(cursor)
            if self.success_logger:
                self.success_logger.info(f"Mongo Query completed for solution {solution_id}, found {len(projects_list)} records")
            return projects_list
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Error fetching projects for solution {solution_id}: {e}")
            raise e

# ---------------------------------------------------------------------------
# Transformations 
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
                F.lit(config_manager.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
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
                F.lit(config_manager.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url')),
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

    projects_df = projects_df.withColumn("orgData",utils.get_org_name_udf()(F.col("userProfile.organisations")))
    projects_df = projects_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))
       
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

    if config_manager.success_logger:
        config_manager.success_logger.info("Flattening data completed")
        config_manager.success_logger.info("Getting Entities data from userLocations")

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

    projects_df.unpersist()
    projects_prj_evi.unpersist()
    projects_task_cnt.unpersist()
    projects_dff.unpersist()
    projects_task_cnt.unpersist()
    projects_tsk_evi.unpersist()
    projects_df_cols = projects_df_cols.dropDuplicates()

    entities_df = utils.melt(prj_df_expl_ul,
            id_vars=["_id","exploded_userLocations.name","exploded_userLocations.type","exploded_userLocations.id"],
            value_vars=["exploded_userLocations.code"]
        ).select("_id","name","value","type","id").dropDuplicates()
    prj_df_expl_ul.unpersist()
    
    # Filter to only valid location types to prevent pivot errors
    location_types = ["state", "district", "block", "cluster", "school"]
    entities_df = entities_df.filter(F.col("type").isin(location_types))
    
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
       
    projects_df_final = projects_df_cols.join(entities_df_res,projects_df_cols["project_id"]==entities_df_res["_id"],how='left')\
            .drop(entities_df_res["_id"])
    entities_df_res.unpersist()
    projects_df_cols.unpersist()
    final_projects_df = projects_df_final.dropDuplicates()

    necessary_columns = ["state_name","state_externalId","district_name","district_externalId","block_name",
                        "block_externalId","organisation_name","organisation_id"]
    final_df_columns = final_projects_df.columns
    for miss_cols in necessary_columns:
        if miss_cols not in final_df_columns:
            final_projects_df = final_projects_df.withColumn(miss_cols, lit(None).cast(StringType()))

    return final_projects_df



# ---------------------------------------------------------------------------
# Druid Ingestion
# ---------------------------------------------------------------------------
def trigger_druid_ingestion(solution_id, config: ConfigManager):
    """
    Triggers Druid batch ingestion task.
    """
    success_logger, error_logger = config.get_logger()
    
    entitiesArr = [
        "state_externalId", "block_externalId", "district_externalId", "cluster_externalId", "school_externalId",
        "state_name","block_name","district_name","cluster_name","school_name","board_name","state_code", 
        "block_code", "district_code", "cluster_code", "school_code"
    ]
    
    submissionReportColumnNamesArr = [
        'project_title', 'project_goal', 'project_created_date', 'project_last_sync',
        'area_of_improvement', 'status_of_project', 'tasks', 'tasks_date', 'tasks_status',
        'sub_task', 'sub_task_status', 'sub_task_date', 'task_start_date', 'task_end_date',
        'sub_task_start_date', 'sub_task_end_date', 'designation', 'project_deleted_flag', 'solution_created_at',
        'task_evidence', 'task_evidence_status', 'project_id', 'task_id', 'sub_task_id',
        'project_created_type', 'task_assigned_to', 'channel', 'parent_channel', 'program_id',
        'program_name', 'project_updated_date', 'createdBy', 'project_title_editable',
        'project_duration', 'program_externalId', 'private_program', 'task_deleted_flag',
        'sub_task_deleted_flag', 'project_terms_and_condition','task_remarks',
        'organisation_name','project_description','project_completed_date','solution_id',
        'project_remarks','project_evidence','organisation_id','user_type', 'certificate_id',
        'certificate_status','certificate_issued_on','certificate_status_customised','certificate_template_url',
        {"type":"long","name":"task_count"},{"type":"long","name":"task_evidence_count"},{"type":"long","name":"project_evidence_count"},{"type":"long","name":"task_sequence"}
    ]
    
    dimensionsArr = list(set(entitiesArr)) + submissionReportColumnNamesArr
    
    try:        
        if NATURE_OF_UPLOAD == "local":
            if solution_id:
                druid_spec = SL_PROJECT_LOCAL_INGESTION_SPEC
                payload = json.loads(druid_spec)
                base_dir_config = payload["spec"]["ioConfig"]["inputSource"]["baseDir"]
                if isinstance(base_dir_config, list):
                    base_dir_config = base_dir_config[0]
                base_dir_path = os.path.dirname(base_dir_config)
                
                # Check for filter in the existing payload first
                file_name_pattern = payload["spec"]["ioConfig"]["inputSource"].get("filter")
                if not file_name_pattern:
                    file_name_pattern = os.path.basename(base_dir_config)
                    
                file_name_final = f"{os.path.splitext(file_name_pattern)[0]}_{solution_id}.json"
                
                payload["spec"]["ioConfig"]["inputSource"]["baseDir"] = base_dir_path
                payload["spec"]["ioConfig"]["inputSource"]["filter"] = file_name_final    
                payload["spec"]["dataSchema"]["dimensionsSpec"]["dimensions"] = dimensionsArr
        else:    
            if solution_id:
                druid_spec = SL_PROJECT_CLOUD_INGESTION_SPEC
                payload = json.loads(druid_spec)
                uris = payload["spec"]["ioConfig"]["inputSource"]["uris"]
                current_cloud = re.split("://+", uris[0])[0]
                uri_path = re.split("://+", uris[0])[1]
                edited_uri = re.split(".json", uri_path)[0]
                payload["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{edited_uri}_{solution_id}.json"
                payload['spec']['ioConfig'].update({"appendToExisting":True})

        datasource = payload["spec"]["dataSchema"]["dataSource"]
        headers = {'Content-Type': 'application/json'}
        druid_batch_end_point = DRUID_BATCH_URL   
        start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(payload), headers=headers)
        
        if start_supervisor.status_code == 200:
            if success_logger:
                success_logger.info(f"Started the batch ingestion task successfully for the datasource {datasource}")
        else:
            if error_logger:
                error_logger.error(f"Failed to start batch ingestion task {datasource}. Status: {start_supervisor.status_code}")
                error_logger.error(start_supervisor.text)
    
    except Exception as e:
        if error_logger:
            error_logger.error(f"Exception during Druid ingestion trigger: {e}")


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
class ProjectPipeline:
    def __init__(self):
        self.config = ConfigManager()
        self.spark = init_spark_session("projects_optimized_project_batch")
        self.utils = Utils()
        self.ingestion: IngestionManager = IngestionManager(self.config)
        self.success_logger, self.error_logger = self.config.get_logger()
        
        # Cloud setup
        self.cloud_init = self._initialize_multi_cloud()

    def process_solution(self, solution_id: str):
        if self.success_logger:
            self.success_logger.info(f"***** Spark Job Started for Solution ID: {solution_id} *****")

        try:
            solution_doc = self.ingestion.fetch_solution_details(solution_id)
            if not solution_doc:
                if self.success_logger: self.success_logger.warning(f"No solution found for {solution_id}")
                return

            solution_doc['createdAt'] = self.utils.to_utc(solution_doc['createdAt'])
            solution_list = [solution_doc]
            solution_df = self.spark.createDataFrame(solution_list, get_solution_schema())
            solution_df = solution_df.withColumnRenamed("_id", "solution_id") \
                                     .withColumnRenamed("createdAt", "solution_created_at")
            
            self.success_logger.info("The solution is created at: {solution_created_at}".format(solution_created_at=solution_doc['createdAt']))

            projects_list = self.ingestion.fetch_projects_for_solution(solution_id)

            prj_rdd = self.spark.sparkContext.parallelize(projects_list)
            processed_rdd = prj_rdd.mapPartitions(process_project_partition)

            if self.success_logger:
                self.success_logger.info("Completed mapPartitions")

            projects_df = self.spark.createDataFrame(processed_rdd, get_projects_schema())

            final_df = transform_projects_df(projects_df, self.utils)

            final_df = final_df.join(F.broadcast(solution_df), "solution_id", "left")

            if self.success_logger:
                self.success_logger.info("Final Dataframe is ready to be written to json")

            os.makedirs(SL_PROJECT_OUTPUT_DIR, exist_ok=True)

            temp_output_dir = os.path.join(SL_PROJECT_OUTPUT_DIR, f"temp_{solution_id}")

            final_df.coalesce(1).write.format("json").option("ignoreNullFields", "false").mode("overwrite").save(temp_output_dir)
            
            output_file_name = f"sl_project_{solution_id}.json"
            for filename in os.listdir(temp_output_dir):
                if filename.endswith(".json"):
                    src = os.path.join(temp_output_dir, filename)
                    dst = os.path.join(SL_PROJECT_OUTPUT_DIR, output_file_name)
                    if os.path.exists(dst):
                        os.remove(dst)
                    os.rename(src, dst)
                    if self.success_logger: self.success_logger.info(f"Renamed output file to: {output_file_name}")
                    break
            
            shutil.rmtree(temp_output_dir)

            if NATURE_OF_UPLOAD == "cloud":
                self.upload_file_to_cloud(SL_PROJECT_OUTPUT_DIR, output_file_name, solution_id)
                self.utils.delete_local_output_file(solution_id)

            trigger_druid_ingestion(solution_id, self.config)

            if self.success_logger:
                self.success_logger.info(f"Successfully processed solution {solution_id}")
            
            return len(projects_list)
        
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Error processing solution {solution_id}", exc_info=True)
            raise e

    def upload_file_to_cloud(self, local_path, file_name, solution_id):
        """Uploads the generated file to cloud storage using settings from config spec."""
        try:
            blob_path = SL_PROJECT_BLOB_PATH

            if self.success_logger:
                self.success_logger.info(f"Uploading {file_name} to cloud path: {blob_path}")
            self.cloud_init.upload_to_cloud(blob_Path=blob_path, local_Path=local_path, file_Name=file_name)
            
        except Exception as e:
            if self.error_logger:
                self.error_logger.error(f"Failed to upload {file_name} for {solution_id}: {e}")

    def _initialize_multi_cloud(self):
        """Initializes the MultiCloud instance, ensuring the module path is in sys.path."""
        cloud_path = CLOUD_MODULE_PATH
        if self.success_logger:
            self.success_logger.info(f"Cloud path: {cloud_path}")
        if cloud_path and cloud_path not in sys.path:
            sys.path.append(cloud_path)
        
        try:
            from cloud import MultiCloud
            return MultiCloud()
        except ImportError as e:
            if self.error_logger:
                self.error_logger.error(f"Failed to import MultiCloud from {cloud_path}: {e}")
            return None

    def run(self):
        solution_ids = self.ingestion.get_all_solution_ids()
        if self.success_logger:
            self.success_logger.info(f"Total Solution IDs gathered: {len(solution_ids)}")
        
        for solution_id in solution_ids:
            if self.success_logger:
                self.success_logger.info(f"Processing Solution ID: {solution_id}")
            try:
                self.process_solution(solution_id)
            except Exception as e:
                if self.error_logger:
                    self.error_logger.error(f"Failed to process {solution_id}: {e}")

def main():
    success_logger = None
    error_logger = None
    try:
        success_logger, error_logger = config_manager.get_logger()
    except:
        pass

    if args.is_first_time:
        if success_logger:
            success_logger.info("First time running the Ingestion job")
        pipeline = ProjectPipeline()
        pipeline.utils.delete_entire_datasource("sl-project")
        pipeline.run()
    else:
        if success_logger:
            success_logger.info("Not first time running the Ingestion job")
        pipeline = ProjectPipeline()
        updated_solution_ids:dict= pipeline.ingestion.fetch_recent_updated_solution_ids()
        if len(updated_solution_ids) == 0:
            if success_logger:
                success_logger.info("No recent updates found")
            return
        if success_logger:
            success_logger.info(f"Recently updated solution IDs: {updated_solution_ids}")
        for log_id, sol_ids in updated_solution_ids.items():
            if success_logger:
                success_logger.info(f"Processing Log ID: {log_id}")
            pipeline.ingestion.update_program_activity_status_start(log_id)
            total_processed_projects = 0
            
            for sol_id in sol_ids:
                if success_logger:
                    success_logger.info(f"Processing Solution ID: {sol_id}")
                sol_details = pipeline.ingestion.fetch_solution_details(sol_id)
                if sol_details:
                    pipeline.utils.segement_deletion(sol_details['createdAt'], "sl-project")
                    count = pipeline.process_solution(sol_id)
                    if count is None: count = 0
                    total_processed_projects += count
                    if success_logger:
                        success_logger.info(f"Processed Solution ID: {sol_id}, count: {count}")
                    pipeline.ingestion.update_program_activity_progress(log_id, sol_id, count)
                else:
                    if error_logger:
                        error_logger.error(f"Solution details not found for {sol_id}, skipping.")

            pipeline.ingestion.update_program_activity_completed(log_id, total_processed_projects)

if __name__ == '__main__':
    main()
