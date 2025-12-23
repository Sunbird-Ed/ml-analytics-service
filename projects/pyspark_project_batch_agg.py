# -------------------------------------------------------------------------
# Name : pyspark_project_batch_agg.py
# Author : Vivek
# Description : Generates aggregated metrics for projects using MongoDB
#               aggregation pipelines directly via Spark MongoDB Connector
# -------------------------------------------------------------------------

import os, re
import argparse
import logging
import json, sys
import requests
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from configparser import ConfigParser, ExtendedInterpolation
from pyspark.sql import SparkSession
from datetime import datetime
from bson.objectid import ObjectId
# from cloud import MultiCloud

# Configuration setup
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

# Logging setup
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successLogger.propagate = False  # Prevent duplicate logs
# Use only TimedRotatingFileHandler for automatic rotation
successHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_success'), 
    when="w0", 
    backupCount=4
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorLogger.propagate = False  # Prevent duplicate logs
# Use only TimedRotatingFileHandler for automatic rotation
errorHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'project_error'), 
    when="w0", 
    backupCount=4
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)

# Argument parser
details = argparse.ArgumentParser(description='Pass the ProgramID')
details.add_argument('--program_id', metavar='--program_id', type=str, help='Program IDs', required=False)
args = details.parse_args()
program_unique_id = None
if args.program_id:
    program_Id = args.program_id
    program_unique_id = ObjectId(program_Id)

successLogger.debug(f" ")
successLogger.info(f"Starting aggregation for program_id: {program_unique_id}")

# Initialize Spark Session with MongoDB connector
spark = SparkSession.builder \
    .appName("projects-aggregations") \
    .config("spark.driver.memory", "50g") \
    .config("spark.executor.memory", "100g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "32g") \
    .config("spark.eventLog.enabled", True) \
    .config("spark.mongodb.input.uri", config.get('MONGO', 'url')) \
    .config("spark.mongodb.input.database", config.get('MONGO', 'database_name')) \
    .config("spark.mongodb.input.collection", config.get('MONGO', 'projects_collection')) \
    .getOrCreate()

# ===============================================================
# PIPELINE 1: ml_projects_distinctCount (Solution Level)
# ===============================================================
successLogger.info(f"Building solution-level aggregation pipeline")

# Build match stage based on program_id
if program_unique_id:
    match_stage = {
        "$match": {
            "$and": [
                {"programId": {"$oid": str(program_unique_id)}},
                {"isAPrivateProgram": False},
                {"isDeleted": False}
            ]
        }
    }
else:
    match_stage = {
        "$match": {
            "$and": [
                {"isAPrivateProgram": False},
                {"isDeleted": False}
            ]
        }
    }

# Define the detailed aggregation pipeline
pipeline_detailed = [
    match_stage,
    {
        "$unwind": {
            "path": "$userProfile.userLocations",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$addFields": {
            "orgFiltered": {
                "$filter": {
                    "input": {"$ifNull": ["$userProfile.organisations", []]},
                    "as": "org",
                    "cond": {"$ne": ["$$org.isSchool", True]}
                }
            }
        }
    },
    {
        "$unwind": {
            "path": "$orgFiltered",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$addFields": {
            "project_id": {"$toString": "$_id"},
            "program_id": {"$toString": "$programId"},
            "program_name": "$programInformation.name",
            "solution_id": {"$toString": "$solutionInformation._id"},
            "project_title": "$title",
            "status_of_project": "$status",
            "createdBy": "$userId",
            "project_created_type": {
                "$cond": {
                    "if": {"$ne": ["$projectTemplateId", None]},
                    "then": "project imported from library",
                    "else": "user created project"
                }
            },
            "private_program": {
                "$cond": {
                    "if": {"$eq": ["$isAPrivateProgram", True]},
                    "then": "true",
                    "else": "false"
                }
            },
            "parent_channel": "SHIKSHALOKAM",
            "certificate_status_customised": {
                "$cond": {
                    "if": {
                        "$and": [
                            {"$ne": ["$certificate.eligible", None]},
                            {"$eq": ["$certificate.eligible", True]},
                            {"$ne": ["$certificate.osid", None]}
                        ]
                    },
                    "then": "Issued",
                    "else": ""
                }
            },
            "project_evidence_status": {
                "$cond": {
                    "if": {"$gte": [{"$size": {"$ifNull": ["$attachments", []]}}, 1]},
                    "then": True,
                    "else": False
                }
            },
            "task_evidence_status": {
                "$cond": {
                    "if": {
                        "$gt": [
                            {
                                "$size": {
                                    "$filter": {
                                        "input": {"$ifNull": ["$tasks", []]},
                                        "as": "task",
                                        "cond": {"$gt": [{"$size": {"$ifNull": ["$$task.attachments", []]}}, 0]}
                                    }
                                }
                            },
                            0
                        ]
                    },
                    "then": True,
                    "else": False
                }
            },
            "state_name": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "state"]},
                    "then": "$userProfile.userLocations.name",
                    "else": None
                }
            },
            "state_externalId": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "state"]},
                    "then": "$userProfile.userLocations.id",
                    "else": None
                }
            },
            "district_name": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "district"]},
                    "then": "$userProfile.userLocations.name",
                    "else": None
                }
            },
            "district_externalId": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "district"]},
                    "then": "$userProfile.userLocations.id",
                    "else": None
                }
            },
            "block_name": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "block"]},
                    "then": "$userProfile.userLocations.name",
                    "else": None
                }
            },
            "block_externalId": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "block"]},
                    "then": "$userProfile.userLocations.id",
                    "else": None
                }
            },
            "organisation_id": "$orgFiltered.organisationId",
            "organisation_name": "$orgFiltered.orgName"
        }
    },
    {
        "$addFields": {
            "evidence_status": {
                "$or": ["$project_evidence_status", "$task_evidence_status"]
            }
        }
    },
    {
        "$group": {
            "_id": "$project_id",
            "program_name": {"$first": "$program_name"},
            "program_id": {"$first": "$program_id"},
            "project_title": {"$first": "$project_title"},
            "solution_id": {"$first": "$solution_id"},
            "status_of_project": {"$first": "$status_of_project"},
            "createdBy": {"$first": "$createdBy"},
            "project_created_type": {"$first": "$project_created_type"},
            "private_program": {"$first": "$private_program"},
            "parent_channel": {"$first": "$parent_channel"},
            "certificate_status_customised": {"$first": "$certificate_status_customised"},
            "evidence_status": {"$first": "$evidence_status"},
            "state_name": {"$max": "$state_name"},
            "state_externalId": {"$max": "$state_externalId"},
            "district_name": {"$max": "$district_name"},
            "district_externalId": {"$max": "$district_externalId"},
            "block_name": {"$max": "$block_name"},
            "block_externalId": {"$max": "$block_externalId"},
            "organisation_id": {"$first": "$organisation_id"},
            "organisation_name": {"$first": "$organisation_name"}
        }
    },
    {
        "$group": {
            "_id": {
                "program_name": "$program_name",
                "program_id": "$program_id",
                "project_title": "$project_title",
                "solution_id": "$solution_id",
                "status_of_project": "$status_of_project",
                "state_name": "$state_name",
                "state_externalId": "$state_externalId",
                "district_name": "$district_name",
                "district_externalId": "$district_externalId",
                "block_name": "$block_name",
                "block_externalId": "$block_externalId",
                "organisation_name": "$organisation_name",
                "organisation_id": "$organisation_id",
                "private_program": "$private_program",
                "project_created_type": "$project_created_type",
                "parent_channel": "$parent_channel"
            },
            "no_of_certificate_issued": {
                "$sum": {
                    "$cond": {
                        "if": {"$eq": ["$certificate_status_customised", "Issued"]},
                        "then": 1,
                        "else": 0
                    }
                }
            },
            "unique_projects": {"$addToSet": "$_id"},
            "unique_solution": {"$addToSet": "$solution_id"},
            "unique_users": {"$addToSet": "$createdBy"},
            "no_of_imp_with_evidence": {
                "$sum": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": ["$evidence_status", True]},
                                {"$eq": ["$status_of_project", "submitted"]}
                            ]
                        },
                        "then": 1,
                        "else": 0
                    }
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "program_name": "$_id.program_name",
            "program_id": "$_id.program_id",
            "project_title": "$_id.project_title",
            "solution_id": "$_id.solution_id",
            "status_of_project": "$_id.status_of_project",
            "state_name": "$_id.state_name",
            "state_externalId": "$_id.state_externalId",
            "district_name": "$_id.district_name",
            "district_externalId": "$_id.district_externalId",
            "block_name": "$_id.block_name",
            "block_externalId": "$_id.block_externalId",
            "organisation_name": "$_id.organisation_name",
            "organisation_id": "$_id.organisation_id",
            "private_program": "$_id.private_program",
            "project_created_type": "$_id.project_created_type",
            "parent_channel": "$_id.parent_channel",
            "no_of_certificate_issued": 1,
            "unique_projects": {"$size": "$unique_projects"},
            "unique_solution": {"$size": "$unique_solution"},
            "unique_users": {"$size": "$unique_users"},
            "no_of_imp_with_evidence": 1
        }
    },
    {
        "$sort": {
            "program_name": 1,
            "project_title": 1,
            "state_name": 1
        }
    }
]

# Convert pipeline to JSON string for Spark MongoDB connector
pipeline_json_detailed = json.dumps(pipeline_detailed)

successLogger.info(f"Executing solution-level aggregation pipeline")

# Read from MongoDB using pipeline
try:
    projects_detailed_df = spark.read \
        .format("mongo") \
        .option("uri", config.get('MONGO', 'url')) \
        .option("database", config.get('MONGO', 'database_name')) \
        .option("collection", config.get('MONGO', 'projects_collection')) \
        .option("pipeline", pipeline_json_detailed) \
        .load()
    
    # Add timestamp
    from pyspark.sql.functions import current_timestamp
    projects_detailed_df = projects_detailed_df.withColumn("time_stamp", current_timestamp())
    
    # Select columns in the specified order
    projects_detailed_df = projects_detailed_df.select(
        "program_name",
        "program_id",
        "project_title",
        "solution_id",
        "status_of_project",
        "state_name",
        "state_externalId",
        "district_name",
        "district_externalId",
        "block_name",
        "block_externalId",
        "organisation_name",
        "organisation_id",
        "private_program",
        "project_created_type",
        "parent_channel",
        "no_of_certificate_issued",
        "unique_projects",
        "unique_solution",
        "unique_users",
        "no_of_imp_with_evidence",
        "time_stamp"
    )
    
    successLogger.info(f"Aggregation JSON count for ml_projects_distinctCount: {projects_detailed_df.count()}")
    
    # Save to JSON
    output_path_distinct = config.get("OUTPUT_DIR", "projects_distinctCount")
    projects_detailed_df.coalesce(1).write.format("json").mode("overwrite").save(
        output_path_distinct + "/"
    )
    
    successLogger.info(f"Solution-level aggregation completed")
    
    # Rename files
    for filename in os.listdir(output_path_distinct + "/"):
        if filename.endswith(".json"):
            if program_unique_id:
                new_name = f"ml_projects_distinctCount_{program_unique_id}.json"
            else:
                new_name = "ml_projects_distinctCount.json"
            os.rename(
                output_path_distinct + "/" + filename,
                output_path_distinct + "/" + new_name
            )
            successLogger.info(f"Renamed solution-level file: {new_name}")
    
except Exception as e:
    errorLogger.error(f"Error in solution-level aggregation: {str(e)}")
    raise

# ===============================================================
# PIPELINE 2: ml_projects_distinctCount_prgmlevel (Program Level)
# ===============================================================
successLogger.info(f"Building program-level aggregation pipeline")

# Define the program-level aggregation pipeline
pipeline_prgmlevel = [
    match_stage,
    {
        "$unwind": {
            "path": "$userProfile.userLocations",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$addFields": {
            "project_id": {"$toString": "$_id"},
            "program_id": {"$toString": "$programId"},
            "program_name": "$programInformation.name",
            "status_of_project": "$status",
            "createdBy": "$userId",
            "project_created_type": {
                "$cond": {
                    "if": {"$ne": ["$projectTemplateId", None]},
                    "then": "project imported from library",
                    "else": "user created project"
                }
            },
            "private_program": {
                "$cond": {
                    "if": {"$eq": ["$isAPrivateProgram", True]},
                    "then": "true",
                    "else": "false"
                }
            },
            "parent_channel": "SHIKSHALOKAM",
            "certificate_status_customised": {
                "$cond": {
                    "if": {
                        "$and": [
                            {"$ne": ["$certificate.eligible", None]},
                            {"$eq": ["$certificate.eligible", True]},
                            {"$ne": ["$certificate.osid", None]}
                        ]
                    },
                    "then": "Issued",
                    "else": ""
                }
            },
            "project_evidence_status": {
                "$cond": {
                    "if": {"$gte": [{"$size": {"$ifNull": ["$attachments", []]}}, 1]},
                    "then": True,
                    "else": False
                }
            },
            "task_evidence_status": {
                "$cond": {
                    "if": {
                        "$gt": [
                            {
                                "$size": {
                                    "$filter": {
                                        "input": {"$ifNull": ["$tasks", []]},
                                        "as": "task",
                                        "cond": {"$gt": [{"$size": {"$ifNull": ["$$task.attachments", []]}}, 0]}
                                    }
                                }
                            },
                            0
                        ]
                    },
                    "then": True,
                    "else": False
                }
            },
            "state_name": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "state"]},
                    "then": "$userProfile.userLocations.name",
                    "else": None
                }
            },
            "state_externalId": {
                "$cond": {
                    "if": {"$eq": ["$userProfile.userLocations.type", "state"]},
                    "then": "$userProfile.userLocations.id",
                    "else": None
                }
            }
        }
    },
    {
        "$addFields": {
            "evidence_status": {
                "$or": ["$project_evidence_status", "$task_evidence_status"]
            }
        }
    },
    {
        "$group": {
            "_id": "$project_id",
            "program_name": {"$first": "$program_name"},
            "program_id": {"$first": "$program_id"},
            "status_of_project": {"$first": "$status_of_project"},
            "createdBy": {"$first": "$createdBy"},
            "project_created_type": {"$first": "$project_created_type"},
            "private_program": {"$first": "$private_program"},
            "parent_channel": {"$first": "$parent_channel"},
            "certificate_status_customised": {"$first": "$certificate_status_customised"},
            "evidence_status": {"$first": "$evidence_status"},
            "state_name": {"$max": "$state_name"},
            "state_externalId": {"$max": "$state_externalId"}
        }
    },
    {
        "$group": {
            "_id": {
                "program_name": "$program_name",
                "program_id": "$program_id",
                "status_of_project": "$status_of_project",
                "state_name": "$state_name",
                "state_externalId": "$state_externalId",
                "private_program": "$private_program",
                "project_created_type": "$project_created_type",
                "parent_channel": "$parent_channel"
            },
            "no_of_certificate_issued": {
                "$sum": {
                    "$cond": {
                        "if": {"$eq": ["$certificate_status_customised", "Issued"]},
                        "then": 1,
                        "else": 0
                    }
                }
            },
            "unique_projects": {"$addToSet": "$_id"},
            "unique_users": {"$addToSet": "$createdBy"},
            "no_of_imp_with_evidence": {
                "$sum": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$eq": ["$evidence_status", True]},
                                {"$eq": ["$status_of_project", "submitted"]}
                            ]
                        },
                        "then": 1,
                        "else": 0
                    }
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "program_name": "$_id.program_name",
            "program_id": "$_id.program_id",
            "status_of_project": "$_id.status_of_project",
            "state_name": "$_id.state_name",
            "state_externalId": "$_id.state_externalId",
            "private_program": "$_id.private_program",
            "project_created_type": "$_id.project_created_type",
            "parent_channel": "$_id.parent_channel",
            "no_of_certificate_issued": 1,
            "unique_projects": {"$size": "$unique_projects"},
            "unique_users": {"$size": "$unique_users"},
            "no_of_imp_with_evidence": 1
        }
    },
    {
        "$sort": {
            "program_name": 1,
            "status_of_project": 1,
            "state_name": 1
        }
    }
]

# Convert pipeline to JSON string
pipeline_json_prgmlevel = json.dumps(pipeline_prgmlevel)

successLogger.info(f"Executing program-level aggregation pipeline")

# Read from MongoDB using pipeline
try:
    projects_prgmlevel_df = spark.read \
        .format("mongo") \
        .option("uri", config.get('MONGO', 'url')) \
        .option("database", config.get('MONGO', 'database_name')) \
        .option("collection", config.get('MONGO', 'projects_collection')) \
        .option("pipeline", pipeline_json_prgmlevel) \
        .load()
    
    # Add timestamp
    projects_prgmlevel_df = projects_prgmlevel_df.withColumn("time_stamp", current_timestamp())
    
    # Select columns in the specified order
    projects_prgmlevel_df = projects_prgmlevel_df.select(
        "program_name",
        "program_id",
        "status_of_project",
        "state_name",
        "state_externalId",
        "private_program",
        "project_created_type",
        "parent_channel",
        "no_of_certificate_issued",
        "unique_projects",
        "unique_users",
        "no_of_imp_with_evidence",
        "time_stamp"
    )
    
    successLogger.info(f"Aggregation JSON count for ml_projects_distinctCount_prgmlevel: {projects_prgmlevel_df.count()}")
    
    # Save to JSON
    output_path_prglevel = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")
    projects_prgmlevel_df.coalesce(1).write.format("json").mode("overwrite").save(
        output_path_prglevel + "/"
    )

    successLogger.info(f"Program-level aggregation completed")

    # Rename files
    for filename in os.listdir(output_path_prglevel + "/"):
        if filename.endswith(".json"):
            if program_unique_id:
                new_name = f"ml_projects_distinctCount_prgmlevel_{program_unique_id}.json"
            else:
                new_name = "ml_projects_distinctCount_prgmlevel.json"
            os.rename(
                output_path_prglevel + "/" + filename,
                output_path_prglevel + "/" + new_name
            )
            successLogger.info(f"Renamed program-level file: {new_name}")
    
except Exception as e:
    errorLogger.error(f"Error in program-level aggregation: {str(e)}")
    raise

# ===============================================================
# CLOUD FILES UPLOAD SIMULATED
# ===============================================================
successLogger.info(f"Local Cloud upload files start time: {datetime.now()}")

import shutil

local_distinctCnt_path = config.get("OUTPUT_DIR", "projects_distinctCount")
local_distinctCnt_prgmlevel_path = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")

# Move solution-level files
# for files in os.listdir(local_distinctCnt_path):
#     if "ml_projects_distinctCount.json" in files or \
#        (program_unique_id and f"ml_projects_distinctCount_{program_unique_id}.json" in files):
#         shutil.move(
#             os.path.join(local_distinctCnt_path, files),
#             os.path.join("/home/user2/Documents/sunbird-ml-analytics-service/sunbird-ml-analytics-service/local_s3/projects_distinctCount", files)
#         )
#         successLogger.info(f"Moved file: {files}")

# # Move program-level files
# for files in os.listdir(local_distinctCnt_prgmlevel_path):
#     if "ml_projects_distinctCount_prgmlevel.json" in files or \
#        (program_unique_id and f"ml_projects_distinctCount_prgmlevel_{program_unique_id}.json" in files):
#         shutil.move(
#             os.path.join(local_distinctCnt_prgmlevel_path, files),
#             os.path.join("/home/user2/Documents/sunbird-ml-analytics-service/sunbird-ml-analytics-service/local_s3/projects_distinctCount_prgmlevel", files)
#         )
#         successLogger.info(f"Moved file: {files}")

# successLogger.info(f"Local Cloud upload files end time: {datetime.now()}")

# ===============================================================
# LOCAL CLEANUP, NOT NEEDED. AS FILES MOVED TO SIMULATED CLOUD
# ===============================================================
# successLogger.info(f"Local files cleanup started")

# if program_unique_id :
#  os.remove(config.get("OUTPUT_DIR", "projects_distinctCount") + f"/ml_projects_distinctCount_{program_unique_id}.json")
#  os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + f"/ml_projects_distinctCount_prgmlevel_{program_unique_id}.json")
# else :
#  os.remove(config.get("OUTPUT_DIR", "projects_distinctCount") + "/ml_projects_distinctCount.json")
#  os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json")

# successLogger.info(f"Local files cleanup completed")

# ===============================================================
# DRUID INGESTION TRIGGER
# ===============================================================
successLogger.info(f"Druid Ingestion Trigger started")

druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

# solution-level druid ingestion
ml_distinctCnt_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_projects_status_spec_agg"))
ml_distinctCnt_projects_datasource = ml_distinctCnt_projects_spec["spec"]["dataSchema"]["dataSource"]
if program_unique_id:
    ml_distinctCnt_projects_spec["spec"]["ioConfig"]["inputSource"] = {
        "type": "local",
            "baseDir": "/Users/user/Documents/Diksha/dev/ml-analytics-service/local_S3/projects_distinctCount/",
        "filter": f"ml_projects_distinctCount_{program_unique_id}.json"
    }
    ml_distinctCnt_projects_spec['spec']['ioConfig'].update({"appendToExisting": True})
distinctCnt_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_projects_spec), headers=headers)
if distinctCnt_projects_start_supervisor.status_code == 200:
    successLogger.info("Started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_projects_datasource)
else:
    errorLogger.error("Failed to start batch ingestion task of ml-project-status " + str(distinctCnt_projects_start_supervisor.status_code))
    errorLogger.error(distinctCnt_projects_start_supervisor.text)

# program-level druid ingestion
ml_distinctCnt_prgmlevel_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec_agg"))
ml_distinctCnt_prgmlevel_projects_datasource = ml_distinctCnt_prgmlevel_projects_spec["spec"]["dataSchema"]["dataSource"]
if program_unique_id:
    ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"] = {
        "type": "local",
        "baseDir": "/home/user2/Documents/sunbird-ml-analytics-service/sunbird-ml-analytics-service/local_s3/projects_distinctCount_prgmlevel/",
        "filter": f"ml_projects_distinctCount_prgmlevel_{program_unique_id}.json"
    }
    ml_distinctCnt_prgmlevel_projects_spec['spec']['ioConfig'].update({"appendToExisting": True})
distinctCnt_prgmlevel_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_prgmlevel_projects_spec), headers=headers)
if distinctCnt_prgmlevel_projects_start_supervisor.status_code == 200:
    successLogger.info("Started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_prgmlevel_projects_datasource)
else:
    errorLogger.error("Failed to start batch ingestion task of ml-project-programLevel-status " + str(distinctCnt_prgmlevel_projects_start_supervisor.status_code))
    errorLogger.error(distinctCnt_prgmlevel_projects_start_supervisor.text)

successLogger.info(f"Druid Ingestion Trigger completed")

# ===============================================================
# UPLOAD FILES TO CLOUD STORAGE
# ===============================================================
successLogger.info(f"Cloud upload files start time: {datetime.now()}")

# Cloud setup
sys.path.append(config.get("COMMON", "cloud_module_path"))
cloud_init = MultiCloud()
# solution-level distinct count file path configs
local_distinctCnt_path = config.get("OUTPUT_DIR", "projects_distinctCount")
blob_distinctCnt_path = config.get("COMMON", "projects_distinctCnt_blob_path")
# program-level distinct count file path configs
local_distinctCnt_prgmlevel_path = config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel")
blob_distinctCnt_prgmlevel_path = config.get("COMMON", "projects_distinctCnt_prgmlevel_blob_path")

# solution-level cloud upload
for files in os.listdir(local_distinctCnt_path):
    if "ml_projects_distinctCount.json" in files or f"ml_projects_distinctCount_{program_unique_id}.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_distinctCnt_path, local_Path = local_distinctCnt_path, file_Name = files)

# program-level cloud upload
for files in os.listdir(local_distinctCnt_prgmlevel_path):
    if "ml_projects_distinctCount_prgmlevel.json" in files or f"ml_projects_distinctCount_prgmlevel_{program_unique_id}.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_distinctCnt_prgmlevel_path, local_Path = local_distinctCnt_prgmlevel_path, file_Name = files)

successLogger.info(f"Cloud upload files end time: {datetime.now()}")

# ===============================================================
# LOCAL FILES CLEANUP
# ===============================================================
successLogger.info(f"Local files cleanup started")

if program_unique_id :
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount") + f"/ml_projects_distinctCount_{program_unique_id}.json")
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + f"/ml_projects_distinctCount_prgmlevel_{program_unique_id}.json")
else :
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount") + "/ml_projects_distinctCount.json")
 os.remove(config.get("OUTPUT_DIR", "projects_distinctCount_prgmlevel") + "/ml_projects_distinctCount_prgmlevel.json")

successLogger.info(f"Local files cleanup completed")

# ===============================================================
# DRUID INGESTION TRIGGER
# ===============================================================
successLogger.info(f"Druid Ingestion Trigger started")

druid_batch_end_point = config.get("DRUID", "batch_url")
headers = {'Content-Type': 'application/json'}

# solution-level druid ingestion
ml_distinctCnt_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_projects_status_spec"))
ml_distinctCnt_projects_datasource = ml_distinctCnt_projects_spec["spec"]["dataSchema"]["dataSource"]
if program_unique_id :
    current_cloud = re.split("://+", ml_distinctCnt_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[0]
    uri = re.split("://+", ml_distinctCnt_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[1]
    edited_uri = re.split(".json", uri)[0]
    ml_distinctCnt_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0]  = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
    ml_distinctCnt_projects_spec['spec']['ioConfig'].update({"appendToExisting":True})
distinctCnt_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_projects_spec), headers=headers)
if distinctCnt_projects_start_supervisor.status_code == 200:
    successLogger.info("Started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_projects_datasource)
else:
    errorLogger.error("Failed to start batch ingestion task of ml-project-status " + str(distinctCnt_projects_start_supervisor.status_code))
    errorLogger.error(distinctCnt_projects_start_supervisor.text)

# program-level druid ingestion
ml_distinctCnt_prgmlevel_projects_spec = json.loads(config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec"))
ml_distinctCnt_prgmlevel_projects_datasource = ml_distinctCnt_prgmlevel_projects_spec["spec"]["dataSchema"]["dataSource"]
if program_unique_id:
    current_cloud = re.split("://+", ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[0]
    uri = re.split("://+", ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0])[1]
    edited_uri = re.split(".json", uri)[0]
    ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{edited_uri}_{program_unique_id}.json"
    ml_distinctCnt_prgmlevel_projects_spec["spec"]["ioConfig"].update({"appendToExisting":True})
distinctCnt_prgmlevel_projects_start_supervisor = requests.post(druid_batch_end_point, data=json.dumps(ml_distinctCnt_prgmlevel_projects_spec), headers=headers)
if distinctCnt_prgmlevel_projects_start_supervisor.status_code == 200:
    successLogger.info("Started the batch ingestion task sucessfully for the datasource " + ml_distinctCnt_prgmlevel_projects_datasource)
else:
    errorLogger.error("Failed to start batch ingestion task of ml-project-programLevel-status " + str(distinctCnt_prgmlevel_projects_start_supervisor.status_code))
    errorLogger.error(distinctCnt_prgmlevel_projects_start_supervisor.text)

successLogger.info(f"Druid Ingestion Trigger completed")

successLogger.info(f"Successfully completed aggregation for program_id: {program_unique_id}")

spark.stop()