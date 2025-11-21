# -----------------------------------------------------------------
# Name : pyspark_project_batch.py
# Author : Shakthiehswari, Ashwini, Vivek
# Description : Extracts the Status of the Project submissions 
#  either Started / In-Progress / Submitted along with the users 
#  entity information and extended to include additional reports 
#  for unique leaders/schools and program-wise breakdown
# -----------------------------------------------------------------

import json, sys, time, csv
from configparser import ConfigParser, ExtendedInterpolation
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
import glob, requests

# ========================== CONFIGURATION ==========================
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
sys.path.append(config.get("COMMON", "cloud_module_path"))

from cloud import MultiCloud
cloud_init = MultiCloud()

# ========================== LOGGING SETUP ==========================
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successLogger.propagate = False
successHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'nvsk_project_success'),
    when="w0",
    backupCount=4
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorLogger.propagate = False
errorHandler = TimedRotatingFileHandler(
    config.get('LOGS', 'nvsk_project_error'),
    when="w0",
    backupCount=4
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)

successLogger.info("NVSK processing started.")

# ========================== HELPER FUNCTIONS ==========================
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
    def melt(df: DataFrame, id_vars: Iterable[str], value_vars: Iterable[str],
             var_name: str = "variable", value_name: str = "value") -> DataFrame:

        _vars_and_vals = array(*(
            struct(lit(c).alias(var_name), col(c).alias(value_name))
            for c in value_vars))

        # Add to the DataFrame and explode
        _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

        cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
        return _tmp.select(*cols)
except Exception as e:
    errorLogger.error(e, exc_info=True)

# ========================== SEARCH ENTITIES METHOD ==========================
def searchEntities(url,ids_list):
    try:
        returnData = {}
        apiSuccessFlag = False
        headers = {
          'Authorization': config.get('API_HEADERS', 'authorization_access_token'),
          'content-Type': 'application/json'
        }
        # prepare api body 
        payload = json.dumps({
          "request": {
            "filters": {
              "id": ids_list
            }
          }
        })
        response = requests.request("POST", url, headers=headers, data=payload)
        delta_ids = []
        entity_name_mapping = {}
        
        if response.status_code == 200:
            # convert the response to dictionary 
            response = response.json()

            data = response['result']['response']
            
            entity_name_mapping = {}
            # prepare entity name - id mapping
            for index in data:
                entity_name_mapping[index['id']] = index['name']

            # fetch the ids from the mapping 
            ids_from_api = list(entity_name_mapping.keys())

            # check with the input data to make sure there are no missing data from loc search 
            delta_ids = list(set(ids_list) - set(ids_from_api))
            apiSuccessFlag = True
        else :
            delta_ids = ids_list
        returnData['mapping'] = entity_name_mapping
        returnData['apiSuccessFlag'] = apiSuccessFlag
        returnData['delta'] = delta_ids
        return returnData
        
    except Exception as e:
       errorLogger.error(e,exc_info=True)

# ========================== SPARK & MONGO SETUP ==========================
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

successLogger.info("Querying MongoDB for projects collection")
projects_cursorMongo = projectsCollec.aggregate(
    [{"$match": {"isAPrivateProgram": False, "isDeleted": False,
                 "programInformation.name": {"$regex": "^((?!(?i)(test)).)*$"}}},
     {
         "$project": {
             "_id": {"$toString": "$_id"},
             "status": 1,
             "attachments": 1,
             "tasks": {"attachments": 1, "_id": {"$toString": "$_id"}},
             "userProfile": 1,
             "userRoleInformation": {"district": 1, "state": 1},
             "programInformation": {"name": 1, "externalId": 1, "_id": {"$toString": "$programInformation._id"}},
             "userId": {"$toString": "$userId"},
             "categories": 1,
             "createdAt": 1 
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
                    ]), True)
            ),
            StructField('rootOrgId', StringType(), True),
            StructField('schoolName', StringType(), True),
            StructField('schoolId', StringType(), True)
        ])
    ),
    StructField("userRoleInformation", StructType([
        StructField("district", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("programInformation", StructType([
        StructField("name", StringType(), True),
        StructField("externalId", StringType(), True),
        StructField("_id", StringType(), True)
    ]), True),
    StructField('userId', StringType(), True),
    StructField('categories', ArrayType(
        StructType([
            StructField('name', StringType(), True),
            StructField('externalId', StringType(), True)
        ])
    ), True),
    StructField('createdAt', StringType(), True) 
])

# ========================== DATA TRANSFORMATION ==========================
successLogger.info("Creating DataFrame from MongoDB cursor")
projects_df = spark.createDataFrame(projects_cursorMongo, projects_schema)

projects_df = projects_df.withColumn(
    "project_evidence_status",
    F.when(
        size(F.col("attachments")) >= 1, True
    ).otherwise(False)
)
projects_df = projects_df.withColumn("exploded_tasks", F.explode_outer(F.col("tasks")))

projects_df = projects_df.withColumn(
    "task_evidence_status",
    F.when(
        size(projects_df["exploded_tasks"]["attachments"]) >= 1, True
    ).otherwise(False)
)

projects_df = projects_df.withColumn(
    "evidence_status",
    F.when(
        (projects_df["project_evidence_status"] == False) & (projects_df["task_evidence_status"] == False), False
    ).otherwise(True)
)

projects_df = projects_df.withColumn(
    "exploded_userLocations", F.explode_outer(projects_df["userProfile"]["userLocations"])
)

projects_df = projects_df.withColumn(
    "category",
    F.when(
        size(F.col("categories")) > 0,
        F.concat_ws(", ", F.col("categories").getField("name"))
    ).otherwise("Uncategorized")
)

entities_df = melt(projects_df,
                   id_vars=["_id", "exploded_userLocations.name", "exploded_userLocations.type",
                            "exploded_userLocations.id", "userRoleInformation.district", "userRoleInformation.state"],
                   value_vars=["exploded_userLocations.code"]
                   ).select("_id", "name", "value", "type", "id", "district", "state").dropDuplicates()

projects_df = projects_df.join(entities_df, projects_df["_id"] == entities_df["_id"], how='left') \
    .drop(entities_df["_id"])
projects_df = projects_df.filter(F.col("status") != "null")

entities_df.unpersist()

# ============================================================================
# REPORT 1: District-wise Micro Improvement Projects
# ============================================================================
successLogger.info("Generating Report 1: District-wise Micro Improvement Projects...")

projects_df_final = projects_df.select(
    projects_df["_id"].alias("project_id"),
    projects_df["status"],
    projects_df["evidence_status"],
    projects_df["district"],
    projects_df["state"],
)

# DataFrame for user locations values of State and Districts only 
userLocations_df = melt(projects_df,
                        id_vars=["_id", "exploded_userLocations.name", "exploded_userLocations.type", "exploded_userLocations.id"],
                        value_vars=["exploded_userLocations.code"]
                        ).select("_id", "id", "name", "value", "type").filter(
    (col("type") == "state") | (col("type") == "district")).dropDuplicates()

# Fetch only Latest Data of Locations from the DF 
userLocations_df = userLocations_df.groupBy("id").agg(
    first("_id", ignorenulls=True).alias("projectId"),
    first("name", ignorenulls=True).alias("name"),
    first("value", ignorenulls=True).alias("value"),
    first("type", ignorenulls=True).alias("type")
)

projects_df_final = projects_df_final.dropDuplicates()

district_final_df = projects_df_final.groupBy("state", "district") \
    .agg(countDistinct(F.col("project_id")).alias("Total_Micro_Improvement_Projects"),
         countDistinct(when(F.col("status") == "started", True) \
                       , F.col("project_id")).alias("Total_Micro_Improvement_Started"),
         countDistinct(when(F.col("status") == "inProgress", True), \
                       F.col("project_id")).alias("Total_Micro_Improvement_InProgress"),
         countDistinct(when(F.col("status") == "submitted", True), \
                       F.col("project_id")).alias("Total_Micro_Improvement_Submitted"), \
         countDistinct(when((F.col("evidence_status") == True) & (F.col("status") == "submitted"), True), \
                       F.col("project_id")).alias("Total_Micro_Improvement_Submitted_With_Evidence")).sort("state","district")

# select only  district ids from the Dataframe 
district_to_list = projects_df_final.select("district").rdd.flatMap(lambda x: x).collect()
# select only  state ids from the Dataframe 
state_to_list = projects_df_final.select("state").rdd.flatMap(lambda x: x).collect()

# merge the list of district and state ids , remove the duplicates 
ids_list = list(set(district_to_list)) + list(set(state_to_list))

# remove the None values from the list 
ids_list = [value for value in ids_list if value is not None]

# call function to get the entity from location master 
response = searchEntities(config.get("API_ENDPOINTS", "base_url") + config.get("API_ENDPOINTS", "location_search"),ids_list)

data_tuples = [] #empty List for creating the DF

# if Location search API is success get the mapping details from API
if response['apiSuccessFlag']:
  # Convert dictionary to list of tuples
  data_tuples = list(response['mapping'].items())

# if any delta ids found , fetch the details from DF 
if response['delta']:
      delta_ids_from_response = userLocations_df.filter(col("id").isin(response['delta']))
      for row in delta_ids_from_response.collect() :
          data_tuples.append((row['id'],row['name']))

# TODO : Get the data from DF only if location search API failure case
# data_tuples = []
# for row in userLocations_df.collect():
#     data_tuples.append((row['id'], row['name']))

# Define the schema for State details 
state_schema = StructType([StructField("id", StringType(), True), StructField("state_name", StringType(), True)])

# Define the schema for District details
district_schema = StructType([StructField("id", StringType(), True), StructField("district_name", StringType(), True)])

# Create a DataFrame for State 
state_id_mapping = spark.createDataFrame(data_tuples, schema=state_schema)

# Create a DataFrame for District
district_id_mapping = spark.createDataFrame(data_tuples, schema=district_schema)

# Join to get the State names from State ids 
district_final_df = district_final_df.join(state_id_mapping, district_final_df["state"] == state_id_mapping["id"], "left")
# Join to get the State names from District ids 
district_final_df = district_final_df.join(district_id_mapping, district_final_df["district"] == district_id_mapping["id"], "left")
# Select only relevant fields to prepare the final DF , Sort it wrt state names
final_data_to_csv = district_final_df.select("state_name", "district_name", "Total_Micro_Improvement_Projects","Total_Micro_Improvement_Started", "Total_Micro_Improvement_InProgress", "Total_Micro_Improvement_Submitted", "Total_Micro_Improvement_Submitted_With_Evidence").sort("state_name", "district_name")
# DF To file
local_path = config.get("COMMON", "nvsk_imp_projects_data_local_path")
blob_path = config.get("COMMON", "nvsk_imp_projects_data_blob_path")
final_data_to_csv.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(local_path)
final_data_to_csv.unpersist()

# Renaming a file
path = local_path
extension = 'csv'
os.chdir(path)
result = glob.glob(f'*.{extension}')
os.rename(f'{path}' + f'{result[0]}', f'{path}' + 'data.csv')

# Create JSON
json_keys = ["state_name", "district_name", "Total_Micro_Improvement_Projects", "Total_Micro_Improvement_Started", "Total_Micro_Improvement_InProgress", "Total_Micro_Improvement_Submitted","Total_Micro_Improvement_Submitted_With_Evidence"]
jsonTableData = []
with open(os.path.join(local_path, 'data.csv'), 'r') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)
    for row in csv_reader:
        jsonTableData.append(row)

final_json = {
    'keys': json_keys,
    'tableData': jsonTableData
}

with open(os.path.join(local_path, 'micro_improvement.json'), 'w') as json_file:
    json.dump(final_json, json_file, indent=2)

os.rename(os.path.join(local_path, 'data.csv'), f'{local_path}' + 'micro_improvement.csv')
successLogger.info("Report 1 (District-wise summary) completed.")

# ============================================================================
# REPORT 2: Summary Statistics
# ============================================================================
successLogger.info("Generating Report 2: Summary Statistics...")

unique_leaders = projects_df.select("userId").distinct().count()

unique_schools_df = projects_df.select(
    F.explode_outer(F.col("userProfile.userLocations")).alias("user_location")
).filter(
    (F.col("user_location.type") == "school") & (F.col("user_location.id").isNotNull())
).select(
    F.col("user_location.id").alias("school_id")
).distinct()

unique_schools = unique_schools_df.count()

summary_data = [(unique_leaders, unique_schools)]
summary_schema = StructType([
    StructField("Number of Unique Leaders on the Improvement Journey", IntegerType(), True),
    StructField("Number of Unique Schools on the Improvement Journey", IntegerType(), True)
])

summary_df = spark.createDataFrame(summary_data, summary_schema)

summary_local_path = local_path + "summary/"
if not os.path.exists(summary_local_path):
    os.makedirs(summary_local_path)

summary_df.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(summary_local_path)

os.chdir(summary_local_path)
result = glob.glob(f'*.csv')
os.rename(f'{summary_local_path}{result[0]}', f'{summary_local_path}improvement_journey_summary.csv')

summary_json_keys = [
    "Number of Unique Leaders on the Improvement Journey",
    "Number of Unique Schools on the Improvement Journey"
]
summary_json_data = []

with open(os.path.join(summary_local_path, 'improvement_journey_summary.csv'), 'r') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)
    for row in csv_reader:
        summary_json_data.append(row)

summary_json = {
    'keys': summary_json_keys,
    'tableData': summary_json_data
}

with open(os.path.join(summary_local_path, 'improvement_journey_summary.json'), 'w') as json_file:
    json.dump(summary_json, json_file, indent=2)

successLogger.info("Report 2 (Summary statistics) completed.")

# ============================================================================
# REPORT 3: Program-wise Detailed Report
# ============================================================================
successLogger.info("Generating Report 3: Program-wise Detailed Report...")

try:
    programsCollec = db[config.get('MONGO', 'programs_collection')]
    
    unique_program_ids_list = projects_df.select("programInformation._id") \
        .filter(col("programInformation._id").isNotNull()) \
        .distinct() \
        .rdd.flatMap(lambda x: x).collect()
    
    successLogger.debug(f"Unique Program IDs found: {len(unique_program_ids_list)}")

    object_ids = []
    for pid in unique_program_ids_list:
        try:
            object_ids.append(ObjectId(pid))
        except Exception as e:
            errorLogger.error(f"Could not convert {pid} to ObjectId. Skipping.", exc_info=True)

    if object_ids:
        successLogger.debug("Querying Mongo for Program metadata using _id...")
        program_cursor = programsCollec.find(
            {"_id": {"$in": object_ids}},
            {"_id": 1, "createdAt": 1, "name": 1}
        )
        
        cleaned_program_data = []
        for doc in program_cursor:
            doc['_id'] = str(doc['_id'])
            if 'createdAt' in doc and doc['createdAt']:
                doc['createdAt'] = doc['createdAt'].isoformat() if hasattr(doc['createdAt'], 'isoformat') else str(doc['createdAt'])
            cleaned_program_data.append(doc)

        program_meta_schema = StructType([
            StructField("_id", StringType(), True),
            StructField("createdAt", StringType(), True), 
            StructField("name", StringType(), True)
        ])

        program_meta_df = spark.createDataFrame(cleaned_program_data, program_meta_schema)

        program_meta_df = program_meta_df.withColumn(
            "program_year",
            F.year(F.to_timestamp(F.col("createdAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        )
        
        projects_df = projects_df.join(
            program_meta_df.select(col("_id").alias("p_id"), "program_year"),
            projects_df["programInformation._id"] == col("p_id"), 
            "left"
        ).drop("p_id")
        
    else:
        successLogger.debug("No valid ObjectIds found to query programs. Adding null program_year.")
        projects_df = projects_df.withColumn("program_year", F.lit(None).cast(IntegerType()))

    # --- AGGREGATION LOGIC ---

    schools_df = projects_df.select(
        F.col("_id").alias("project_id"),
        F.explode_outer(F.col("userProfile.userLocations")).alias("user_location")
    ).filter(
        F.col("user_location.type") == "school"
    ).select(
        F.col("project_id"),
        F.col("user_location.id").alias("school_id"),
        F.col("user_location.name").alias("school_name")
    ).distinct()

    program_df = projects_df.select(
        F.col("programInformation.name").alias("program_name"),
        F.col("category"),
        F.col("state"),
        F.col("district"),
        F.col("_id").alias("project_id"),
        F.col("status"),
        F.col("evidence_status"),
        F.col("userId"),
        F.col("program_year")
    ).filter(
        (F.col("program_name").isNotNull()) &
        (F.col("state").isNotNull()) &
        (F.col("district").isNotNull())
    )

    program_df = program_df.join(
        schools_df,
        program_df["project_id"] == schools_df["project_id"],
        "left"
    ).drop(schools_df["project_id"])

    program_summary_df = program_df.groupBy(
        "program_name", "category", "state", "district", "program_year"
    ).agg(
        countDistinct(F.col("project_id")).alias("Total_Micro_Improvement_Projects"),
        countDistinct(
            when(F.col("status") == "started", True), F.col("project_id")
        ).alias("Total_Micro_Improvement_Started"),
        countDistinct(
            when(F.col("status") == "inProgress", True), F.col("project_id")
        ).alias("Total_Micro_Improvement_InProgress"),
        countDistinct(
            when(F.col("status") == "submitted", True), F.col("project_id")
        ).alias("Total_Micro_Improvement_Submitted"),
        countDistinct(
            when((F.col("evidence_status") == True) & (F.col("status") == "submitted"), True),
            F.col("project_id")
        ).alias("Total_Micro_Improvement_Submitted_With_Evidence"),
        countDistinct(F.col("userId")).alias("Unique_users_in_program"),
        countDistinct(
            when(F.col("status") == "submitted", True), F.col("userId")
        ).alias("Unique_users_completed"),
        countDistinct(F.col("school_id")).alias("Unique_schools_in_program"),
        countDistinct(
            when(F.col("status") == "submitted", True), F.col("school_id")
        ).alias("Unique_schools_completed")
    )

    program_summary_df = program_summary_df.join(
        state_id_mapping, 
        program_summary_df["state"] == state_id_mapping["id"], 
        "left"
    ).drop(state_id_mapping["id"])

    program_summary_df = program_summary_df.join(
        district_id_mapping, 
        program_summary_df["district"] == district_id_mapping["id"], 
        "left"
    ).drop(district_id_mapping["id"])

    program_final_df = program_summary_df.select(
        F.col("program_name").alias("Program Name"),
        F.col("category").alias("Category"),
        F.col("state_name").alias("State Name"),
        F.col("district_name").alias("District Name"),
        F.col("Total_Micro_Improvement_Projects").alias("Total Micro Improvement Projects"),
        F.col("Total_Micro_Improvement_Started").alias("Total Micro Improvement Started"),
        F.col("Total_Micro_Improvement_InProgress").alias("Total Micro Improvement InProgress"),
        F.col("Total_Micro_Improvement_Submitted").alias("Total Micro Improvement Submitted"),
        F.col("Total_Micro_Improvement_Submitted_With_Evidence").alias("Total Micro Improvement Submitted With Evidence"),
        F.col("Unique_users_in_program").alias("Unique Users In Program"),
        F.col("Unique_users_completed").alias("Unique Users Completed"),
        F.col("Unique_schools_in_program").alias("Unique Schools In Program"),
        F.col("Unique_schools_completed").alias("Unique Schools Completed"),
        F.col("program_year").alias("Year")
    ).sort("State Name", "District Name", "Program Name")

    program_local_path = local_path + "program_details/"
    if not os.path.exists(program_local_path):
        os.makedirs(program_local_path)

    program_final_df.coalesce(1).write.format("csv").option("header", True).mode("overwrite").save(program_local_path)

    os.chdir(program_local_path)
    result = glob.glob(f'*.csv')
    os.rename(f'{program_local_path}{result[0]}', f'{program_local_path}program_wise_improvement.csv')

    program_json_keys = [
        "Program Name", "Category", "State Name", "District Name",
        "Total Micro Improvement Projects", "Total Micro Improvement Started",
        "Total Micro Improvement InProgress", "Total Micro Improvement Submitted",
        "Total Micro Improvement Submitted With Evidence",
        "Unique Users In Program", "Unique Users Completed",
        "Unique Schools In Program", "Unique Schools Completed", "Year"
    ]
    program_json_data = []

    with open(os.path.join(program_local_path, 'program_wise_improvement.csv'), 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            program_json_data.append(row)

    program_json = {
        'keys': program_json_keys,
        'tableData': program_json_data
    }

    with open(os.path.join(program_local_path, 'program_wise_improvement.json'), 'w') as json_file:
        json.dump(program_json, json_file, indent=2)

    successLogger.info("Report 3 (Program-wise detailed report) completed.")

except Exception as e:
    errorLogger.error("Error in Report 3 Logic", exc_info=True)

# ============================================================================
# CLOUD UPLOAD
# ============================================================================

cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/micro_improvement", local_Path = local_path, file_Name = 'micro_improvement.json')
cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/micro_improvement", local_Path = local_path, file_Name = 'micro_improvement.csv')

cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/improvement_journey_summary", local_Path = summary_local_path, file_Name = 'improvement_journey_summary.json')
cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/improvement_journey_summary", local_Path = summary_local_path, file_Name = 'improvement_journey_summary.csv')

cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/program_wise_improvement", local_Path = program_local_path, file_Name = 'program_wise_improvement.json')
cloud_init.upload_to_NVSK_cloud(blob_Path = f"{blob_path}/program_wise_improvement", local_Path = program_local_path, file_Name = 'program_wise_improvement.csv')

successLogger.info("NVSK processing completed successfully.")

# ========================== CLEANUP ==========================
spark.stop()