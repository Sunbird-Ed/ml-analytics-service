
import pyspark
import logging
import requests
import datetime
from datetime import date
from pyspark.sql import Row
from pymongo import MongoClient
import json, csv, sys, os, time
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from bson.objectid import ObjectId
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from azure.storage.blob import BlockBlobService
from pyspark.sql.functions import element_at, split, col
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

# Reading varible from config files
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

# Formats the logs 
formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# Success Logger
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successHandler = RotatingFileHandler(config.get('LOGS','observation_streaming_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_success'), when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Error Logger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS','observation_streaming_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

# Interacting with Azure Blob
blob_service_client = BlockBlobService(
   account_name=config.get("AZURE", "account_name"), 
   sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
blob_path = config.get("AZURE", "observations_blob_path")
blob_service_client.get_blob_to_path(container_name,  f"{blob_path}sl-observation.json", "./obs_old.json")

# Starting Spark session
spark = SparkSession.builder.appName("obs_batch_status").config(
   "spark.driver.memory", "50g"
).config(
   "spark.executor.memory", "100g"
).config(
   "spark.memory.offHeap.enabled", True
).config(
   "spark.memory.offHeap.size", "32g"
).getOrCreate()

old_df = spark.read.json("obs_old.json")
old_df.printSchema()

# Gathering data from MongoDB
clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]

# Mongo Query
mongo_query = obsSubmissionsCollec.aggregate(
   [{"$match":{"status":"completed"}},
       {
      "$project": {
         "_id": {"$toString": "$_id"}, 
         "userProfile": 1
      }}
    ]
)

# Schema to gather data from Mongo DB
new_schema = StructType([
      StructField('_id', StringType(), True),
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
                "userLocations", ArrayType(
                     StructType([
                            StructField("type", StringType(), True),
                            StructField("name", StringType(), True),
                            StructField("id", StringType(), True)
                     ])
              ))
   
          ])
      ),
])

sc=spark.sparkContext
obs_df = spark.sparkContext.parallelize(list(mongo_query))
new_df = spark.createDataFrame(obs_df,new_schema)


# Getting Organisation details
orgSchema = ArrayType(StructType([StructField("orgId", StringType(), False),StructField("orgName", StringType(), False)]))
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
new_df = new_df.withColumn("orgData",orgInfo_udf(F.col("userProfile.organisations")))
new_df = new_df.withColumn("exploded_orgInfo",F.explode_outer(F.col("orgData")))

# Getting data from UserLocation
nameSchema = ArrayType(StructType([
              StructField("state_name", StringType(), True),
              StructField("district_name", StringType(), True),
              StructField("block_name", StringType(), True),
              StructField("cluster_name", StringType(), True),
              StructField("school_name", StringType(), True),
              StructField("state_externalId", StringType(), True),
              StructField("block_externalId", StringType(), True),
              StructField("district_externalId", StringType(), True),
              StructField("school_externalId", StringType(), True),
              StructField("cluster_externalId", StringType(), True)]))
def getName(val):
       namearr = []
       nameObj = {}
       if val is not None:
              nameObj = {}
              for loc in val:
                     nameObj[f'{loc["type"]}_externalId'] = loc["id"]
                     nameObj[f'{loc["type"]}_name'] = loc["name"]
                     namearr.append(nameObj)
       return namearr
nameInfo_udf = udf(getName,nameSchema)
new_df = new_df.withColumn("nameData",nameInfo_udf(F.col("userProfile.userLocations")))
new_df = new_df.withColumn("exploded_nameInfo",F.explode_outer(F.col("nameData")))


# Creating the new dataframe with updated columns
obs_new_df = new_df.select(
       new_df["_id"].alias("submission_id"),  
       concat_ws(",",array_distinct(F.col("userProfile.profileUserTypes.type"))).alias("user_type"),
       concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
       new_df["exploded_orgInfo"]["orgId"].alias("organisation_id"),
       new_df["exploded_orgInfo"]["orgName"].alias("organisation_name"),
       new_df["exploded_nameInfo"]["block_name"].alias("block_name"),
       new_df["exploded_nameInfo"]["district_name"].alias("district_name"),
       new_df["exploded_nameInfo"]["state_name"].alias("state_name"),
       new_df["exploded_nameInfo"]["school_name"].alias("school_name"),
       new_df["exploded_nameInfo"]["cluster_name"].alias("cluster_name"),
       new_df["exploded_nameInfo"]["block_externalId"].alias("block_externalId"),
       new_df["exploded_nameInfo"]["state_externalId"].alias("state_externalId"),
       new_df["exploded_nameInfo"]["district_externalId"].alias("district_externalId"),
       new_df["exploded_nameInfo"]["school_externalId"].alias("school_externalId"),
       new_df["exploded_nameInfo"]["cluster_externalId"].alias("cluster_externalId")
)

obs_old_df = old_df.drop("organisation_name")
# Join two DF
pre_final_df = obs_old_df.join(obs_new_df, obs_old_df.observationSubmissionId ==  obs_new_df.submission_id,"full")
final_df = pre_final_df.dropDuplicates()

# Save in Azurite
final_df.coalesce(1).write.format("json").mode("overwrite").save("observation_new")

saved_file = [file for file in os.listdir('./observation_new') if file.endswith('.json')] 
blob_service_client.create_blob_from_path(container_name, "sl-observation_new.json",f"observation_new/{saved_file[0]}")
print("_Uploaded to Azure_")

# Druid Query
payload = {}
payload = json.loads(config.get("AZURE", "observation_ingestion_spec"))
datasource = [payload["spec"]["dataSchema"]["dataSource"]]
ingestion_spec = [json.dumps(payload)]       
for i, j in zip(datasource,ingestion_spec):
    druid_end_point = config.get("DRUID", "metadata_url") + i
    druid_batch_end_point = config.get("DRUID", "batch_url")
    headers = {'Content-Type' : 'application/json'}
    get_timestamp = requests.get(druid_end_point, headers=headers)
    start_supervisor = requests.post(druid_batch_end_point, data=j, headers=headers)
    if start_supervisor.status_code == 200:
        successLogger.debug("started the batch ingestion task sucessfully for the datasource " + i)
        time.sleep(50)
    else:
        errorLogger.error(start_supervisor.text)
        errorLogger.error("failed to start batch ingestion task" + str(start_supervisor.status_code))   

# removing the downloaded file
os.remove("obs_old.json")
print("Downloaded File removed: The code run was successfull.")
