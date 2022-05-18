# -----------------------------------------------------------------
# Name : pyspark_temp_script_add_cols.py
# Author : Ashwini
# Description : This program adds columns based on some condition
#               taking input from already proccessed data, to refresh the datasource
# -----------------------------------------------------------------

import os
from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
from pyspark.sql import DataFrame
from pyspark.sql.functions import element_at, split, col

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]

obs_sub_cursorMongo = obsSubmissionsCollec.aggregate(
    [{"$match": {'status':'completed'}},
        {"$project": {
         "_id": {"$toString": "$_id"},
         "entityId": {"$toString": "$entityId"},
         "status": 1,
         "entityExternalId": 1,
         "entityInformation": {"name": 1},
         "entityType": 1,
         "createdBy": 1,
         "solutionId": {"$toString": "$solutionId"},
         "solutionExternalId": 1,
         "updatedAt": 1, 
         "completedDate": 1,
         "programId": {"$toString": "$programId"},
         "programExternalId": 1,
         "appInformation": {"appName": 1},
         "isAPrivateProgram": 1,
         "isRubricDriven":1,
         "criteriaLevelReport":1,
         "ecm_marked_na": {
            "$reduce": {
               "input": "$evidencesStatus",
               "initialValue": "",
               "in": {
                  "$cond": [
                     {"$eq": [{"$toBool":"$$this.notApplicable"},True]}, 
                     {"$concat" : ["$$value", "$$this.name", ";"]}, 
                     "$$value"
                  ]
               }
            }
         },
         "userRoleInformation": 1,
         "userProfile":1
      }
   }]
)


obs_sub_schema = StructType(
   [
      StructField('status', StringType(), True),
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
                        StructField('orgName', StringType(), True)
                     ]), True)
             )
          ])
      )
   ]
)

spark = SparkSession.builder.appName(
   "obs_sub_status"
).config(
   "spark.driver.memory", "50g"
).config(
   "spark.executor.memory", "100g"
).config(
   "spark.memory.offHeap.enabled", True
).config(
   "spark.memory.offHeap.size", "32g"
).getOrCreate()


sc=spark.sparkContext
obs_sub_rdd = spark.sparkContext.parallelize(list(obs_sub_cursorMongo))
obs_sub_df = spark.createDataFrame(obs_sub_rdd,obs_sub_schema)


obs_sub_df= obs_sub_df.select(
             "_id", "status",
             concat_ws(",",F.col("userProfile.framework.board")).alias("board_name"),
             element_at(col('userProfile.organisations.orgName'), -1).alias("organisation_name"),
             element_at(col('userProfile.organisations.organisationId'), -1).alias("organisation_id")
)
obs_sub_cursorMongo.close()

source_df = spark.read.json("sl_observation.json")
source_dff = source_df.drop("organisation_name","organisation_id")

final_df = source_dff.join(obs_sub_df, source_dff["observationSubmissionId"] == obs_sub_df["_id"] ,how = "left").select(source_dff["*"],obs_sub_df["organisation_name"],obs_sub_df["organisation_id"],obs_sub_df["board_name"])
final_dff = final_df.dropDuplicates()

final_dff.coalesce(1).write.format("csv").option("header",True).mode("overwrite").save(
        config.get("OUTPUT_DIR", "observation_refresh")+"/"
)
