from pymongo import MongoClient
from bson.objectid import ObjectId
import csv,sys
import json
import re
from datetime import datetime
import argparse
import os
from configparser import ConfigParser,ExtendedInterpolation
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
from pyspark.sql import DataFrame
from typing import Iterable

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/source/observations/config.ini")

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

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
ObsSubCollec = db[config.get('MONGO', 'observation_sub_collection')]
solutionCollec = db[config.get('MONGO', 'solutions_collection')]
programCollec = db[config.get("MONGO", 'programs_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]

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
   print(e)


parser = argparse.ArgumentParser()
parser.add_argument('--programId','--programId',type=str)
parser.add_argument('--solutionId','--solutionId',type=str)
parser.add_argument('--solCSVFileName','--solCSVFileName',type=str)
argument = parser.parse_args()

#for i in ObsSubCollec.find({"programId":ObjectId(argument.programId),"solutionId":ObjectId(argument.solutionId)}):
#    print(i)
#    sys.exit()

obs_sub_cursorMongo = ObsSubCollec.aggregate(
	[{"$match": {"programId":ObjectId(argument.programId),"solutionId":ObjectId(argument.solutionId)}},
         {
             "$project": {
                      "_id": {"$toString": "$_id"},
                      "entityId": {"$toString": "$entityId"},
                      "status": 1,         
                      "solutionId": {"$toString": "$solutionId"},
                      "programId": {"$toString": "$programId"},
                      "userRoleInformation": 1,
                      "createdBy": 1
                       
	     }
        }]
)


#schema for the observation submission dataframe
obs_sub_schema = StructType(
   [
      StructField('_id', StringType(), True),
      StructField('status', StringType(), True),
      StructField('entityId', StringType(), True),
      StructField('solutionId', StringType(), True),
      StructField('programId', StringType(), True),
      StructField('createdBy', StringType(), True),
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
   ]
)

spark = SparkSession.builder.appName(
   "obs_sub_reports"
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
obs_sub_df1 = spark.createDataFrame(obs_sub_rdd,obs_sub_schema)

print(str(obs_sub_df1.count()))

#observation program dataframe
obs_pgm_cursorMongo = programCollec.aggregate(
   [{"$match": {"_id":ObjectId(argument.programId)}},
    {"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
)

#schema for the observation program dataframe
obs_pgm_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_pgm_rdd = spark.sparkContext.parallelize(list(obs_pgm_cursorMongo))
obs_pgm_df = spark.createDataFrame(obs_pgm_rdd,obs_pgm_schema)
obs_pgm_cursorMongo.close()

#observation solution dataframe
obs_sol_cursorMongo = solutionCollec.aggregate(
   [{"$match": {"_id":ObjectId(argument.solutionId)}},
    {"$project": {"_id": {"$toString": "$_id"}, "name": 1}}]
)

#schema for the observation solution dataframe
obs_sol_schema = StructType([
   StructField('name', StringType(), True),
   StructField('_id', StringType(), True)
])

obs_sol_rdd = spark.sparkContext.parallelize(list(obs_sol_cursorMongo))
obs_sol_df = spark.createDataFrame(obs_sol_rdd,obs_sol_schema)

obs_sol_cursorMongo.close()

#match solution id from solution df to submission df to fetch the solution name
obs_sub_pgm_df = obs_sub_df1.join(
   obs_pgm_df,
   obs_sub_df1.programId==obs_pgm_df._id,
   'inner'
).drop(obs_pgm_df["_id"])
obs_sub_pgm_df = obs_sub_pgm_df.withColumnRenamed("name", "program_name")

obs_sub_sol_df = obs_sub_pgm_df.join(
   obs_sol_df,
   obs_sub_pgm_df.solutionId==obs_sol_df._id,
   'inner'
).drop(obs_sol_df["_id"])
obs_sub_sol_df = obs_sub_sol_df.withColumnRenamed("name", "solution_name")

obs_sub_sol_df = obs_sub_sol_df.select(F.col("_id").alias("submission_id"),"status","entityId","createdBy","userRoleInformation.district","program_name","solution_name")
obs_sub_cursorMongo.close()

obs_entities_id_df = obs_sub_sol_df.select("district")
entitiesId_obs_status_df_before = []
entitiesId_arr = []
uniqueEntitiesId_arr = []
entitiesId_obs_status_df_before = obs_entities_id_df.toJSON().map(lambda j: json.loads(j)).collect()
for eid in entitiesId_obs_status_df_before:
   try:
    entitiesId_arr.append(eid["district"])
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
entities_df = melt(entities_df,
        id_vars=["_id","entityType","metaInformation.name"],
        value_vars=["registryDetails.locationId", "registryDetails.code"]
    ).select("_id","entityType","name","value"
            ).dropDuplicates()
entities_df = entities_df.withColumn("variable",F.col("entityType"))

obs_sub_df_melt = melt(obs_sub_sol_df,
        id_vars=["submission_id","status","entityId","createdBy","district","program_name","solution_name"],
        value_vars=["district"]
        )

obs_ent_sub_df_melt = obs_sub_df_melt\
                 .join(entities_df,["variable","value"],how="left")\
                 .select(obs_sub_df_melt["*"],entities_df["name"])
obs_ent_sub_df_melt = obs_ent_sub_df_melt.withColumn("flag",F.concat(F.col("variable"),F.lit("_name")))
obs_ent_sub_df_melt = obs_ent_sub_df_melt.groupBy(["status","submission_id"])\
                               .pivot("flag").agg(first(F.col("name")))
obs_sub_df_final = obs_sub_sol_df.join(obs_ent_sub_df_melt,["status","submission_id"],how="left")
obs_sub_df_final = obs_sub_df_final.drop("district")

obs_sub_df_final_metric = obs_sub_df_final.groupBy("program_name","solution_name","district_name").agg(countDistinct(F.col("createdBy"),when(F.col("status") == "completed",True)).alias("Unique Users who submitted form"),countDistinct(F.col("submission_id"),when(F.col("status") == "completed",True)).alias("Total submissions"),countDistinct(F.col("createdBy"),when(F.col("status") == "started",True)).alias("Unique Users who started form"),countDistinct(F.col("entityId"),when(F.col("status") == "completed",True)).alias("Total entities observed"))

obs_sub_df_final_metric.repartition(1).write.format("csv").option("header",True).mode("overwrite").save(
               "/opt/sparkjobs/source/hp_reports_metrics/output/"+argument.solCSVFileName+"/" 
)
