import requests,json,csv,os,sys
from configparser import ConfigParser,ExtendedInterpolation
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import countDistinct
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--interval','--interval',type=str,required=True,help='provide druid query interval')
parser.add_argument('--configPath','--configPath',type=str,required=True,help='provide script config path')
argument = parser.parse_args()

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(argument.configPath+ "/config.ini")

spark = SparkSession.builder.appName("active_user").config(
    "spark.driver.memory", "50g"
).config(
    "spark.executor.memory", "100g"
).config(
    "spark.memory.offHeap.enabled", True
).config(
    "spark.memory.offHeap.size", "32g"
).getOrCreate()

sc = spark.sparkContext


activeUser_querySpec = {"queryType":"scan","dataSource":config.get("DRUID","datasource_name"),"resultFormat": "list","columns":["actor_id","mid"],"filter":{"type":"and","fields":[{"type":"selector","dimension":"edata_pageid","value":"admin-home"},{"type":"selector","dimension":"user_login_type","value":"administrator"}]},"intervals":[argument.interval]}

urlDruidQuery = config.get("DRUID","url")
header = {"Content-Type" : "application/json"}

activeUser_responseQuery = requests.post(url=urlDruidQuery,headers=header,data=json.dumps(activeUser_querySpec))

ml_active_user_schema = StructType(
   [
      StructField(
        'events',
         ArrayType(
             StructType([
                  StructField('actor_id', StringType(), True),
                  StructField('mid', StringType(), True)
                  ])
         ))
   ])
if activeUser_responseQuery.status_code == 200 :
    activeUser_responseDataArr = activeUser_responseQuery.json()
    ml_active_user_rdd = spark.sparkContext.parallelize(list(activeUser_responseDataArr))
    ml_active_user_df = spark.createDataFrame(ml_active_user_rdd,ml_active_user_schema)
    ml_active_user_df = ml_active_user_df.withColumn(\
       "exploded_events", F.explode_outer(ml_active_user_df["events"])\
    )
    ml_active_user_df = ml_active_user_df.select("exploded_events.actor_id")
    ml_active_user_df = ml_active_user_df.groupBy("actor_id").count()
    ml_active_user_df = ml_active_user_df.filter(F.col("count")>=2)
    ml_active_user_df = ml_active_user_df.select(countDistinct("actor_id"))
    ml_active_user_df.show()
