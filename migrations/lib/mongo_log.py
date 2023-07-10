from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, sys
from configparser import ConfigParser,ExtendedInterpolation
from datetime import datetime
import constants

root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(root_path + "config.ini")

client = MongoClient(config.get('MONGO', 'url'))
db = client[config.get('MONGO', 'database_name')]
log_collec = db[constants.reports_log_collec]

curr_datetime = datetime.now()

def insert_doc(docMongo,type):
    docMongo["createdAt"] = curr_datetime
    docMongo["updatedAt"] = curr_datetime

    if type == "crud":
       docMongo["status"] = "Success"
    elif type == "error" or type == "exception":
       docMongo["status"] = "Failed"
    elif type == "duplicate_run":
       docMongo["status"] = "Skipped"

    log_collec.insert_one(docMongo)


def query_mongo(file_path,file):
    #Query MongoDb
    mydoc = log_collec.find({"config_file_name":file_path})
    doc_count = mydoc.count()

    if doc_count > 0:
       for doc in mydoc:
          if doc["status"] == "Failed":
                return "create"
          elif doc["status"] == "Success" and  doc["config"] == file:
                return "update"
          else:
                return "pass"

    else :
       return "create"


def query_retire(file):
   #Query MongoDb
   mydocRetire = log_collec.find({"file": file})
   mydocRetire_count = mydocRetire.count()
   
   if mydocRetire_count > 0:
      for doc in mydocRetire:
         if doc["status"] == "Failed":
               return True
         elif doc["status"] == "Success" :
               return False
   else :
      return True

