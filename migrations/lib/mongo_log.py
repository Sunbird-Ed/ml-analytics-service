from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, sys
from configparser import ConfigParser,ExtendedInterpolation
from datetime import datetime



config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/ml-analytics-service/migrations/releases/report_config.ini")

client = MongoClient(config.get('MONGO', 'url'))
db = client[config.get('MONGO', 'database_name')]
log_collec = db[config.get('MONGO', 'reports_log_collec')]

curr_datetime = datetime.now()

def insert_doc(doc,type):
    doc["createdAt"] = curr_datetime
    doc["updatedAt"] = curr_datetime
    doc["release"] = config.get("REPORTS_FILEPATH","folder_name")

    if type == "crud":
       doc["status"] = "Success"
    elif type == "error" or type == "exception":
       doc["status"] = "Failed"
    elif type == "duplicate_run":
       doc["status"] = "Skipped"

    log_collec.insert_one(doc)

def query_mongo(file_name,file,type):
    #Query MongoDb
    mydoc = log_collec.find({"configFileName":file_name})
    doc_count = mydoc.count()

    if doc_count > 0:
       for doc in mydoc:
          if (doc["createdAt"].strftime("%m/%d/%Y") == curr_datetime.strftime("%m/%d/%Y")):
             if doc["status"] == "Failed":
                return "create"
             elif doc["status"] == "Success" and  doc["config"] == file:
                return "update"
             else:
                return "pass"
          else :
             return "create"
    else :
       return "create"
