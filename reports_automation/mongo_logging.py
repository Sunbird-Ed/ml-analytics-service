from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, sys
from configparser import ConfigParser,ExtendedInterpolation
from datetime import datetime



config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/report_config.ini")

client = MongoClient(config.get('MONGO', 'url'))
db = client[config.get('MONGO', 'database_name')]
log_collec = db[config.get('MONGO', 'reports_log_collec')]

def insert_doc(doc,type):
    curr_datetime = datetime.now()
    doc["createdAt"] = curr_datetime
    doc["release"] = config.get("REPORTS_FILEPATH","json_path")


    if type == "crud":
       doc["status"] = "Success"
    elif type == "error" or type == "exception":
       doc["status"] = "Failed"


    log_collec.insert_one(doc)




