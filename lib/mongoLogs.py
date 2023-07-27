from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, sys
from configparser import ConfigParser,ExtendedInterpolation
from datetime import datetime
import lib.constants as constants


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

client = MongoClient(config.get('MONGO', 'url'))
db = client[config.get('MONGO', 'database_name')]
log_collec = db[constants.batch_logger_collection]

curr_datetime = datetime.now()


def getLogs(query): 
    mydoc = log_collec.find_one(query)
    if mydoc:
       doc_count = 1
    else:
       doc_count = 0

    returnValue = {}

    if doc_count > 0:
      returnValue['response'] = {}
      returnValue['response'] = {
              "dataSource" : mydoc['dataSource'],
              "taskId" : mydoc['taskId'],
              "taskCreatedDate" : mydoc['taskCreatedDate']
          }
      if not mydoc["statusCode"] == constants.success_status_1 or not mydoc["statusCode"] == constants.success_status_2:
        returnValue['duplicateChecker'] = True
        returnValue['dataFixer'] = True
      elif mydoc["statusCode"] == constants.success_status_1 or mydoc["statusCode"] == constants.success_status_2:
        returnValue['duplicateChecker'] = True
        returnValue['dataFixer'] = False
    else :
       returnValue['response'] = {}
       returnValue['duplicateChecker'] = False
       returnValue['dataFixer'] = False
    
    return returnValue

def insertLog(docMongo):
    docMongo["createdAt"] = curr_datetime
    docMongo["updatedAt"] = curr_datetime

    if docMongo["statusCode"] == constants.success_status_1 or docMongo["statusCode"] == constants.success_status_2:
       docMongo["status"] = "Success"
    else:
       docMongo["status"] = "Failed"
    
    log_collec.insert_one(docMongo)