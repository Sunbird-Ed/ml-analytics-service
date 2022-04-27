from configparser import ConfigParser, ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import Iterable
import dateutil,os
from dateutil import parser

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

clientProd = MongoClient(config.get('MONGO', 'mongo_url'))
db = clientProd[config.get('MONGO', 'database_name')]
obsSubmissionsCollec = db[config.get('MONGO', 'observation_sub_collection')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]

print("Number of unique users submitted observations till date:- "+str(len(obsSubmissionsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-01-01T00:00:00.000Z")}}))))
print("Number of unique users submitted projects till date:- "+str(len(projectsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-01-01T00:00:00.000Z")}}))))
print("Number of unique users submitted observations on jan 2022:- "+str(len(obsSubmissionsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-01-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-01-31T00:00:00.000Z")}}))))
print("Number of unique users submitted projects on jan 2022:- "+str(len(projectsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-01-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-01-31T00:00:00.000Z")}}))))
print("Number of unique users submitted observations on feb 2022:- "+str(len(obsSubmissionsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-02-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-02-28T00:00:00.000Z")}}))))
print("Number of unique users submitted projects on feb 2022:- "+str(len(projectsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-02-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-02-28T00:00:00.000Z")}}))))
print("Number of unique users submitted observations on mar 2022:- "+str(len(obsSubmissionsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-03-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-03-31T00:00:00.000Z")}}))))
print("Number of unique users submitted projects on mar 2022:- "+str(len(projectsCollec.distinct("createdBy",{"createdAt":{"$gte":dateutil.parser.parse("2022-03-01T00:00:00.000Z"),"$lte":dateutil.parser.parse("2022-03-31T00:00:00.000Z")}}))))
