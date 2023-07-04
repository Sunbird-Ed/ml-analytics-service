# -----------------------------------------------------------------
# Gather program ids for storing & running in Shell script
# -----------------------------------------------------------------

import json, sys, time, os, re
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import requests
import datetime
import argparse
import pyspark.sql.utils as ut
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from collections import OrderedDict, Counter
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from datetime import date
from pyspark.sql import DataFrame
from typing import Iterable
from pyspark.sql.functions import element_at, split, col

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'project_success_error')
file_name_for_output_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-output.log"
file_name_for_debug_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-debug.log"

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# handler for debug Log
debug_logHandler = RotatingFileHandler(f"{file_name_for_debug_log}")
debug_logHandler.setFormatter(formatter)

#add the Infologer
infoLogger = logging.getLogger('info log')
infoLogger.setLevel(logging.INFO)
debug_logBackuphandler = TimedRotatingFileHandler(f"{file_name_for_debug_log}",when="w0",backupCount=1)
infoLogger.addHandler(debug_logHandler)
infoLogger.addHandler(debug_logBackuphandler)


clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]

with open(config.get('OUTPUT_DIR', 'program_text_file'), mode='w') as file:
    data = projectsCollec.distinct("programId", {"isAPrivateProgram": False, "isDeleted":False})
    for ids in data:
        ids = str(ids)
        if ids != 'None':
            file.write(f"{ids}\n")

infoLogger.info(f"Gathered ProgramIDs: {datetime.datetime.now()}")






