# -----------------------------------------------------------------
# Gather program ids for storing & running in Shell script
# -----------------------------------------------------------------

import json, sys, time, os
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId
import requests
import datetime
from slackclient import SlackClient
from datetime import date

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
bot = SlackClient(config.get("SLACK","token"))

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]

with open(config.get('OUTPUT_DIR', 'program_text_file'), mode='w') as file:
    data = projectsCollec.distinct("programId")
    for ids in data:
        ids = str(ids)
        if ids != 'None':
            file.write(f"{ids}\n")

bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Gathered ProgramIDs: {datetime.date()}") 





