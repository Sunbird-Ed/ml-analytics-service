import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/ml-analytics-service/config.ini")

sys.path.insert(0, config.get("REPORTS_FILEPATH","script_path"))

from mongo_log import *
from get_token import get_access_token
import constants

response_api = get_access_token()
if response_api["status_code"] == constants.success_code:
   access_token = response_api["result"]["access_token"]

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': config.get("API_HEADERS", "content_type"),
        'Authorization' : config.get("API_HEADERS","authorization_access_token"),
        'x-authenticated-user-token' : access_token
    }


def get_report(report_ids):
   doc = {
            "operation": "get_report"
           }
   try :
         for id in report_ids:
            url_frontend_get = base_url + config.get("API_ENDPOINTS","frontend_get") + id
            response_api = requests.get(
                             url_frontend_get,
                             headers=headers_api
                           )

            if response_api.status_code == constants.success_code:
               json_config = response_api.json()
               json_config = json_config["result"]["reports"][0]
               doc["config"] : json.dumps(json_config)
               response_type = "crud"
               update_report(json_config,id)
            else :
               doc["errmsg"] = str(response_api.status_code)  + response_api.text
               response_type = "error"


   except Exception as exception:
         doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
         response_type = "exception"
   insert_doc(doc,response_type)

# Creation of chart using Json config making an API call
def update_report(json_config,id):
    doc = {
            "operation": "frontend_update",
            "reportid" : json_config["reportid"],
            "title" : json_config["reportconfig"]["title"]
           }
    tag = json_config["title"].replace(" ","_")
    update_config = {"request":{}}
    json_config["createdby"] = config.get("JSON_VARIABLE","createdBy")
    key_arr = ["children","templateurl","accesspath","visibilityflags","reportid","tags"]
    for key in key_arr:
       del json_config[key]
    try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_update = base_url + config.get("API_ENDPOINTS","frontend_update") + id
        doc["config"] : json.dumps(json_config)
        update_config["request"]["report"] =  json_config
        update_config["request"]["report"]["tags"] =  [tag]
        response_api = requests.patch(
                   url_frontend_update,
                   data= json.dumps(update_config),
                   headers=headers_api
                )

        if response_api.status_code == constants.success_code:
            doc["api_response"] = response_api.json()
            typeErr = "crud"
        else:
            doc["errmsg"] = str(response_api.status_code)  + response_api.text
            typeErr = "error"
        doc["api_response"] = response_api.json()

    except Exception as exception:
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"

    insert_doc(doc,typeErr)

report_ids = config.get("REPORT_IDS","update_tag").split(',')
get_report(report_ids)
