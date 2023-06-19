import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

sys.path.insert(0, config.get("REPORTS_FILEPATH","script_path"))
from mongo_logging import *
from get_token import get_access_token

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/report_config.ini")

response_api = get_access_token()
if response_api["status_code"] == 200:
   access_token = response_api["result"]["access_token"]




# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': config.get("API_HEADERS", "content_type"),
        'Authorization' : config.get("API_HEADERS","authorization_access_token"),
        'x-authenticated-user-token' : access_token
    }


doc = {}

def get_report(arr):
    try :
         for id in arr:
            url_frontend_get = base_url + config.get("API_ENDPOINTS","frontend_get") + id
            response_api = requests.get(
                             url_frontend_get,
                             headers=headers_api
                           )
#             doc = {
#                               "configFileName" : file_name,
#                               "config" : json.dumps(json_config),
#                               "operation": "frontend_create"
#                            }
            if response_api.status_code == 200:
               json_config = response_api.json()
               json_config = json_config["result"]["reports"][0]
               type = "crud"
               update_report(json_config,id)
            else :
               doc["errmsg"] = str(response_api.status_code)  + response_api.text
               type = "error"


    except Exception as exception:
         doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
         type = "exception"

# Creation of chart using Json config making an API call
def update_report(json_config,id):
    doc = {}
    json_config["createdby"] = config.get("JSON_VARIABLE","createdBy")
    key_arr = ["children","templateurl","accesspath","visibilityflags"]
    for key in key_arr:
       del json_config[key]
    try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_update = base_url + config.get("API_ENDPOINTS","frontend_update") + id

        doc = {
#                  "configFileName" : file_name,
                  "config" : json.dumps(json_config),
                  "operation": "frontend_update",
                  "reportid" : json_config["reportid"],
                  "title" : json_config["reportconfig"]["title"]
               }


        response_api = requests.patch(
                   url_frontend_update,
                   data= json.dumps(json_config),
                   headers=headers_api
                )

        if response_api.status_code == 200:
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
    data_file.close

report_ids = config.get("REPORT_IDS","update_tag")
get_report(report_ids)