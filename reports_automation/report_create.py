import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

sys.path.insert(0, '/opt/sparkjobs/ml-analytics-service/reports_automation/')
from mongo_logging import *

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/report_config.ini")

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': config.get("API_HEADERS", "content_type"),
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }

folder_path = config.get("REPORTS_FILEPATH","base_path") + config.get("REPORTS_FILEPATH","folder_name") + "/"

# Creation of chart using Json config making an API call
def backEnd_create(file_name):
     try :
        url_backend_create = base_url + config.get("API_ENDPOINTS","backend_create")
        file_path = folder_path + "backEndConfig" + "/" + file_name
        with open(file_path) as data_file:
            json_config = json.load(data_file)
            json_config["request"]["createdBy"] = config.get("JSON_VARIABLE","createdBy")
            json_config["request"]["config"]["container"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["store"] = config.get("JSON_VARIABLE","store")
            json_config["request"]["config"]["key"] = config.get("JSON_VARIABLE","key")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["postContainer"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["container"] = config.get("JSON_VARIABLE","container")

            doc = {
                    "configFileName" : file_name,
                    "config" : json.dumps(json_config),
                    "operation": "backEnd_create"
                  }

        #Api call
        response_api = requests.post(
                url_backend_create,
                data= json.dumps(json_config),
                headers=headers_api
            )

        # Based on status concluding logging the output
        if response_api.status_code == 200:
           response_data = response_api.json()
           if response_data["params"]["status"] ==  "failed":
              doc["errmsg"] = "ReportId already Exists"
              type = "error"
           else:
              type = "crud"

        else:
           doc["errmsg"] = str(response_api.status_code)  + response_api.text
           type = "error"

     except Exception as exception:
             doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
             type = "exception"

     insert_doc(doc,type)

# Creation of report using Json config making an API call
def frontEnd_create(access_token,file_name):
     try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_create = base_url + config.get("API_ENDPOINTS","frontend_create")
        file_path = folder_path + "frontEndConfig" + "/" + file_name
        with open(file_path) as data_file:
                 json_config = json.load(data_file)
                 json_config["request"]["report"]["createdby"] = config.get("JSON_VARIABLE","createdBy")

        doc = {
                  "configFileName" : file_name,
                  "config" : json.dumps(json_config),
                  "operation": "frontEnd_create"
               }

        value_check = query_mongo(file_name,json_config,"frontEndConfig")
        if value_check == "create":

           #Api call
           response_api = requests.post(
                   url_frontend_create,
                   data= json.dumps(json_config),
                   headers=headers_api
                )

           # Based on status concluding the logging output
           if response_api.status_code == 200 or response_api.status_code == 201:
              type = "crud"
           else:
              doc["errmsg"] = str(response_api.status_code)  + response_api.text
              type = "error"

        # If config has changes then call updateAPI
        elif value_check == "update":
           frontEnd_update()

        else :
            doc["operation"]= "frontEnd_create_duplicate_run"
            type = "duplicate_run"
            pass

     except Exception as exception:
            doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
            type = "exception"

     insert_doc(doc,type)
     data_file.close

def frontEnd_update():
    print("Inside update function")
