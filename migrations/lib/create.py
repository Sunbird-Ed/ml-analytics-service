import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/ml-analytics-service/migrations/releases/report_config.ini")

script_path = config.get("REPORTS_FILEPATH","script_path")
sys.path.insert(0, script_path)

from mongo_log import *

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': config.get("API_HEADERS", "content_type"),
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }



# Creation of chart using Json config making an API call
def backend_create(file_name):
     doc = {
            "config_file_name" : file_name,
             "operation": "backend_create"
            }
     try :
        url_backend_create = base_url + config.get("API_ENDPOINTS","backend_create")
        file_path = config.get("REPORTS_FILEPATH","folder_config") + "backend/create/" + file_name
        with open(file_path) as data_file:
            json_config = json.load(data_file)
            json_config["request"]["createdBy"] = config.get("JSON_VARIABLE","createdBy")
            json_config["request"]["config"]["container"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["store"] = config.get("JSON_VARIABLE","store")
            json_config["request"]["config"]["key"] = config.get("JSON_VARIABLE","key")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["postContainer"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["container"] = config.get("JSON_VARIABLE","container")

        doc["config"] = json.dumps(json_config)
        doc["report_id"] = json_config["request"]["reportId"]
        doc["report_title"] = json_config["request"]["config"]["reportConfig"]["id"]

        value_check = query_mongo(file_name,json_config,"backendConfig")
        if value_check == "create":
          #Api call
          response_api = requests.post(
                url_backend_create,
                data= json.dumps(json_config),
                headers=headers_api
            )

          # Based on status concluding logging the output
          if response_api.status_code == 200:
                doc["api_response"] = response_api.json()
                response_type = "crud"

          else:
             doc["errmsg"] = str(response_api.status_code)  + response_api.text
             response_type = "error"

        elif value_check == "update":
             backend_update()
        else :
             doc["operation"]= "backend_create_duplicate_run"
             response_type = "duplicate_run"
             pass

     except Exception as exception:
             doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
             response_type = "exception"

     insert_doc(doc,response_type)

# Creation of report using Json config making an API call
def frontend_create(access_token,file_name):
     doc = {
            "configFileName" : file_name,
            "operation": "frontend_create"
            }
     try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_create = base_url + config.get("API_ENDPOINTS","frontend_create")
        file_path = config.get("REPORTS_FILEPATH","folder_config") + "frontend/create/" + file_name
        with open(file_path) as data_file:
                 json_config = json.load(data_file)
                 json_config["request"]["report"]["createdby"] = config.get("JSON_VARIABLE","createdBy")


        doc["config"] = json.dumps(json_config)
        doc["report_title"] = json_config["request"]["report"]["title"]

        value_check = query_mongo(file_name,json_config,"frontendConfig")
        if value_check == "create":

          #Api call
          response_api = requests.post(
                   url_frontend_create,
                   data= json.dumps(json_config),
                   headers=headers_api
                )


          # Based on status concluding the logging output
          if response_api.status_code == 200 or response_api.status_code == 201:
              response_data = response_api.json()
              doc["report_id"] = response_data["result"]["reportId"]
              response_type = "crud"
          else:
              doc["errmsg"] = str(response_api.status_code)  + response_api.text
              response_type = "error"
          doc["api_response"] = response_api.json()
        elif value_check == "update":
           frontend_update()

        else :
            doc["operation"]= "frontend_create_duplicate_run"
            response_type = "duplicate_run"
            pass
        data_file.close
     except Exception as exception:
            doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
            response_type = "exception"

     insert_doc(doc,response_type)


