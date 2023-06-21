import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
# config.read(root_path + "config.ini")
config.read("/Users/adithyadinesh/Documents/shikshalokam/Data_Engineering/report_config.ini")

sys.path.insert(0, root_path + "migrations/lib")

script_path = config.get("REPORTS_FILEPATH","script_path")
sys.path.insert(0, script_path)

from mongo_log import *
import constants
from update import backend_update,frontend_update

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': constants.content_type,
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }


# Creation of chart using Json config making an API call
def backend_create(file_name,base_path):
     doc = {
             "operation": "backend_create"
            }
     try :
        url_backend_create = base_url + constants.backend_create
        file_path = base_path + "/config/backend/create/" + file_name
        with open(file_path) as data_file:
            json_config = json.load(data_file)
            json_config["request"]["createdBy"] = config.get("JSON_VARIABLE","createdBy")
            json_config["request"]["config"]["container"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["store"] = config.get("JSON_VARIABLE","store")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["postContainer"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["container"] = config.get("JSON_VARIABLE","container")

        doc["config_file_name"] = file_path
        doc["config"] = json.dumps(json_config)
        doc["release"] = base_path
        doc["report_id"] = json_config["request"]["reportId"]
        doc["report_title"] = json_config["request"]["config"]["reportConfig"]["id"]

        value_check = query_mongo(file_path,json_config)
        if value_check == "create":
          #Api call
          response_api = requests.post(
                url_backend_create,
                data= json.dumps(json_config),
                headers=headers_api
            )

          # Based on status concluding logging the output
          if response_api.status_code == constants.success_code:
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
def frontend_create(access_token,file_name,base_path):
     doc = {
            "operation": "frontend_create"
            }
     try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_create = base_url + constants.frontend_create
        file_path = os.path.join( base_path , "config/frontend/create" , file_name)
        with open(file_path) as data_file:
                 json_config = json.load(data_file)
                 json_config["request"]["report"]["createdby"] = config.get("JSON_VARIABLE","createdBy")

        doc["config_file_name"] = file_path
        doc["config"] = json.dumps(json_config)
        doc["release"] = base_path
        doc["report_title"] = json_config["request"]["report"]["title"]
        response_type = ""
        value_check = query_mongo(file_path,json_config)
        if value_check == "create":
          #Api call
          response_api = requests.post(
                   url_frontend_create,
                   data= json.dumps(json_config),
                   headers=headers_api
                )

          doc['resp'] = response_api.text
          # Based on status concluding the logging output
          if response_api.status_code == constants.success_code or response_api.status_code == constants.success_code1:
              response_data = response_api.json()
              response_type = "crud"
              #   update the report status to live for program_dashboard reports
              if json_config['request']['report']['reportconfig']['report_type'] == "program_dashboard":
                  json_config['request']['report']["status"] = "live"
                  reportId = response_data["result"]["reportId"]
                  frontend_update(access_token,file_name,base_path,json_config,reportId)

          else:
              doc["errmsg"] = str(response_api.status_code)  + response_api.text
              response_type = "error"
          doc["api_response"] = response_api.json()
        elif value_check == "update":
           frontend_update(access_token,file_name,base_path)

        else :
            doc["operation"]= "frontend_create_duplicate_run"
            response_type = "duplicate_run"
        data_file.close
     except Exception as exception:
            doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
            response_type = "exception"

     insert_doc(doc,response_type)