import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation

sys.path.insert(0, '/opt/sparkjobs/ml-analytics-service/reports_automation/')
from mongo_logging import insert_doc

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

# Creation of chart using Json config making an API call
def backEnd_create(chart_id):
     try :
        url_backend_create = base_url + config.get("API_ENDPOINTS","backend_create")
        file_path = config.get("REPORTS_FILEPATH","base_path") + "release_6.0.0/backEndConfig/" + chart_id +".json"
        with open(file_path) as data_file:
            json_config = json.load(data_file)
            json_config["request"]["createdBy"] = config.get("JSON_VARIABLE","createdBy")
            json_config["request"]["config"]["container"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["store"] = config.get("JSON_VARIABLE","store")
            json_config["request"]["config"]["key"] = config.get("JSON_VARIABLE","key")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["postContainer"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["container"] = config.get("JSON_VARIABLE","container")

            doc = {
                    "configFileName" : chart_id+ ".json",
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
           print("Chart Created successfully")
           type = "crud"
        else:
           print("Chart Creation Failed")
           doc["errmsg"] = str(response_api.status_code)  + response_api.text
           type = "error"

     except Exception as exception:
             doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
             type = "exception"

     insert_doc(doc,type)
     print(doc)

# Creation of report using Json config making an API call
def frontEnd_create(access_token,chart_id):
     try :
        headers_api["x-authenticated-user-token"] = access_token
        url_frontend_create = base_url + config.get("API_ENDPOINTS","frontend_create")
        file_path = config.get("REPORTS_FILEPATH","base_path") + "release_6.0.0/frontEndConfig/" + chart_id +".json"
        with open(file_path) as data_file:
                 json_config = json.load(data_file)
                 json_config["request"]["report"]["createdby"] = config.get("JSON_VARIABLE","createdBy")

        doc = {
                 "configFileName" : chart_id + ".json",
                 "config" : json.dumps(json_config),
                 "operation": "frontEnd_create"
                 }

        #Api call
        response_api = requests.post(
                url_frontend_create,
                data= json.dumps(json_config),
                headers=headers_api
            )

        # Based on status concluding the logging output
        if response_api.status_code == 200 or response_api.status_code == 201:
           print("Report Created Successfully")
           type = "crud"
        else:
           print("Report Creation Failed")
           doc["errmsg"] = str(response_api.status_code)  + response_api.text
           type = "error"

     except Exception as exception:
            doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
            type = "exception"

     insert_doc(doc,type)
     print(doc)

