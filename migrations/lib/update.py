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
import constants

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': constants.content_type,
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }

folder_path = config.get("REPORTS_FILEPATH","folder_config")

reportsLookUp = {}

# Fetch all reports list and return tag and id 
def fetchAllReports():
    doc = {
            "operation": "fetch_all_reports"
        }
    try:
        returnValue = {}
        url_reports_list = base_url + constants.reports_list
        json_body = {
            'request' : {
                'filters' :{}
            }
        }
        #Api call
        response_api = requests.post(
                    url_reports_list,
                    data= json.dumps(json_body),
                    headers=headers_api
                )
        
        # Based on status concluding logging the output
        if response_api.status_code == constants.success_code:
           typeErr = "crud"
           response_data = response_api.json()
           response_data = response_data['result']['reports']
           for eachReports in range(len(response_data)):
               returnValue[response_data[eachReports]['tags'][0]] = response_data[eachReports]['reportid']
        else:
            doc["errmsg"] = "Status Code : " + str(response_api.status_code) + " , Error Message : " + str(response_api.text)
            typeErr = "error"    
            
        return returnValue    
    except Exception as exception :
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"
    insert_doc(doc,typeErr)


# Update chart using Json config making an API call
def backend_update(file_name):
    doc = {}
    try :

        # remove .json from filename
        fileName_without_extension = file_name.split(".json")[0]

        url_backend_update = base_url + constants.backend_update + str(fileName_without_extension)
        file_path = folder_path + "backend/update/" + file_name
        
        with open(file_path) as data_file:
            json_config = json.load(data_file)
            json_config["request"]["createdBy"] = config.get("JSON_VARIABLE","createdBy")
            json_config["request"]["config"]["container"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["store"] = config.get("JSON_VARIABLE","store")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["postContainer"] = config.get("JSON_VARIABLE","container")
            json_config["request"]["config"]["reportConfig"]["mergeConfig"]["container"] = config.get("JSON_VARIABLE","container")
            doc = {
                    "configFileName" : file_name,
                    "config" : json.dumps(json_config),
                    "operation": "backEnd_update"
                  }
        #Api call
        response_api = requests.post(
                url_backend_update,
                data= json.dumps(json_config),
                headers=headers_api
            )
        # Based on status concluding logging the output
        if response_api.status_code == constants.success_code:
            typeErr = "crud"
        else:
           doc["errmsg"] = str(response_api.status_code)  + response_api.text
           typeErr = "error"
        data_file.close
    except Exception as exception:
           doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
           typeErr = "exception"
    
    insert_doc(doc,typeErr)
    

# Creation of report using Json config making an API call
def frontend_update(access_token,file_name):
    doc = {}
    try :
        reportsLookUp = fetchAllReports()

        # remove .json from filename
        fileName_without_extension = file_name.split(".json")[0]
        
        headers_api["x-authenticated-user-token"] = access_token
        
        url_frontend_update = base_url + constants.frontend_update + str(reportsLookUp[fileName_without_extension])
        file_path = folder_path + "frontend/update/" + file_name
        
        with open(file_path) as data_file:
                 json_config = json.load(data_file)
                 json_config.update({
                     "request" : {
                         "report" : {
                             "createdby" : config.get("JSON_VARIABLE","createdBy")
                         }
                     }
                 })

        doc = {
                  "configFileName" : file_name,
                  "config" : json.dumps(json_config),
                  "operation": "frontEnd_update"
               }

        response_api = requests.patch(
                   url_frontend_update,
                   data= json.dumps(json_config),
                   headers=headers_api
                )
        
        if response_api.status_code == constants.success_code:
            typeErr = "crud"
        else:
            doc["errmsg"] = str(response_api.status_code)  + response_api.text
            typeErr = "error"
        
        data_file.close
        

    except Exception as exception:
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"

    insert_doc(doc,typeErr)   


def frontend_update_with_reportId(access_token,reportJson,reportId):
    headers_api["x-authenticated-user-token"] = access_token
    
    url_frontend_update = base_url + constants.frontend_update + str(reportId)
    doc = {
                  "reportId" : reportId,
                  "config" : json.dumps(reportJson),
                  "operation": "frontend_status_update"
            }
    response_api = requests.patch(
                   url_frontend_update,
                   data= json.dumps(reportJson),
                   headers=headers_api
                )
    if response_api.status_code == constants.success_code:
        typeErr = "crud"
    else:
        doc["errmsg"] = str(response_api.status_code)  + response_api.text
        typeErr = "error"
        
    insert_doc(doc,typeErr)