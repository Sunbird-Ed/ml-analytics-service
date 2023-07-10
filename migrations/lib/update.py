import requests
import os, json,sys
from configparser import ConfigParser,ExtendedInterpolation


# Read the Config
root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(root_path + "config.ini")

sys.path.insert(0, root_path + "migrations/lib")

from mongo_log import *
import constants

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': constants.content_type,
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }


reportsLookUp = {}

# Fetch all reports list and return tag and id 
def fetchAllReports(access_token):
    docFetchAll = {
            "operation": "fetch_all_reports"
        }
    try:
        headers_api["x-authenticated-user-token"] = access_token
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
           
           print("---> Frontend reports fetched.")
        else:
            docFetchAll["errmsg"] = "Status Code : " + str(response_api.status_code) + " , Error Message : " + str(response_api.text)

            print("<--- Frontend reports fetch failed.")
            print("Status Code : " + str(response_api.status_code) + " , Error Message : " + str(response_api.text))

            typeErr = "error"    
            
        return returnValue    
    except Exception as exception :
        print("<--- fetch all reports failed")
        print("Exception message {}: {}".format(type(exception).__name__, exception))
        docFetchAll["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"
    insert_doc(docFetchAll,typeErr)


# Update chart using Json config making an API call
def backend_update(file_name,base_path):
    doc = {}
    file_path = base_path + "/config/backend/update/" + file_name
    try :

        # remove .json from filename
        fileName_without_extension = file_name.split(".json")[0]

        url_backend_update = base_url + constants.backend_update + str(fileName_without_extension)
        
    
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
                    "operation": "backEnd_update",
                    "file_path" : file_path
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
           doc['file_path'] = file_path
           doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
           print("<--- backEnd_update failed")
           print("Exception message {}: {}".format(type(exception).__name__, exception))
           typeErr = "exception"
    
    insert_doc(doc,typeErr)
    

# Creation of report using Json config making an API call
def frontend_update(access_token,file_name,base_path,reportJson=False,reportId=False):
    global reportsLookUp
    file_path = os.path.join( base_path , "config/frontend/update" , file_name)
    docUpdate = {}
    proceedFlag = False
    typeErr = ""
    headers_api["x-authenticated-user-token"] = access_token
    
    if reportJson and reportId:
        proceedFlag = True
        url_frontend_update = base_url + constants.frontend_update + str(reportId)
        json_config = reportJson
        print("---> program_dashboard report detected")
        docUpdate = {
            "reportId" : reportId,
            "configFileName" : file_name,
            "config" : json.dumps(reportJson),
            "operation": "frontend_status_update"
        }
    else:
        try :
            docUpdate = {
                      "configFileName" : file_name,
                      "operation": "frontend_update"
                   }
            if len(reportsLookUp) == 0:
                reportsLookUp = fetchAllReports(access_token)

            # remove .json from filename
            fileName_without_extension = file_name.split(".json")[0]

            
            
            with open(file_path) as data_file:
                json_config = json.load(data_file)
                json_config["request"]["report"]["createdby"] = config.get("JSON_VARIABLE","createdBy")
                report_id = reportsLookUp[json_config["request"]["report"]["tags"][0]]

            url_frontend_update = base_url + constants.frontend_update + str(report_id)
            reportId = reportsLookUp[fileName_without_extension]

            docUpdate['config'] = json.dumps(json_config)

            data_file.close  
            proceedFlag = True

        except Exception as exception:
            docUpdate["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
            print("<--- frontend update failed.")
            print("Exception message {}: {}".format(type(exception).__name__, exception))
            docUpdate["filePath"] = file_path
            typeErr = "exception"
            proceedFlag = False

    if proceedFlag :
        try:
            response_api = requests.patch(
                               url_frontend_update,
                               data= json.dumps(json_config),
                               headers=headers_api
                            )
            
            if response_api.status_code == constants.success_code:
                typeErr = "crud"
                print("---> frontend_update Success : " + str(reportId))
            else:
                docUpdate["errmsg"] = str(response_api.status_code)  + response_api.text
                print("<--- frontend_update Failed : " + str(reportId))
                print("Error code : " + str(response_api.status_code))
                print("Error Message : " + str(response_api.text))
                typeErr = "error"
        except Exception as exception:
                docUpdate["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
                print("<--- frontend_update Failed : " + str(reportId))
                print( "Exception message {}: {}".format(type(exception).__name__, exception))
                typeErr = "exception"

    docUpdate["configFileName"] = file_name
    insert_doc(docUpdate,typeErr)
