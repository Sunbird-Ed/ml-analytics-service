import requests
import os, json,sys,csv,json
from configparser import ConfigParser,ExtendedInterpolation
from update import fetchAllReports
import constants

# Read the Config
root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(root_path + "config.ini")

sys.path.insert(0, root_path + "migrations/lib")

from mongo_log import *

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': constants.content_type,
        'Authorization' : config.get("API_HEADERS","authorization_access_token")
    }


# hit the api to retire the frontend reports 
def frontend_retire(access_token,fileNames,base_path):
    reportsLookUp = fetchAllReports(access_token)
    
    headers_api["x-authenticated-user-token"] = access_token

    try:

        for fileName in fileNames:

            # reports file path 
            frontend_retire_reports = os.path.join(base_path,"config/frontend/retire",fileName)

            doc = {
                    "operation": "frontend_retire",
                    "file" : str(frontend_retire_reports)
                }
            
            # check if the file is already executed or not 
            if query_retire(frontend_retire_reports):
                # load data from json file
                with open(frontend_retire_reports) as json_file:
                   frontend_retire_reports_list = json.load(json_file)
                   frontend_retire_reports_list = frontend_retire_reports_list['reports']
                json_file.close

                typeErr = ""

                for tag in frontend_retire_reports_list:
                    url_frontend_retire = base_url + constants.frontend_retire + str(reportsLookUp[tag])
                    response_api = requests.delete(
                               url_frontend_retire,
                               headers=headers_api
                            )

                    doc["reportId"] = str(reportsLookUp[tag])

                    if response_api.status_code == constants.success_code:
                        typeErr = "crud"
                    else:
                        doc["errmsg"] = str(response_api.status_code)  + response_api.text
                        typeErr = "error"
            else:
                doc["operation"]= "frontend_retire_duplicate_run"
                typeErr = "duplicate_run"
        
            insert_doc(doc,typeErr)
    except Exception as exception:
        print(exception)
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"
        insert_doc(doc,typeErr)


# hit the api to retire the backend reports 
def backend_retire(fileNames,base_path):
    
    try:


        for fileName in fileNames:
            # reports file path 
            backend_retire_reports = os.path.join(base_path,"config/backend/retire",fileName)
            doc = {
                    "operation": "backend_retire",
                    "file" : str(backend_retire_reports)
                }
            if query_retire(backend_retire_reports):
                with open(backend_retire_reports) as json_file:
                   backend_retire_reports_list = json.load(json_file)
                   backend_retire_reports_list = backend_retire_reports_list['reports']
                json_file.close

                for reportId in backend_retire_reports_list:
                    doc['reportId'] = reportId
                    url_backend_retire = base_url + constants.backend_retire + str(reportId)
                    response_api = requests.patch(
                        url_backend_retire,
                        headers=headers_api
                        )
    
                    if response_api.status_code == constants.success_code:
                        typeErr = "crud"
                    else:
                        doc["errmsg"] = str(response_api.status_code)  + response_api.text
                        typeErr = "error"
    
                
            else:
                doc["operation"]= "backend_retire_duplicate_run"
                typeErr = "duplicate_run"
        
            insert_doc(doc,typeErr)
    
    except Exception as exception:
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"
        insert_doc(doc,typeErr)