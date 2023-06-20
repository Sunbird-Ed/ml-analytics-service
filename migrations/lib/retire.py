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
def frontend_retire(access_token,tag):
    reportsLookUp = fetchAllReports()

    doc = {
                "reportTag" : tag,
                "operation": "frontEnd_retire"
            }

    headers_api["x-authenticated-user-token"] = access_token

    try:
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
    
    except Exception as exception:
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"

    insert_doc(doc,typeErr)


# hit the api to retire the backend reports 
def backend_retire(reportId):
    doc = {
                "reportId" : reportId,
                "operation": "backEnd_retire"
            }
    
    try:
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
    except Exception as exception:
        doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
        typeErr = "exception"

    insert_doc(doc,typeErr)