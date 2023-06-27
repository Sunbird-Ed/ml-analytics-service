import requests ,os, json,sys,re
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(root_path + "config.ini")

sys.path.insert(0, root_path + "migrations/lib")

from mongo_log import *
from get_token import get_access_token
import constants

response_api = get_access_token()
if response_api["status_code"] == constants.success_code:
   access_token = response_api["result"]["access_token"]

# Required field gathering for API
base_url = config.get("API_ENDPOINTS","base_url")
headers_api = {
        'Content-Type': constants.content_type,
        'Authorization' : config.get("API_HEADERS","authorization_access_token"),
        'x-authenticated-user-token' : access_token
    }


def get_report(report_ids):
   doc = {
            "operation": "get_report"
           }
   try :
         for id in report_ids:
            url_frontend_get = base_url + constants.frontend_get + id
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
        url_frontend_update = base_url + constants.frontend_update + id
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

# report_ids = config.get("REPORT_IDS","update_tag").split(',')
# get_report(report_ids)

def fetch_ml_reports():
    doc = {
            "operation": "get_report"
           }
    try:
        # regular expression to find all report ids starting with keyword ml 
        regex =  r'^ml_'
        reportIds = []
        reports_to_update = {}
        url_frontend_get_all = base_url + constants.reports_list
        json_body = {
            'request' : {
                'filters' :{}
            }
        }
        response_api = requests.post(
                                 url_frontend_get_all,
                                 data= json.dumps(json_body),
                                 headers=headers_api
                               )
        doc["api"] = url_frontend_get_all
        if response_api.status_code == constants.success_code:
            response_type = "crud"
            all_reports = response_api.json()
            all_reports = all_reports["result"]["reports"]
            # iterate through the result and find the report id and config of reports which starts with ml 
            for index in all_reports:
                if 'dataSource' in index['reportconfig'].keys():
                    dataSource = index['reportconfig']['dataSource']
                    if isinstance(dataSource, list):
                        for eachDS in dataSource:
                            if re.match(regex, eachDS['id']):
                                if index["reportid"] not in reportIds:
                                    reportIds.append(index["reportid"])
                                    reports_to_update[index["reportid"]] = index
            
            
            return reports_to_update

        else:
            doc["errmsg"] = str(response_api.status_code)  + response_api.text
            response_type = "error"
                   


    except Exception as exception:
         doc["errmsg"] = "Exception message {}: {}".format(type(exception).__name__, exception)
         response_type = "exception"
         
    insert_doc(doc,response_type)



def update_tag():
    fetchReports = fetch_ml_reports()
    for reportId,jsonConfig in fetchReports.items():
        print(reportId)
        update_report(jsonConfig,reportId)
