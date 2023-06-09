import requests
import os, json, sys
from configparser import ConfigParser,ExtendedInterpolation

sys.path.insert(0, '/opt/sparkjobs/ml-analytics-service/reports_automation/')
from mongo_logging import insert_doc

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/report_config.ini")

base_url = config.get("API_ENDPOINTS","base_url")

headers_api = {
        'Content-Type': config.get("API_HEADERS", "content_type_url")
    }


def get_refresh_token():
    try:
        response_json = {}
        url_refresh = base_url + config.get("API_ENDPOINTS", "refresh_token")

        refresh_payload = {
            'client_id': config.get("API_CREDENTIALS", "client_id"),
            'client_secret': config.get("API_CREDENTIALS", "client_secret"),
            'grant_type': config.get("API_CREDENTIALS", "grant_type"),
            'username': config.get("API_CREDENTIALS", "username"),
            'password': config.get("API_CREDENTIALS", "password")
        }

        response_api = requests.post(
            url_refresh,
            data=refresh_payload,
            headers=headers_api
        )
        response_json = response_api.json()
        response_json["status_code"] = response_api.status_code
        return response_json
    except Exception as exception:
        doc = {
            "operation": "refresh_token",
            "errmsg": "Exception message {}: {}".format(type(exception).__name__, exception),
            "status": "failed"
        }
        insert_doc(doc)
        print(doc)



def get_access_token():
    try :
         url_access = base_url + config.get("API_ENDPOINTS", "access_token")
         response_api = get_refresh_token()
         if response_api["status_code"] == 200:
            print("Refresh API Success")
            headers_api["Authorization"] = config.get("API_HEADERS","authorization_access_token")
            access_payload = {
                  'refresh_token' : response_api["refresh_token"]
             }

            access_get = requests.post(
                  url_access,
                  data= access_payload,
                  headers=headers_api
              )

            access_json = access_get.json()
            access_json["status_code"] = access_get.status_code
            print(access_json)
            return access_json

         else:
            print("Refresh API Failed")
            print(response_api)
            return response_api
    except Exception as exception:
         doc = {"operation":"Access_token","errmsg": "Exception message {}".format(exception) + " " +format(type(exception).__name__), "status":"failed"}
         insert_doc(doc)

# get_access_token()











