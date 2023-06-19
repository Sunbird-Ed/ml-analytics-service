import sys,os
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/ml-analytics-service/config.ini")

script_path = config.get("REPORTS_FILEPATH","script_path")
sys.path.insert(0, script_path)

from get_token import *
from create import *
import constants


access_token = None
response_api = get_access_token()
if response_api["status_code"] == constants.success_code:
   access_token = response_api["result"]["access_token"]

backend_create_files = os.listdir(config.get("REPORTS_FILEPATH","folder_config") + "backend/create/")

frontend_create_files = os.listdir(config.get("REPORTS_FILEPATH","folder_config") + "frontend/create")


#calling create function for chart creation
for file in backend_create_files:
  backend_create(file)

#calling create function for report creation
if (access_token!= None):
    for file in frontend_create_files:
      frontend_create(access_token,file)

