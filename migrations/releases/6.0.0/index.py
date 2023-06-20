import sys,os,json
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("/opt/sparkjobs/ml-analytics-service/config.ini")

script_path = config.get("REPORTS_FILEPATH","script_path")
sys.path.insert(0, script_path)

from get_token import *
from create import *
from update import *
from retire import *
import constants

access_token = None
response_api = get_access_token()
if response_api["status_code"] == constants.success_code:
   access_token = response_api["result"]["access_token"]

base_path = os.getcwd()
backend_create_files = os.listdir( base_path + "/config/backend/create/")
frontend_create_files = os.listdir(base_path + "/config/frontend/create")

# json file for retire 
backend_retire_reports = os.listdir(base_path + "/config/backend/retire/reports.json")
frontend_retire_reports = os.listdir(base_path + "/config/frontend/retire/reports.json")


# load data from json file 
with open(backend_retire_reports) as json_file:
   backend_retire_reports_list = json.load(json_file)
   backend_retire_reports_list = backend_retire_reports_list['reports']
json_file.close

# load data from json file
with open(frontend_retire_reports) as json_file:
   frontend_retire_reports_list = json.load(json_file)
   frontend_retire_reports_list = frontend_retire_reports_list['reports']
json_file.close

#calling create function for chart creation
for file in backend_create_files:
  backend_create(file,base_path)

#calling create function for report creation
if (access_token!= None):
   # calling retire function for report disabling    
   for file in frontend_retire_reports_list:
      frontend_retire(access_token,file)
   
   #calling create function for report creation   
   for file in frontend_create_files:
      frontend_create(access_token,file,base_path)

   # calling update function for report updation
   for file in frontend_update_files:
      frontend_update(access_token,file)
