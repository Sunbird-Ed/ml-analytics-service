import sys,os,json
from configparser import ConfigParser,ExtendedInterpolation

# Read the Config
root_path = "/opt/sparkjobs/ml-analytics-service/"
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(root_path + "config.ini")


base_path = os.getcwd()

sys.path.insert(0, root_path + "migrations/lib")


from get_token import get_access_token
from create import backend_create,frontend_create
from update import backend_update,frontend_update
from retire import backend_retire,frontend_retire
from update_tag import update_tag
import constants

access_token = None
response_api = get_access_token()
if response_api["status_code"] == constants.success_code:
   access_token = response_api["result"]["access_token"]

# get the list of files to create 
backend_create_files = os.listdir( base_path + "/config/backend/create/")
frontend_create_files = os.listdir(base_path + "/config/frontend/create")

# get the list of files to update 
backend_update_files = os.listdir( base_path + "/config/backend/update/")
frontend_update_files = os.listdir(base_path + "/config/frontend/update/")


# json file for retire 
backend_retire_reports = os.listdir(base_path + "/config/backend/retire/")
frontend_retire_reports = os.listdir(base_path + "/config/frontend/retire/")


# call function to update tags of ml reports
update_tag()

# calling retire function for chart disabling    
backend_retire(backend_retire_reports,base_path)


#calling create function for chart creation
for file in backend_create_files:
  backend_create(file,base_path)

#calling update function for chart updation
for file in backend_update_files:
   backend_update(file,base_path)

#calling create function for report creation
if (access_token!= None):
   
   # #calling create function for report creation   
   for file in frontend_create_files:
      frontend_create(access_token,file,base_path)
   
   # calling retire function for report disabling    
   frontend_retire(access_token,frontend_retire_reports,base_path)

   # calling update function for report updation
   for file in frontend_update_files:
      frontend_update(access_token,file,base_path)
