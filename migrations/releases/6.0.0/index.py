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
try:
   backend_create_files = os.listdir( base_path + "/config/backend/create/")
except:
   backend_create_files = []

try:
   frontend_create_files = os.listdir(base_path + "/config/frontend/create")
except:
   frontend_create_files = []

# get the list of files to update 
try:
   frontend_update_files = os.listdir(base_path + "/config/frontend/update/")
except:
   frontend_update_files = []

# json file for retire 
try:
   backend_retire_reports = os.listdir(base_path + "/config/backend/retire/")
except:
   backend_retire_reports = []
try:
   frontend_retire_reports = os.listdir(base_path + "/config/frontend/retire/")
except:
   frontend_retire_reports = []

# STEP 1 
# call function to update tags of ml reports
print("================= Update Tag START =================")
# update_tag()
print("================= Update Tag END   =================")

# STEP 2 
# calling retire function for chart disabling    
print("================= Backend Retire START =================")
backend_retire(backend_retire_reports,base_path)
print("================= Backend Retire END  =================")

# STEP 3
#calling create function for chart creation
print("================= Backend Create START =================")
for file in backend_create_files:
  backend_create(file,base_path)

print("================= Backend Create END =================")


#calling create function for report creation
if (access_token!= None):

   # STEP 4
   # calling update function for report updation
   print("================= Frontend Update START =================")
   for file in frontend_update_files:
      frontend_update(access_token,file,base_path)

   print("================= Frontend Update END =================")

   # STEP 5 
   # calling create function for report creation   
   print("================= Frontend Create START =================")
   for file in frontend_create_files:
      frontend_create(access_token,file,base_path)
   print("================= Frontend Create END =================")

   