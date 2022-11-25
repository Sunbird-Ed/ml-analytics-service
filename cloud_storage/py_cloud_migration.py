# -----------------------------------------------------------------
# Name : py_cloud_migration.py
# Author : Ashwini E
# Description : migrating the cloud specific value to CNAME 
#  that supports all type of cloud, internally CNAME value will be changed
#  according to cloud 
# -----------------------------------------------------------------

import requests,json,os,sys,re,time
from configparser import ConfigParser,ExtendedInterpolation
import argparse

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

parser = argparse.ArgumentParser(description='Please enter a solution observation/survey')
parser.add_argument("--solution", choices={"observation", "survey"} ,required=True)
args = parser.parse_args()
solution_name = args.solution

sys.path.append(config.get("COMMON", "cloud_module_path"))
from cloud import MultiCloud
cloud_init = MultiCloud()

## Get values from Config 
urlQuery = config.get("DRUID","urlQuery")
header = {"Content-Type" : "application/json"}
querySpec = json.loads(config.get("DRUID",solution_name+"_query_spec"))
interval_arr = (config.get("DRUID","intervals")).replace('"','').strip('][').split(",")
file_path = config.get("OUTPUT_DIR",solution_name+"_druid_data") + "druidData.json"

for intr in interval_arr:
   print(intr)
   querySpec["intervals"]= intr
   ## Check the Existence of file and remove it    
   if os.path.exists(file_path):
      os.remove(file_path)

   ## Making an API request to druid
   responseQuery = requests.post(url=urlQuery,headers=header,data=json.dumps(querySpec))    
    
    ## When success get in 
   if responseQuery.status_code == 200 :
      responseDataArr = responseQuery.json()    
      ## looping inside response data from druid
      for druid_data in responseDataArr :
         
         for event_data in druid_data["events"]:                      
            ## Replacing value of a column
            if event_data["evidences"]:              
              url = re.sub(config.get("ML_SURVEY_SERVICE_URL","evidence_base_url"),'',event_data["evidences"])
              event_data["evidences"] = url
         
            ## Writing the data into json file
            with open(file_path,"a") as data:
               data.write(json.dumps(event_data))
               data.write('\n')

      if os.path.exists(file_path):  
         ##upload file to cloud
         cloud_init.upload_to_cloud(blob_Path = config.get("COMMON",solution_name+"_batch_ingestion_data_del"), local_Path = config.get("OUTPUT_DIR",solution_name+"_druid_data"), file_Name = "druidData.json")
      
         ## Re-ingest the modified data to druid
         druid_batch_end_point = config.get("DRUID", "batch_url")
         ingestion_spec = json.loads(config.get("DRUID",solution_name+"_injestion_spec"))         
    
         batch_task = requests.post(druid_batch_end_point, data=json.dumps(ingestion_spec), headers=header)
         datasource_name = ingestion_spec["spec"]["dataSchema"]["dataSource"]
         if batch_task.status_code == 200:
            print("started the batch ingestion task sucessfully for the datasource " + datasource_name)
   
         else:
            print("failed to start batch ingestion task of ml-project-status " + str(batch_task.status_code))
            print(batch_task.text)
         time.sleep(60)

   ## When failure printing the error     
   else :
        print(responseQuery)
        print(responseQuery.json())
