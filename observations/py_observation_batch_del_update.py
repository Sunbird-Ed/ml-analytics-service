import requests,json,os,sys,csv
from configparser import ConfigParser,ExtendedInterpolation

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
sys.path.append(config.get("COMMON", "cloud_module_path"))

from cloud import MultiCloud


cloud_init = MultiCloud()

subId = []
with open(config.get("OUTPUT_DIR","observation_sub_ids")) as csv_file:
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        subId.append(row[0])
    csv_file.close()


## Get values from Config 
urlQuery = config.get("DRUID","urlQuery")
header = {"Content-Type" : "application/json"}
querySpec = json.loads(config.get("DRUID","observation_query_spec"))
querySpec["filter"]= {"type":"in","dimension":"observationSubmissionId","values":subId} 


## Length of IDS in Query
print(len(subId))

## Check the Existence of file and remove it
file_path = config.get("OUTPUT_DIR","observation_druid_data") + "druidData.json"
if os.path.exists(file_path):
   os.remove(file_path)

## Making an API reuest to druid
responseQuery = requests.post(url=urlQuery,headers=header,data=json.dumps(querySpec))

## When success get in 
if responseQuery.status_code == 200 :
    responseDataArr = responseQuery.json()
    print(len(responseDataArr))
    cntr = 1
    ## looping inside response data from druid
    for druid_data in responseDataArr : 
           
      for event_data in druid_data["events"]:          
         ## Addition of key value      
         event_data["isSubmissionDeleted"] = "true"
         print(cntr,event_data["observationSubmissionId"])

         ## Writing the data into json file
         with open(file_path,"a") as data:
            data.write(json.dumps(event_data))
            data.write('\n')
            cntr = cntr + 1  

## When failure printing the error     
else :
  print(responseQuery)
  print(responseQuery.json())


local_path = config.get("OUTPUT_DIR", "observation_druid_data")
blob_path = config.get("COMMON", "observation_batch_ingestion_data_del")

for files in os.listdir(local_path):
    if "druidData.json" in files:
        cloud_init.upload_to_cloud(blob_Path = blob_path, local_Path = local_path, file_Name = files)
    else:
        print("file not found "+local_path)

## Re-ingest the modified data to druid
druid_batch_end_point = config.get("DRUID", "batch_url")
observation_spec = json.loads(config.get("DRUID","observation_injestion_spec"))


batch_task = requests.post(druid_batch_end_point, data=json.dumps(observation_spec), headers=header)
datasource_name = observation_spec["spec"]["dataSchema"]["dataSource"]
if batch_task.status_code == 200:
   print("started the batch ingestion task sucessfully for the datasource " + datasource_name)
   
else:
   print("failed to start batch ingestion task of ml-project-status " + str(batch_task.status_code))
   print(batch_task.text)
