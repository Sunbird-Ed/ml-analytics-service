# -----------------------------------------------------------------
# Name : py_observation_evidence_streaming.py
# Author : Shakthieshwari.A
# Description : Extracts the Evidence or Files Attached at each question level 
#               during the observation submission
# -----------------------------------------------------------------
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, re
import datetime
from datetime import date, time
from configparser import ConfigParser, ExtendedInterpolation
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")
sys.path.append(config.get("COMMON", "cloud_module_path"))

from cloud import MultiCloud
cloud_init = MultiCloud()

# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'observation_streaming_evidence_success_error')
file_name_for_output_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-output.log"
file_name_for_debug_log = f"{file_path_for_output_and_debug_log}{formatted_current_date}-debug.log"

# Remove old log entries
files_with_date_pattern = [file 
for file in os.listdir(file_path_for_output_and_debug_log) 
if re.match(r"\d{2}-\w+-\d{4}-*", 
file)]

for file_name in files_with_date_pattern:
    file_path = os.path.join(file_path_for_output_and_debug_log, file_name)
    if os.path.isfile(file_path):
        file_date = file_name.split('.')[0]
        date = file_date.split('-')[0] + '-' + file_date.split('-')[1] + '-' + file_date.split('-')[2]
        if date < number_of_days_logs_kept:
            os.remove(file_path)


formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# Handler for output and debug Log
output_logHandler = RotatingFileHandler(f"{file_name_for_output_log}")
output_logHandler.setFormatter(formatter)

debug_logHandler = RotatingFileHandler(f"{file_name_for_debug_log}")
debug_logHandler.setFormatter(formatter)

# Add the successLoger
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}", when="w0",backupCount=1)
successLogger.addHandler(output_logHandler)
successLogger.addHandler(successBackuphandler)

# Add the Errorloger
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}",when="w0",backupCount=1)
errorLogger.addHandler(output_logHandler)
errorLogger.addHandler(errorBackuphandler)

# Add the Infologer
infoLogger = logging.getLogger('info log')
infoLogger.setLevel(logging.INFO)
debug_logBackuphandler = TimedRotatingFileHandler(f"{file_name_for_debug_log}",when="w0",backupCount=1)
infoLogger.addHandler(debug_logHandler)
infoLogger.addHandler(debug_logBackuphandler)

try:
  
  #db production
  clientdev = MongoClient(config.get('MONGO','url'))
  db = clientdev[config.get('MONGO','database_name')]
  obsSubCollec = db[config.get('MONGO','observation_sub_collection')]
  quesCollec = db[config.get('MONGO','questions_collection')]
except Exception as e:
  errorLogger.error(e, exc_info=True)

try :
  def convert(lst): 
    return ','.join(lst)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def evidence_extraction(msg_id):
    for obSub in obsSubCollec.find({'_id':ObjectId(msg_id)}):
     if 'isAPrivateProgram' in obSub :
      successLogger.debug("Observation Evidence Submission Id : " + str(msg_id))
      try:
        completedDate = str(obSub['completedDate'])
      except KeyError:
        completedDate = '' 
      evidence_sub_count = 0

      try:
        answersArr = [ v for v in obSub['answers'].values()]
      except KeyError:
        pass

      for ans in answersArr:
        try:
          if len(ans['fileName']):
            evidence_sub_count = evidence_sub_count + len(ans['fileName'])
        except KeyError:
          if len(ans['instanceFileName']):
            for instance in ans['instanceFileName']:
              evidence_sub_count = evidence_sub_count + len(instance)

    if completedDate :
      for answer in answersArr:
       try:
        if answer['qid']:
          observationSubQuestionsObj = {}
          observationSubQuestionsObj['completedDate'] = completedDate
          observationSubQuestionsObj['total_evidences'] = evidence_sub_count
          observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
          observationSubQuestionsObj['entity'] = str(obSub['entityId'])
          observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
          observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name']
          observationSubQuestionsObj['entityType'] = str(obSub['entityType'])
          observationSubQuestionsObj['createdBy'] = obSub['createdBy']
          observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
          observationSubQuestionsObj['solutionId'] = str(obSub['solutionId'])
          observationSubQuestionsObj['observationId'] = str(obSub['observationId'])

          try :
            observationSubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
          except KeyError :
            observationSubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")

          fileName = []
          fileSourcePath = []

          try:
            observationSubQuestionsObj['remarks'] = answer['remarks']
          except KeyError:
            observationSubQuestionsObj['remarks'] = ''
          observationSubQuestionsObj['questionId'] = str(answer['qid'])

          questionsCollec = quesCollec.find({'_id':ObjectId(observationSubQuestionsObj['questionId'])})
          for ques in questionsCollec:
            observationSubQuestionsObj['questionExternalId'] = ques['externalId']
            observationSubQuestionsObj['questionName'] = ques["question"][0] 
          observationSubQuestionsObj['questionResponseType'] = answer['responseType']
          evidence = []
          evidenceCount = 0

          try:
            if answer['fileName']:
              evidence = answer['fileName']
              observationSubQuestionsObj['evidence_count'] = len(evidence)
              evidenceCount = len(evidence)
          except KeyError:
            if answer['instanceFileName']:
              for inst in answer['instanceFileName']:
                evidence.extend(inst)
              observationSubQuestionsObj['evidence_count'] = len(evidence)
              evidenceCount = len(evidence)

          for evi in evidence:
            fileName.append(evi['name'])
            fileSourcePath.append(evi['sourcePath'])
          observationSubQuestionsObj['fileName'] = convert(fileName)
          observationSubQuestionsObj['fileSourcePath'] = convert(fileSourcePath)
          if evidenceCount > 0:
            json.dump(observationSubQuestionsObj, f)
            f.write("\n")
            successLogger.debug("Send Obj to Cloud")
       except KeyError:
        pass
except Exception as e:
  errorLogger.error(e, exc_info=True)

with open('sl_observation_evidence.json', 'w') as f:
 for msg_data in obsSubCollec.find({"status":"completed"}):
    obj_arr = evidence_extraction(msg_data['_id'])

local_path = config.get("OUTPUT_DIR", "observation")
blob_path = config.get("COMMON", "observation_evidevce_blob_path")

for files in os.listdir(local_path):
    if "sl_observation_evidence.json" in files:
      cloud_init.upload_to_cloud(blob_Path = blob_path, local_Path = local_path, file_Name = files)
        
payload = {}
payload = json.loads(config.get("DRUID","observation_evidence_injestion_spec"))
datasource = [payload["spec"]["dataSchema"]["dataSource"]]
ingestion_spec = [json.dumps(payload)]       
for i, j in zip(datasource,ingestion_spec):
    druid_end_point = config.get("DRUID", "metadata_url") + i
    druid_batch_end_point = config.get("DRUID", "batch_url")
    headers = {'Content-Type' : 'application/json'}
    get_timestamp = requests.get(druid_end_point, headers=headers)
    if get_timestamp.status_code == 200:
        successLogger.debug("Successfully fetched time stamp of the datasource " + i )
        timestamp = get_timestamp.json()
        #calculating interval from druid get api
        minTime = timestamp["segments"]["minTime"]
        maxTime = timestamp["segments"]["maxTime"]
        min1 = datetime.datetime.strptime(minTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        max1 = datetime.datetime.strptime(maxTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        new_format = "%Y-%m-%d"
        min1.strftime(new_format)
        max1.strftime(new_format)
        minmonth = "{:02d}".format(min1.month)
        maxmonth = "{:02d}".format(max1.month)
        min2 = str(min1.year) + "-" + minmonth + "-" + str(min1.day)
        max2 = str(max1.year) + "-" + maxmonth  + "-" + str(max1.day)
        interval = min2 + "_" + max2
        time.sleep(50)

        disable_datasource = requests.delete(druid_end_point, headers=headers)

        if disable_datasource.status_code == 200:
            successLogger.debug("successfully disabled the datasource " + i)
            time.sleep(300)
          
            delete_segments = requests.delete(
                druid_end_point + "/intervals/" + interval, headers=headers
            )
            if delete_segments.status_code == 200:
                successLogger.debug("successfully deleted the segments " + i)
                time.sleep(300)

                enable_datasource = requests.get(druid_end_point, headers=headers)
                if enable_datasource.status_code == 204:
                    successLogger.debug("successfully enabled the datasource " + i)
                    
                    time.sleep(300)

                    start_supervisor = requests.post(
                        druid_batch_end_point, data=j, headers=headers
                    )
                    successLogger.debug("ingest data")
                    if start_supervisor.status_code == 200:
                        successLogger.debug(
                            "started the batch ingestion task sucessfully for the datasource " + i
                        )
                        time.sleep(50)
                    else:
                        errorLogger.error(
                            "failed to start batch ingestion task" + str(start_supervisor.status_code)
                        )
                else:
                    errorLogger.error("failed to enable the datasource " + i)
            else:
                errorLogger.error("failed to delete the segments of the datasource " + i)
        else:
            errorLogger.error("failed to disable the datasource " + i)

    elif get_timestamp.status_code == 204:
        start_supervisor = requests.post(
            druid_batch_end_point, data=j, headers=headers
        )
        if start_supervisor.status_code == 200:
            successLogger.debug(
                "started the batch ingestion task sucessfully for the datasource " + i
            )
            time.sleep(50)
        else:
            errorLogger.error(start_supervisor.text)
            errorLogger.error(
                "failed to start batch ingestion task" + str(start_supervisor.status_code)
            )            


