# -----------------------------------------------------------------
# Name : py_survey_evidence_streaming.py
# Author :
# Description :
#
# -----------------------------------------------------------------

from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json
import datetime
from datetime import date,time
import requests
import dateutil
from dateutil import parser as date_parser
from configparser import ConfigParser, ExtendedInterpolation
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler
from azure.storage.blob import BlockBlobService


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_success')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_success'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_error')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_error'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    
    #db production
    client = MongoClient(config.get('MONGO', 'mongo_url'))
    db = client[config.get('MONGO', 'database_name')]
    surveySubmissionsCollec = db[config.get('MONGO', 'survey_submissions_collection')]
    questionsCollec = db[config.get('MONGO', 'questions_collection')]

except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def convert(lst): 
        return ','.join(lst)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def evidence_extraction(msg_id):
        for obSub in surveySubmissionsCollec.find({'_id':ObjectId(msg_id)}):
         if 'isAPrivateProgram' in obSub :
            successLogger.debug("Survey Evidence Submission Id : " + str(msg_id))
            try:
                completedDate = str(obSub['completedDate'])
            except KeyError:
                pass
            evidence_sub_count = 0
            try:
                answersArr = [ v for v in obSub['answers'].values()]
            except KeyError:
                pass
            for ans in answersArr:
                try:
                    if len(ans['fileName']):
                        evidence_sub_count   = evidence_sub_count + len(ans['fileName'])
                except KeyError:
                    if len(ans['instanceFileName']):
                        for instance in ans['instanceFileName']:
                            evidence_sub_count   = evidence_sub_count + len(instance)
            for answer in answersArr:
                surveySubQuestionsObj = {}
                surveySubQuestionsObj['completedDate'] = completedDate
                surveySubQuestionsObj['total_evidences'] = evidence_sub_count
                surveySubQuestionsObj['surveySubmissionId'] = str(obSub['_id'])
                surveySubQuestionsObj['createdBy'] = obSub['createdBy']
                surveySubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
                surveySubQuestionsObj['solutionId'] = str(obSub['solutionId'])
                surveySubQuestionsObj['surveyId'] = str(obSub['surveyId'])
                fileName = []
                fileSourcePath = []
                try:
                    surveySubQuestionsObj['remarks'] = answer['remarks']
                except KeyError:
                    surveySubQuestionsObj['remarks'] = ''
                surveySubQuestionsObj['questionId'] = str(answer['qid'])
                for ques in questionsCollec.find({'_id':ObjectId(surveySubQuestionsObj['questionId'])}):
                    surveySubQuestionsObj['questionExternalId'] = ques['externalId']
                    surveySubQuestionsObj['questionName'] = ques['question'][0]
                surveySubQuestionsObj['questionResponseType'] = answer['responseType']
                try:
                    surveySubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
                except KeyError :
                    surveySubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")
                evidence = []
                evidenceCount = 0
                try:
                    if answer['fileName']:
                        evidence = answer['fileName']
                        surveySubQuestionsObj['evidence_count'] = len(evidence)
                        evidenceCount = len(evidence)
                except KeyError:
                    if answer['instanceFileName']:
                        for inst in answer['instanceFileName'] :
                            evidence.extend(inst)
                        surveySubQuestionsObj['evidence_count'] = len(evidence)
                        evidenceCount = len(evidence)
                for evi in evidence:
                    fileName.append(evi['name'])
                    fileSourcePath.append(evi['sourcePath'])
                surveySubQuestionsObj['fileName'] = convert(fileName)
                surveySubQuestionsObj['fileSourcePath'] = convert(fileSourcePath)
                if evidenceCount > 0:
                    json.dump(surveySubQuestionsObj, f)
                    f.write("\n")
                    successLogger.debug("Send Obj to Azure")
except Exception as e:
    errorLogger.error(e, exc_info=True)
with open('sl_survey_evidence.json', 'w') as f:
 for msg_data in surveySubmissionsCollec.find({"status":"completed"}):
    evidence_extraction(msg_data['_id'])
    
blob_service_client = BlockBlobService(
    account_name=config.get("AZURE", "account_name"),
    sas_token=config.get("AZURE", "sas_token")
)
container_name = config.get("AZURE", "container_name")
local_path = config.get("OUTPUT_DIR", "survey")
blob_path = config.get("AZURE", "survey_evidevce_blob_path")

for files in os.listdir(local_path):
    if "sl_survey_evidence.json" in files:
        blob_service_client.create_blob_from_path(
            container_name,
            os.path.join(blob_path,files),
            local_path + "/" + files
        )    
        
payload = {}
payload = json.loads(config.get("DRUID","survey_evidence_injestion_spec"))
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


