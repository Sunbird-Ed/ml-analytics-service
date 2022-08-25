# -----------------------------------------------------------------
# Name : py_survey_evidence_streaming.py
# Author : Ajay, Snehangsu
# Description : Gathers data from Kafka topic and pre-processes the data to produce into another
# -----------------------------------------------------------------

import time
import faust
import logging
import dateutil
import requests
import os, json
import datetime
from datetime import date,time
from dateutil import parser as date_parser
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Handles logs for success
successHandler = RotatingFileHandler(config.get('LOGS', 'survey_evidence_streaming_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'survey_evidence_streaming_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Handles logs for errors
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(config.get('LOGS', 'survey_evidence_streaming_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'survey_evidence_streaming_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    '''Initializing the faust session'''
    app = faust.App(
        'ml_survey_evidence_faust',
        broker='kafka://'+config.get("KAFKA", "url"),
        value_serializer='raw',
        web_port=7004,
        broker_max_poll_records=500
    )

    kafka_url = config.get("KAFKA", "url")
    producer = KafkaProducer(bootstrap_servers=[kafka_url])

except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def convert(lst):
        '''Joins arguments passed with a comma''' 
        return ','.join(lst)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def evidence_extraction(obSub):
        '''Gatherig and pre-processing the data from the data passed in the kafka topic'''
        if obSub['status'] == 'completed':
            if 'isAPrivateProgram' in obSub:
                successLogger.debug("Survey Evidence Submission Id : " + obSub['_id'])
            try:
                completedDate = obSub['completedDate']
            except KeyError:
                pass
            evidence_sub_count = 0
            try:
                answersArr = [v for v in obSub['answers'].values()]
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
                try:
                    surveySubQuestionsObj['questionExternalId'] = str(answer['externalId'])
                    surveySubQuestionsObj['questionName'] = answer['payload']['question'][0]
                except KeyError:
                    surveySubQuestionsObj['questionExternalId'] = ''
                    surveySubQuestionsObj['questionName'] = ''
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
                    producer.send(
                        (config.get("KAFKA", "survey_evidence_druid_topic")), 
                        json.dumps(surveySubQuestionsObj).encode('utf-8')
                    )
                    producer.flush()
                    successLogger.debug("Send Obj to Kafka")
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    #loop the consumer messages and produce it to another topic
    @app.agent(config.get("KAFKA", "survey_raw_topic"))
    async def survey_Faust(consumer) :
        async for msg in consumer:
            msg_val = msg.decode('utf-8')
            msg_data = json.loads(msg_val)
            successLogger.debug("========== START OF SURVEY EVIDENCE SUBMISSION ========")
            evidence_extraction(msg_data)
            successLogger.debug("********* END OF SURVEY EVIDENCE SUBMISSION ***********")
except Exception as e:
    errorLogger.error(e,exc_info=True)


if __name__ == '__main__':
   app.main()
