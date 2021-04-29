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
from kafka import KafkaConsumer, KafkaProducer
import dateutil
from dateutil import parser as date_parser
from configparser import ConfigParser, ExtendedInterpolation
import faust
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_success_log_filename')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_success_log_filename'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    onfig.get('LOGS', 'survey_evidence_streaming_error_log_filename')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOGS', 'survey_evidence_streaming_error_log_filename'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    app = faust.App(
        'sl_py_survey_evidence_prod',
        broker='kafka://'+config.get("KAFKA", "kafka_url"),
        value_serializer='raw',
        web_port=7005
    )

    kafka_url = config.get("KAFKA", "kafka_url")
    producer = KafkaProducer(bootstrap_servers=[kafka_url])

    #db production
    client = MongoClient(config.get('MONGO', 'mongo_url'))
    db = client[config.get('MONGO', 'database_name')]
    surveySubmissionsCollec = db[config.get('MONGO', 'survey_submissions_collection')]
    questionsCollec = db[config.get('MONGO', 'questions_collec')]

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
            successLogger.debug("Survey Evidence Submission Id : " + str(msg_id))
            try:
                completedDate = str(
                    datetime.datetime.date(obSub['completedDate'])
                ) + 'T' + str(
                    datetime.datetime.time(obSub['completedDate'])
                ) + 'Z'
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
                    surveySubQuestionsObj['appName'] = config.get("COMMON", "diksha_survey_app_name")
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
                        (config.get("KAFKA", "kafka_evidence_druid_topic")), 
                        json.dumps(surveySubQuestionsObj).encode('utf-8')
                    )
                    producer.flush()
                    successLogger.debug("Send Obj to Kafka")
except Exception as e:
    errorLogger.error(e, exc_info=True)


try:
    #loop the consumer messages and produce it to another topic
    @app.agent(config.get("KAFKA", "kafka_dev_topic"))
    async def survey_Faust(consumer) :
        async for msg in consumer:
            msg_val = msg.decode('utf-8')
            msg_data = json.loads(msg_val)
            successLogger.debug("========== START OF SURVEY EVIDENCE SUBMISSION ========")
            evidence_extraction(msg_data['_id'])
            successLogger.debug("********* END OF SURVEY EVIDENCE SUBMISSION ***********")
except Exception as e:
    errorLogger.error(e,exc_info=True)


if __name__ == '__main__':
   app.main()
