# -----------------------------------------------------------------
# Name : py_observation_evidence_streaming.py
# Author : Shakthieshwari.A
# Description : Extracts the Evidence or Files Attached at each question level 
#               during the observation submission
# -----------------------------------------------------------------
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json
import datetime
# from datetime import date, time
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation
import faust
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
  config.get('LOGS','observation_streaming_evidence_success')
)
successBackuphandler = TimedRotatingFileHandler(
  config.get('LOGS','observation_streaming_evidence_success'),
  when="w0",
  backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
  config.get('LOGS','observation_streaming_evidence_error')
)
errorBackuphandler = TimedRotatingFileHandler(
  config.get('LOGS','observation_streaming_evidence_error'),
  when="w0",
  backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
  kafka_url = config.get("KAFKA", "url")
  app = faust.App(
    'sl_observation_evidences_diksha_faust',
    broker='kafka://'+kafka_url,
    value_serializer='raw',
    web_port=7002,
    broker_max_poll_records=500
  )
  rawTopicName = app.topic(config.get("KAFKA", "observation_raw_topic"))
  producer = KafkaProducer(bootstrap_servers=[kafka_url])
  #db production
  clientdev = MongoClient(config.get('MONGO','mongo_url'))
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
      successLogger.debug("Observation Evidence Submission Id : " + str(msg_id))
      try:
        completedDate = str(
          datetime.datetime.date(obSub['completedDate'])
        ) + 'T' + str(
          datetime.datetime.time(obSub['completedDate'])
        ) + 'Z'
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
        if answer['qid']:
          observationSubQuestionsObj = {}
          observationSubQuestionsObj['completedDate'] = completedDate
          observationSubQuestionsObj['total_evidences'] = evidence_sub_count
          observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
          observationSubQuestionsObj['entity'] = str(obSub['entityId'])
          observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
          observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name']
          observationSubQuestionsObj['entityTypeId'] = str(obSub['entityTypeId'])
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
            producer.send(
              (config.get("KAFKA", "observation_evidence_druid_topic")), 
              json.dumps(observationSubQuestionsObj).encode('utf-8')
            )     
            producer.flush()
            successLogger.debug("Send Obj to Kafka")
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  @app.agent(rawTopicName)
  async def observationEvidenceFaust(consumer):
    async for msg in consumer :
      msg_val = msg.decode('utf-8')
      msg_data = json.loads(msg_val)
      successLogger.debug("========== START OF OBSERVATION EVIDENCE SUBMISSION ========")
      obj_arr = evidence_extraction(msg_data['_id'])
      successLogger.debug("********* END OF OBSERVATION EVIDENCE SUBMISSION ***********")
except Exception as e:
  errorLogger.error(e, exc_info=True)

if __name__ == '__main__':
  app.main()
