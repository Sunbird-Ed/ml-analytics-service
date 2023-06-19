# -----------------------------------------------------------------
# Name : py_observation_evidence_streaming.py
# Author : Shakthieshwari.A, Snehangsu, Ajay
# Description : Extracts the Evidence or Files Attached at each question level 
# during the observation submission
# -----------------------------------------------------------------

import faust
import logging
import os, json
import datetime
from datetime import date
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


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
old_file_path_success = config.get('LOGS', 'observation_streaming_evidence_success')
old_file_path_error = config.get('LOGS', 'observation_streaming_evidence_error')
if os.path.isfile(old_file_path_success):
    os.remove(old_file_path_success)
if os.path.isfile(old_file_path_error):
    os.remove(old_file_path_error)

for file_name in os.listdir(file_path_for_output_and_debug_log):
    file_path = os.path.join(file_path_for_output_and_debug_log, file_name)
    if os.path.isfile(file_path):
        file_date = file_name.split('.')[0]
        date = file_date.split('-')[0] + '-' + file_date.split('-')[1] + '-' + file_date.split('-')[2]
        if date < number_of_days_logs_kept:
            os.remove(file_path)


formatter = logging.Formatter('%(asctime)s - %(levelname)s')
# Handles success logs
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successHandler = RotatingFileHandler(f"{file_name_for_output_log}")
successBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}",when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Handles Error logs
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(f"{file_name_for_output_log}")
errorBackuphandler = TimedRotatingFileHandler(f"{file_name_for_output_log}",when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

#add the Infologer
infoLogger = logging.getLogger('info log')
infoLogger.setLevel(logging.INFO)
infoHandler = RotatingFileHandler(f"{file_name_for_debug_log}")
infoBackuphandler = TimedRotatingFileHandler(f"{file_name_for_debug_log}",when="w0",backupCount=1)
infoHandler.setFormatter(formatter)
infoLogger.addHandler(infoHandler)
infoLogger.addHandler(infoBackuphandler)

try:
  kafka_url = config.get("KAFKA", "url")
  app = faust.App(
   'ml_observation_evidence_faust',
   broker='kafka://'+kafka_url,
   value_serializer='raw',
   web_port=7002,
   broker_max_poll_records=500
  )
  rawTopicName = app.topic(config.get("KAFKA", "observation_raw_topic"))
  producer = KafkaProducer(bootstrap_servers=[kafka_url])

except Exception as e:
  errorLogger.error(e, exc_info=True)

try :
  def convert(lst): 
    return ','.join(lst)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def evidence_extraction(obSub):
    if obSub['status']=='completed':
      if 'isAPrivateProgram' in obSub :
        successLogger.debug("Observation Evidence Submission Id : " + obSub['_id'])
        try:
          completedDate = obSub['completedDate']
        except KeyError:
          completedDate = '' 
        evidence_sub_count = 0

      try:
        answersArr = [ v for v in obSub['answers'].values()]
      except KeyError:
        answersArr = []

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
            try:
              observationSubQuestionsObj['questionExternalId'] = str(answer['externalId'])
              observationSubQuestionsObj['questionName'] = answer['question'][0]
            except KeyError:
              observationSubQuestionsObj['questionExternalId'] = ''
              observationSubQuestionsObj['questionName'] = ''

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
        except KeyError:
          pass
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
 @app.agent(rawTopicName)
 async def observationEvidenceFaust(consumer):
    async for msg in consumer :
      msg_val = msg.decode('utf-8')
      msg_data = json.loads(msg_val)
      successLogger.debug("========== START OF OBSERVATION EVIDENCE SUBMISSION ========")
      evidence_extraction(msg_data)
      successLogger.debug("********* END OF OBSERVATION EVIDENCE SUBMISSION ***********")
except Exception as e:
  errorLogger.error(e, exc_info=True)

if __name__ == '__main__':
 app.main()
