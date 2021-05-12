# -----------------------------------------------------------------
# Name : sl_py_observation_streaming.py
# Author : Ashwini.E , Shakthieshwari.A
# Description : Program to read data from one kafka topic and 
#   produce it to another kafka topic 
# -----------------------------------------------------------------

import faust
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json
import datetime
# from datetime import date,time
import requests
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser,ExtendedInterpolation
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler
import redis

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
  config.get('LOGS', 'observation_streaming_success_log_filename')
)
successBackuphandler = TimedRotatingFileHandler(
  config.get('LOGS', 'observation_streaming_success_log_filename'),
  when="w0",
  backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
  config.get('LOGS', 'observation_streaming_error_log_filename')
)
errorBackuphandler = TimedRotatingFileHandler(
  config.get('LOGS', 'observation_streaming_error_log_filename'),
  when="w0",
  backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

domArr = []

kafka_url = config.get("KAFKA", "kafka_url")
#consume the message from kafka topic
app = faust.App(
  'sl_observation_diksha_faust',
  broker='kafka://'+kafka_url,
  value_serializer='raw',
  web_port=7001,
  broker_max_poll_records=500
)
rawTopicName = app.topic(config.get("KAFKA", "kafka_raw_data_topic"))
producer = KafkaProducer(bootstrap_servers=[kafka_url])
#db production
client = MongoClient(config.get('MONGO', 'mongo_url'))
db = client[config.get('MONGO', 'database_name')]
obsSubCollec = db[config.get('MONGO', 'observation_sub_collec')]
solCollec = db[config.get('MONGO', 'solutions_collec')]
obsCollec = db[config.get('MONGO', 'observations_collec')]
questionsCollec = db[config.get('MONGO', 'questions_collec')]
entitiesCollec = db[config.get('MONGO', 'entities_collec')]
criteriaQuestionsCollec = db[config.get('MONGO', 'criteria_questions_collection')]
criteriaCollec = db[config.get('MONGO', 'criteria_collec')]
programsCollec = db[config.get('MONGO', 'programs_collec')]
# redis cache connection 
redis_connection = redis.ConnectionPool(
  host=config.get("REDIS", "host"), 
  decode_responses=True, 
  port=config.get("REDIS", "port"), 
  db=config.get("REDIS", "db_name")
)
datastore = redis.StrictRedis(connection_pool=redis_connection)

try:
  def removeduplicate(it):
    seen = []
    for x in it:
      if x not in seen:
        yield x
        seen.append(x)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def getRelatedEntity(entityId):
    urlEntityRelated = config.get(
      "URL", "base_url"
    ) + "/" + config.get(
      "URL", "url_entity_related"
    ) + str(entityId)
    headersEntityRelated = {
      'Content-Type': config.get("API_HEADERS", "content_type"),
      'Authorization': "Bearer "+ config.get("API_HEADERS", "authorization"),
      'internal-access-token': config.get("API_HEADERS", "internal_access_token")
    }
    responseEntityRelated = requests.get(urlEntityRelated, headers=headersEntityRelated)
    if responseEntityRelated.status_code == 200:
      successLogger.debug("entityRelated api")
      return responseEntityRelated.json()
    else:
      errorLogger.error(" Failure in EntityRelatedApi ")
      errorLogger.error(responseEntityRelated)
      errorLogger.error(responseEntityRelated.text)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def getUserRoles(userId):
    urlUserRoles = config.get(
      "URL", "base_url"
    ) + "/" + config.get(
      "URL", "url_user_profile_api"
    ) + str(userId)
    headersUserRoles ={
      'Content-Type': config.get("API_HEADERS", "content_type"),
      'Authorization': "Bearer "+ config.get("API_HEADERS", "authorization"),
      'internal-access-token': config.get("API_HEADERS", "internal_access_token")
    }
    responseUserRoles = requests.get(urlUserRoles, headers=headersUserRoles)
    if responseUserRoles.status_code == 200 :
      successLogger.debug("user profile api")
      return responseUserRoles.json()
    else:
      errorLogger.error("user profile api failed")
      errorLogger.error(responseUserRoles)
      errorLogger.error(responseUserRoles.text)
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  #initialising the values
  class node:
    #Construction of Node with component,status and children
    def _init_(self, type=None, externalId=None, name=None, children=None):
      self.type = type
      self.externalId = externalId
      self.name = name
      if children is None:
        self.children = []
      else:
        self.children = children


  #Construction of tree through recursion            
  class implementation:
    def buildnode(self, ob, parent, ansCriteriaId):
      node1= node()
      node1.type=ob['type']
      node1.externalId=ob['externalId']
      node1.name=ob['name']
      node1.parent = parent
      node1.children=[]

      if (node1.type == 'criteria') and (node1.externalId == ansCriteriaId ):
        criteriaObj = {}
        criteriaObj['type'] = node1.type
        criteriaObj['externalId'] = str(node1.externalId)
        criteriaObj['name'] = node1.name
        criteriaObj['parent'] = parent
        domArr.append(criteriaObj)

      try:
        for children in ob['children']:
          parent = ob['name']
          node1.children.append(self.buildnode(children,parent,ansCriteriaId))
      except KeyError:
        if ob['criteria']:
          for cri in ob['criteria']:
            if str(cri['criteriaId']) == ansCriteriaId :
              criObj = {}
              criObj['type'] = 'criteria'
              criObj['externalId'] = str(cri['criteriaId'])
              criObj['name']=''
              criObj['parent']=ob['name']
              domArr.append(criObj)
      val = len(domArr)
      arr = domArr[0:val]
      return arr
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  def obj_creation(msg_id):
    successLogger.debug("Observation Submission Id : " + str(msg_id))
    cursorMongo = obsSubCollec.find(
      {'_id':ObjectId(msg_id)}, no_cursor_timeout=True
    )
    for obSub in cursorMongo :
      observationSubQuestionsArr = []
      completedDate = None
      try:
        completedDate = str(datetime.datetime.date(obSub['completedDate'])) + 'T' + str(datetime.datetime.time(obSub['completedDate'])) + 'Z'
      except KeyError:
        pass
      createdAt = str(datetime.datetime.date(obSub['createdAt'])) + 'T' + str(datetime.datetime.time(obSub['createdAt'])) + 'Z'
      updatedAt = str(datetime.datetime.date(obSub['updatedAt'])) + 'T' + str(datetime.datetime.time(obSub['updatedAt'])) + 'Z'
      evidencesArr = [ v for v in obSub['evidences'].values() ]
      evidence_sub_count = 0
      entityId = obSub['entityId']

      # fetch entity latitude and longitude from the database
      entityLatitude = None
      entityLongitude = None
      for ent in entitiesCollec.find({'_id':ObjectId(entityId)}):
        try :
          if ent['metaInformation']['gpsLocation'] :
            gpsLocation = (ent['metaInformation']['gpsLocation']).split(',')
            entityLatitude = gpsLocation[0]
            entityLongitude = gpsLocation[1]
        except KeyError :
          entityLatitude = ''
          entityLongitude = ''
      userObj = {}
      userObj = datastore.hgetall("user:" + obSub["createdBy"])
      if userObj :
        stateName = None
        blockName = None
        districtName = None
        clusterName = None
        rootOrgId = None
        userSubType = None
        userSchool = None
        userSchoolUDISE = None
        userSchoolName = None

        try:
          userSchool = userObj["school"]
        except KeyError :
          userSchool = ''

        try:
          userSchoolUDISE = userObj["schooludisecode"]
        except KeyError :
          userSchoolUDISE = ''

        try:
          userSchoolName = userObj["schoolname"]
        except KeyError :
          userSchoolName = ''

        try:
          userSubType = userObj["usersubtype"]
        except KeyError :
          userSubType = ''

        try:
          stateName = userObj["state"]
        except KeyError :
          stateName = ''

        try:
          blockName = userObj["block"]
        except KeyError :
          blockName = ''

        try:
          districtName = userObj["district"]
        except KeyError :
          districtName = ''

        try:
          clusterName = userObj["cluster"]
        except KeyError :
          clusterName = ''

        try:
          rootOrgId = userObj["rootorgid"]
        except KeyError :
          rootOrgId = ''

        userRoles = {}
        obsAppName = None
        try :
          obsAppName = obSub["appInformation"]["appName"].lower()
        except KeyError :
          obsAppName = config.get("COMMON", "diksha_survey_app_name")
        userRolesArrUnique = []
        if obsAppName == config.get("COMMON", "diksha_survey_app_name") : 
          userRoles = getUserRoles(obSub["createdBy"])
          userRolesArr = []
          if userRoles:
            userRoleKeyCheck = "result" in userRoles
            if userRoleKeyCheck == True :
              try :
                if len(userRoles["result"]["roles"]) > 0 :
                  for rol in userRoles["result"]["roles"] :
                    for ent in rol["entities"]:
                      userEntityRelated = None
                      userEntityRelated = getRelatedEntity(ent["_id"])
                      userEntityRelatedResultKeyCheck = None
                      roleObj = {}
                      roleObj["role_title"] = rol["title"]
                      if userEntityRelated:
                        userEntityRelatedResultKeyCheck = "result" in userEntityRelated
                        if userEntityRelatedResultKeyCheck == True:
                          if userEntityRelated["result"]:
                            if (userEntityRelated["result"]["entityType"] == "district") or (userEntityRelated["result"]["entityType"] == "block") or (userEntityRelated["result"]["entityType"] == "cluster") or (userEntityRelated["result"]["entityType"] == "state"):
                              roleObj['user_'+userEntityRelated["result"]["entityType"]+'Name'] = userEntityRelated["result"]["metaInformation"]["name"]
                            if userEntityRelated["result"]["entityType"] == "school" :
                              roleObj['user_schoolName'] = userEntityRelated["result"]["metaInformation"]["name"]
                              roleObj['user_schoolId'] = str(userEntityRelated["result"]["metaInformation"]["id"])
                              roleObj['user_schoolUDISE_code'] = userEntityRelated["result"]["metaInformation"]["externalId"]
                            for usrEntityData in userEntityRelated["result"]["relatedEntities"]:
                              if (usrEntityData['entityType'] == "district") or (usrEntityData['entityType'] == "block") or (usrEntityData['entityType'] == "cluster") or (usrEntityData['entityType'] == "state"):
                                roleObj['user_'+usrEntityData['entityType']+'Name'] = usrEntityData['metaInformation']['name']
                              if usrEntityData['entityType'] == "school" :
                                roleObj['user_schoolName'] = usrEntityData["metaInformation"]["name"]
                                roleObj['user_schoolId'] = str(usrEntityData["metaInformation"]["id"])
                                roleObj['user_schoolUDISE_code'] = usrEntityData["metaInformation"]["externalId"]
                      userRolesArr.append(roleObj)
              except KeyError :
                userRolesArr = []
                
          if len(userRolesArr) > 0:
            userRolesArrUnique = list(removeduplicate(userRolesArr))
        elif obsAppName == config.get("COMMON", "diksha_integrated_app_name"):
          roleObj = {}
          roleObj["role_title"] = userSubType
          roleObj["user_stateName"] = stateName
          roleObj["user_blockName"] = blockName
          roleObj["user_districtName"] = districtName
          roleObj["user_clusterName"] = clusterName
          roleObj["user_schoolName"] = userSchoolName
          roleObj["user_schoolId"] = userSchool
          roleObj["user_schoolUDISE_code"] = userSchoolUDISE
          userRolesArrUnique.append(roleObj)
        entityRelated = None
        entityRelated = getRelatedEntity(entityId)
        entityRelatedResultKeyCheck = None
        entityRelatedData = None
        if entityRelated:
          entityRelatedResultKeyCheck = "result" in entityRelated
          if entityRelatedResultKeyCheck == True: 
            entityRelatedData = entityRelated['result']

        if 'answers' in obSub.keys() :  
          answersArr = [ v for v in obSub['answers'].values()]
          for ans in answersArr:
            try:
              if len(ans['fileName']):
                evidence_sub_count   = evidence_sub_count + len(ans['fileName'])
            except KeyError:
              evidence_sub_count = 0
          for ans in answersArr:
            def sequenceNumber(externalId, answer, answerSection, solutionObj):
              try:
                for num in range(
                  len(solutionObj['questionSequenceByEcm'][answer['evidenceMethod']][answerSection])
                ):
                  if solutionObj['questionSequenceByEcm'][answer['evidenceMethod']][answerSection][num] == externalId:
                    return num + 1
              except KeyError:
                return ''

            def creatingObj(
              answer, quesexternalId, ans_val, instNumber, responseLabel, 
              entityLatitudeCreateObjFn, entityLongitudeCreateObjFn, usrRolFn
            ):
              observationSubQuestionsObj = {}
              observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
              observationSubQuestionsObj['appName'] = obsAppName
              try :
                if obSub["isRubricDriven"] == True :
                  observationSubQuestionsObj['solution_type'] = "observation_with_rubric"
                else :
                  observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
              except KeyError :
                observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
              # geo tag validation , question answered within 200 meters of the selected entity
              if entityLatitudeCreateObjFn and entityLongitudeCreateObjFn :
                entityGeoFencing = (entityLatitudeCreateObjFn,entityLongitudeCreateObjFn)
                answerGpsLocation = []
                try :
                  if answer['gpsLocation']:
                    answerGpsLocation = answer['gpsLocation'].split(',')
                    answerLatitude = None
                    answerLongitude = None
                    answerLatitude = answerGpsLocation[0]
                    answerLongitude = answerGpsLocation[1]
                except KeyError :
                  answerGpsLocation = []
                
                if len(answerGpsLocation) > 0 :
                  answerGeoFencing = (answerLatitude,answerLongitude)
                  calcuGeoLocMtrs = (geodesic(entityGeoFencing, answerGeoFencing).km)*1000
                  calcuGeoLocMtrsFloat = float(calcuGeoLocMtrs)

                  if calcuGeoLocMtrsFloat <= float(200) :
                    observationSubQuestionsObj['location_validated_with_geotag'] = 'verified'
                    observationSubQuestionsObj['distance_in_meters'] = int(calcuGeoLocMtrsFloat)
                  else :
                    observationSubQuestionsObj['location_validated_with_geotag'] = 'not verified'
                    observationSubQuestionsObj['distance_in_meters'] = int(calcuGeoLocMtrsFloat)
                else :
                  observationSubQuestionsObj['location_validated_with_geotag'] = 'gps location not found for question'
                  observationSubQuestionsObj['distance_in_meters'] = ''
              else :
                observationSubQuestionsObj['location_validated_with_geotag'] = 'gps location not found for school'
                observationSubQuestionsObj['distance_in_meters'] = ''                          

              observationSubQuestionsObj['entity'] = str(obSub['entityId'])
              observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
              observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name'] 

              if entityRelatedData : 
                entityType =  entityRelatedData["entityType"]
                observationSubQuestionsObj[entityType] = entityRelatedData['_id']
                observationSubQuestionsObj[entityType+'Name'] = entityRelatedData['metaInformation']['name']
                observationSubQuestionsObj[entityType+'ExternalId'] = entityRelatedData['metaInformation']['externalId']
                for entityData in entityRelatedData["relatedEntities"]:
                  if entityData['entityType']:
                    entityType = entityData['entityType']
                    observationSubQuestionsObj[entityType] = entityData['_id']
                    observationSubQuestionsObj[entityType+'Name'] = entityData['metaInformation']['name']
                    observationSubQuestionsObj[entityType+'ExternalId'] = entityData['metaInformation']['externalId']                           

              observationSubQuestionsObj['entityTypeId'] = str(obSub['entityTypeId'])

              try:
                observationSubQuestionsObj['schoolTypes'] = obSub['entityInformation']['schoolTypes']
              except KeyError:
                observationSubQuestionsObj['schoolTypes'] = ''

              try:
                observationSubQuestionsObj['administrationTypes'] = obSub['entityInformation']['administrationTypes']
              except KeyError:
                observationSubQuestionsObj['administrationTypes'] = ''
              observationSubQuestionsObj['createdBy'] = obSub['createdBy']

              try:
                if obSub['isAPrivateProgram']:
                  observationSubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                else:
                  observationSubQuestionsObj['isAPrivateProgram'] = False
              except KeyError:
                observationSubQuestionsObj['isAPrivateProgram'] = False

              try:
                observationSubQuestionsObj['programExternalId'] = obSub['programExternalId']
              except KeyError :
                observationSubQuestionsObj['programExternalId'] = ''

              try:
                observationSubQuestionsObj['programId'] = str(obSub['programId'])
              except KeyError :
                observationSubQuestionsObj['programId'] = ''

              try:
                for pgm in programsCollec.find({"_id":ObjectId(obSub['programId'])}):
                  observationSubQuestionsObj['programName'] = pgm['name']
                  observationSubQuestionsObj['programDescription'] = pgm['description']
              except KeyError :
                observationSubQuestionsObj['programName'] = ''
                observationSubQuestionsObj['programDescription'] = ''

              observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
              observationSubQuestionsObj['solutionId'] = str(obSub['solutionId'])
              observationSubQuestionsObj['observationId'] = str(obSub['observationId'])
              for critQues in criteriaQuestionsCollec.find({'_id':ObjectId(answer["criteriaId"])}):
                observationSubQuestionsObj['criteriaExternalId'] = critQues['externalId']
                observationSubQuestionsObj['criteriaName'] = critQues['name']
                observationSubQuestionsObj['criteriaDescription'] = critQues['description']
                for eviCQ in critQues["evidences"] :
                  for secCQ in eviCQ["sections"] :
                    for quesCQ in secCQ["questions"] :
                      if str(quesCQ["_id"]) == answer["qid"] :
                        observationSubQuestionsObj['section'] = secCQ["code"]
              for solu in solCollec.find({'_id':ObjectId(obSub['solutionId'])}):
                solutionObj = {}
                solutionObj = solu

              observationSubQuestionsObj['solutionName'] = solutionObj['name']
              observationSubQuestionsObj['scoringSystem'] = solutionObj['scoringSystem']
              observationSubQuestionsObj['solutionDescription'] = solutionObj['description']
              observationSubQuestionsObj['questionSequenceByEcm'] = sequenceNumber(quesexternalId,answer,observationSubQuestionsObj['section'],solutionObj)

              try:
                if solutionObj['scoringSystem'] == 'pointsBasedScoring':
                  try:
                    observationSubQuestionsObj['totalScore'] = obSub['pointsBasedMaxScore']
                  except KeyError :
                    observationSubQuestionsObj['totalScore'] = ''
                  try:
                    observationSubQuestionsObj['scoreAchieved'] = obSub['pointsBasedScoreAchieved']
                  except KeyError :
                    observationSubQuestionsObj['scoreAchieved'] = ''
                  try:
                    observationSubQuestionsObj['totalpercentage'] = obSub['pointsBasedPercentageScore']
                  except KeyError :
                    observationSubQuestionsObj['totalpercentage'] = ''

                  try:
                    observationSubQuestionsObj['maxScore'] = answer['maxScore']
                  except KeyError :
                    observationSubQuestionsObj['maxScore'] = ''

                  try:
                    observationSubQuestionsObj['minScore'] = answer['scoreAchieved']
                  except KeyError :
                    observationSubQuestionsObj['minScore'] = ''

                  try:
                    observationSubQuestionsObj['percentageScore'] = answer['percentageScore']
                  except KeyError :
                    observationSubQuestionsObj['percentageScore'] = ''

                  try:
                    observationSubQuestionsObj['pointsBasedScoreInParent'] = answer['pointsBasedScoreInParent']
                  except KeyError :
                    observationSubQuestionsObj['pointsBasedScoreInParent'] = ''
              except KeyError:
                observationSubQuestionsObj['totalScore'] = ''
                observationSubQuestionsObj['scoreAchieved'] = ''
                observationSubQuestionsObj['totalpercentage'] = ''
                observationSubQuestionsObj['maxScore'] = ''
                observationSubQuestionsObj['minScore'] = ''
                observationSubQuestionsObj['percentageScore'] = ''
                observationSubQuestionsObj['pointsBasedScoreInParent'] = ''

              observationSubQuestionsObj['entityType'] = obSub['entityType']

              try:
                for ob in obsCollec.find({'_id':obSub['observationId']},{'name':1}):
                  observationSubQuestionsObj['observationName'] = ob['name']
              except KeyError :
                observationSubQuestionsObj['observationName'] = ''

              observationSubQuestionsObj['questionId'] = str(answer['qid'])
              observationSubQuestionsObj['questionAnswer'] = ans_val
              observationSubQuestionsObj['questionResponseType'] = answer['responseType']
              if answer['responseType'] == 'number':
                if answer['payload']['labels']:
                  observationSubQuestionsObj['questionResponseLabel_number'] = responseLabel
                else:
                  observationSubQuestionsObj['questionResponseLabel_number'] = 0
              if answer['payload']['labels']:
                observationSubQuestionsObj['questionResponseLabel'] = responseLabel
              else:
                observationSubQuestionsObj['questionResponseLabel'] = ''
              observationSubQuestionsObj['questionExternalId'] = quesexternalId
              observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]
              observationSubQuestionsObj['questionECM'] = answer['evidenceMethod']
              observationSubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
              observationSubQuestionsObj['completedDate'] = completedDate
              observationSubQuestionsObj['createdAt'] = createdAt
              observationSubQuestionsObj['updatedAt'] = updatedAt
              observationSubQuestionsObj['remarks'] = answer['remarks']
              if len(answer['fileName']):
                multipleFiles = None
                fileCnt = 1
                for filedetail in answer['fileName']:
                  if fileCnt == 1:
                    multipleFiles = config.get('URL', 'evidence_base_url') + filedetail['sourcePath']
                    fileCnt = fileCnt + 1
                  else:
                    multipleFiles = multipleFiles + ' , ' + config.get('URL', 'evidence_base_url') + filedetail['sourcePath']
                observationSubQuestionsObj['evidences'] = multipleFiles                                  
                observationSubQuestionsObj['evidence_count'] = len(answer['fileName'])
              observationSubQuestionsObj['total_evidences'] = evidence_sub_count

              # to fetch the parent question of matrix
              if ans['responseType']=='matrix':
                observationSubQuestionsObj['instanceParentQuestion'] = ans['payload']['question'][0]
                observationSubQuestionsObj['instanceParentId'] = ans['qid']
                observationSubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                observationSubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                for critQuesInst in criteriaQuestionsCollec.find({'_id':ObjectId(ans["criteriaId"])}):
                  observationSubQuestionsObj['instanceParentCriteriaExternalId'] = critQuesInst['externalId']
                  observationSubQuestionsObj['instanceParentCriteriaExternalId'] = critQuesInst['name']
                  for eviCQInst in critQuesInst["evidences"] :
                    for secCQInst in eviCQInst["sections"] :
                      for quesCQInst in secCQInst["questions"] :
                        if str(quesCQInst["_id"]) == ans["qid"] :
                          observationSubQuestionsObj['instanceParentSection'] = secCQInst["code"]
                  observationSubQuestionsObj['instanceId'] = instNumber
                  for ques in questionsCollec.find({'_id':ObjectId(ans['qid'])}):
                    observationSubQuestionsObj['instanceParentExternalId'] = ques['externalId']
                  observationSubQuestionsObj['instanceParentEcmSequence']= sequenceNumber(
                    observationSubQuestionsObj['instanceParentExternalId'], answer,
                    observationSubQuestionsObj['instanceParentSection'], solutionObj
                  )
              else:
                observationSubQuestionsObj['instanceParentQuestion'] = ''
                observationSubQuestionsObj['instanceParentId'] = ''
                observationSubQuestionsObj['instanceParentResponsetype'] =''
                observationSubQuestionsObj['instanceId'] = instNumber
                observationSubQuestionsObj['instanceParentExternalId'] = ''
                observationSubQuestionsObj['instanceParentEcmSequence'] = '' 
              observationSubQuestionsObj['channel'] = rootOrgId
              observationSubQuestionsObj['parent_channel'] = "SHIKSHALOKAM"

              ### Assessment Domain Logic - Start ###
              domainArr = []
              for domain in solutionObj['themes']:
                parent = None
                builder = None
                parent = domain['name']
                builder = implementation()
                domObj = {}
                domObj['name'] = domain['name']
                domObj['type'] = domain['type']
                domObj['externalId']=str(domain['externalId'])
                
                try:
                  if domain['criteria']:
                    domObj['theme']=builder.buildnode(domain, parent, str(answer['criteriaId']))
                except KeyError:
                  domObj['theme'] = builder.buildnode(domain, parent, str(answer['criteriaId']))

                domainArr.append(domObj)
                domArr.clear()

              for dom in domainArr:
                if dom['theme']:
                  for obj in dom['theme']:
                    try:
                      if obj['type'] == 'criteria':
                        if (str(obj['externalId']) == str(answer['criteriaId'])):
                          for criteria in obSub['criteria'] :
                            if str(criteria["_id"]) == str(answer['criteriaId']) :
                              obj['name'] = criteria['name']
                              obj['score'] = criteria['score']
                              try:
                                obj['score_achieved'] = criteria['scoreAchieved']
                              except KeyError :
                                obj['score_achieved'] = ''
                              obj['description'] = criteria['description']
                              try:
                                levelArray = []
                                levelArray = criteria['rubric']['levels'].values()
                                for labelValue in levelArray:
                                  if (str((criteria['score'])) == labelValue['level']):
                                    obj['label'] = labelValue['label']
                              except Exception:
                                obj['label'] = ''

                              try:
                                prj_id = []
                                title = []
                                goal = []
                                externalId =[]
                                for prj in criteria['improvement-projects']:
                                  prj_id.append(str(prj['_id']))
                                  title.append(prj['title'])
                                  goal.append(prj['goal'])
                                  externalId.append(prj['externalId'])
                                obj['imp_project_id'] = prj_id
                                obj['imp_project_title'] = title
                                obj['imp_project_goal'] = goal
                                obj['imp_project_externalId'] = externalId
                              except KeyError:
                                obj['imp_project_id'] = []
                                obj['imp_project_title'] = []
                                obj['imp_project_goal'] = []
                                obj['imp_project_externalId'] = []
                          if type(obj['externalId']) != str:
                            for cri in criteriaCollec.find({'_id':ObjectId(str(obj['externalId']))}):
                              obj['externalId'] = cri['externalId']
                              obj['name']=cri['name']
                              obj['score']=cri['score']
                              obj['score_achieved'] = criteria['scoreAchieved']
                              obj['description'] = cri['description']
                              try:
                                levelArray = []
                                levelArray = cri['rubric']['levels'].values()
                                for labelValue in levelArray:
                                  if (str((cri['score'])) == labelValue['level']):
                                    obj['label'] = labelValue['label']
                              except Exception:
                                obj['label'] = ''
                    except KeyError:
                      pass 

              for themes in domainArr:
                for st in themes["theme"]:
                  if (st["type"] == "criteria") and (observationSubQuestionsObj['criteriaId'] == str(st["externalId"])):
                    observationSubQuestionsObj['domainName'] = themes['name']
                    observationSubQuestionsObj['domainExternalId'] = themes['externalId']
                    try :
                      for submTheme in obSub["themes"]: 
                        if submTheme["externalId"] == themes['externalId'] :
                          observationSubQuestionsObj['domainLevel'] = submTheme["pointsBasedLevel"]
                          observationSubQuestionsObj['domainScore'] = submTheme["scoreAchieved"]
                    except KeyError :
                      observationSubQuestionsObj['domainLevel'] = ''
                      observationSubQuestionsObj['domainScore'] = ''       
                    for theme in themes['theme']:
                      observationSubQuestionsObj['childName'] = theme['name']
                      observationSubQuestionsObj['ancestorName'] = theme['parent']
                      observationSubQuestionsObj['childType'] = theme['type']
                      observationSubQuestionsObj['childExternalid'] = theme['externalId']

                      try:
                        observationSubQuestionsObj['level'] = theme['score']
                      except KeyError:
                        observationSubQuestionsObj['level'] = ''

                      try:
                        observationSubQuestionsObj['criteriaScore'] = theme['score_achieved']
                      except KeyError:
                        observationSubQuestionsObj['criteriaScore'] = ''

                      try:
                        observationSubQuestionsObj['label'] = theme['label']
                      except KeyError:
                        observationSubQuestionsObj['label'] = ''

                      try:
                        if (len(theme['imp_project_id']) >=0):
                          for i in range(len(theme['imp_project_id'])):
                            observationSubQuestionsObj['imp_project_id'] = theme['imp_project_id'][i]
                            observationSubQuestionsObj['imp_project_title'] = theme['imp_project_title'][i]
                            observationSubQuestionsObj['imp_project_goal'] = theme['imp_project_goal'][i]
                            observationSubQuestionsObj['imp_project_externalId'] = theme['imp_project_externalId'][i]
                      except KeyError:
                        observationSubQuestionsObj['imp_project_id'] = ""
                        observationSubQuestionsObj['imp_project_title'] = ""
                        observationSubQuestionsObj['imp_project_goal'] = ""
                        observationSubQuestionsObj['imp_project_externalId'] = ""

              if usrRolFn :
                observationSubQuestionsObj = {**usrRolFn, **observationSubQuestionsObj} 
              observationSubQuestionsObj["submissionNumber"] = obSub["submissionNumber"]
              observationSubQuestionsObj["submissionTitle"] = obSub["title"] 
              try:
                observationSubQuestionsObj["criteriaLevelReport"] = obSub["criteriaLevelReport"]
              except KeyError :
                observationSubQuestionsObj["criteriaLevelReport"] = ''

              return observationSubQuestionsObj

            def fetchingQuestiondetails(ansFn, instNumber, entityLatitudeQuesFn, entityLongitudeQuesFn):        
              for ques in questionsCollec.find({'_id':ObjectId(ansFn['qid'])}):
                if len(ques['options']) == 0:
                  try:
                    if len(ansFn['payload']['labels']) > 0:
                      if(len(userRolesArrUnique)) > 0:
                        for usrRol in userRolesArrUnique :
                          finalObj = {}
                          finalObj =  creatingObj(
                            ansFn,ques['externalId'],
                            ansFn['value'],instNumber,
                            ansFn['payload']['labels'][0],
                            entityLatitudeQuesFn,
                            entityLongitudeQuesFn,usrRol
                          )
                          if finalObj["completedDate"]:
                            producer.send(
                              (config.get("KAFKA", "kafka_druid_topic")), 
                              json.dumps(finalObj).encode('utf-8')
                            )
                            producer.flush()
                            successLogger.debug("Send Obj to Kafka")
                      else :
                        finalObj = {}
                        finalObj =  creatingObj(
                          ansFn,ques['externalId'],
                          ansFn['value'],
                          instNumber,
                          ansFn['payload']['labels'][0],
                          entityLatitudeQuesFn,
                          entityLongitudeQuesFn,
                          None
                        ) 
                        if finalObj["completedDate"]:
                          producer.send(
                            (config.get("KAFKA", "kafka_druid_topic")), 
                            json.dumps(finalObj).encode('utf-8')
                          )
                          producer.flush()
                          successLogger.debug("Send Obj to Kafka")
                  except KeyError:
                    pass
                else:
                  labelIndex = 0
                  for quesOpt in ques['options']:
                    try:
                      if type(ansFn['value']) == str or type(ansFn['value']) == int:
                        if quesOpt['value'] == ansFn['value'] :
                          if(len(userRolesArrUnique)) > 0:
                            for usrRol in userRolesArrUnique :
                              finalObj = {}
                              finalObj =  creatingObj(
                                ansFn,
                                ques['externalId'],
                                ansFn['value'],
                                instNumber,
                                ansFn['payload']['labels'][0],
                                entityLatitudeQuesFn,
                                entityLongitudeQuesFn,
                                usrRol
                              )
                              if finalObj["completedDate"]:
                                producer.send(
                                  (config.get("KAFKA", "kafka_druid_topic")), 
                                  json.dumps(finalObj).encode('utf-8')
                                )
                                producer.flush()
                                successLogger.debug("Send Obj to Kafka")
                          else :
                            finalObj = {}
                            finalObj =  creatingObj(
                              ansFn,ques['externalId'],
                              ansFn['value'],
                              instNumber,
                              ansFn['payload']['labels'][0],
                              entityLatitudeQuesFn,
                              entityLongitudeQuesFn,
                              None
                            )
                            if finalObj["completedDate"]:
                              producer.send(
                                (config.get("KAFKA", "kafka_druid_topic")), 
                                json.dumps(finalObj).encode('utf-8')
                              )
                              producer.flush()
                              successLogger.debug("Send Obj to Kafka") 
                            
                      elif type(ansFn['value']) == list:
                        for ansArr in ansFn['value']:
                          if quesOpt['value'] == ansArr:
                            if(len(userRolesArrUnique)) > 0:
                              for usrRol in userRolesArrUnique :
                                finalObj = {}
                                finalObj =  creatingObj(
                                  ansFn,
                                  ques['externalId'],
                                  ansArr,
                                  instNumber,
                                  quesOpt['label'],
                                  entityLatitudeQuesFn,
                                  entityLongitudeQuesFn,
                                  usrRol
                                )
                                if finalObj["completedDate"]:
                                  producer.send(
                                    (config.get("KAFKA", "kafka_druid_topic")), 
                                    json.dumps(finalObj).encode('utf-8')
                                  )
                                  producer.flush()
                                  successLogger.debug("Send Obj to Kafka")
                            else :
                              finalObj = {}
                              finalObj =  creatingObj(
                                ansFn,
                                ques['externalId'],
                                ansArr,
                                instNumber,
                                quesOpt['label'],
                                entityLatitudeQuesFn,
                                entityLongitudeQuesFn,
                                None
                              )
                              if finalObj["completedDate"]:
                                producer.send(
                                  (config.get("KAFKA", "kafka_druid_topic")), 
                                  json.dumps(finalObj).encode('utf-8')
                                )
                                producer.flush()
                                successLogger.debug("Send Obj to Kafka")
                            labelIndex = labelIndex + 1
                    except KeyError:
                      pass
                #to check the value is null ie is not answered
                try:
                  if type(ansFn['value']) == str and ansFn['value'] == '':
                    if(len(userRolesArrUnique)) > 0:
                      for usrRol in userRolesArrUnique :
                        finalObj = {}
                        finalObj =  creatingObj(
                          ansFn,
                          ques['externalId'],
                          ansFn['value'],
                          instNumber,
                          None,
                          entityLatitudeQuesFn,
                          entityLongitudeQuesFn,
                          usrRol
                        )
                        if finalObj["completedDate"]:
                          producer.send(
                            (config.get("KAFKA", "kafka_druid_topic")), 
                            json.dumps(finalObj).encode('utf-8')
                          )
                          producer.flush()
                          successLogger.debug("Send Obj to Kafka")
                    else :
                      finalObj = {}
                      finalObj =  creatingObj(
                        ansFn,
                        ques['externalId'],
                        ansFn['value'],
                        instNumber,
                        None,
                        entityLatitudeQuesFn,
                        entityLongitudeQuesFn,
                        None
                      )
                      if finalObj["completedDate"]:
                        producer.send(
                          (config.get("KAFKA", "kafka_druid_topic")), 
                          json.dumps(finalObj).encode('utf-8')
                        )
                        producer.flush()
                        successLogger.debug("Send Obj to Kafka")
                except KeyError:
                  pass

            if (
              ans['responseType'] == 'text' or ans['responseType'] == 'radio' or 
              ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or 
              ans['responseType'] == 'number' or ans['responseType'] == 'date'
            ):   
              inst_cnt = ''
              fetchingQuestiondetails(ans,inst_cnt, entityLatitude, entityLongitude)
            elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
              inst_cnt =0
              for instances in ans['value']:
                inst_cnt = inst_cnt + 1
                for instance in instances.values():
                  fetchingQuestiondetails(instance, inst_cnt, entityLatitude, entityLongitude)
    cursorMongo.close()
except Exception as e:
  errorLogger.error(e, exc_info=True)

try:
  @app.agent(rawTopicName)
  async def observationFaust(consumer) :
    async for msg in consumer :
      msg_val = msg.decode('utf-8')
      msg_data = json.loads(msg_val)
      successLogger.debug("========== START OF OBSERVATION SUBMISSION ========")
      obj_arr = obj_creation(msg_data['_id'])
      successLogger.debug("********* END OF OBSERVATION SUBMISSION ***********")
except Exception as e:
  errorLogger.error(e, exc_info=True)

if __name__ == '__main__':
  app.main()

