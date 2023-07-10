# -----------------------------------------------------------------
# Name : sl_py_observation_streaming.py
# Author : Ashwini.E , Shakthieshwari.A
# Description : Program to read data from one kafka topic and 
#   produce it to another kafka topic 
# -----------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pymongo import MongoClient
from bson.objectid import ObjectId
import os, json, re
import datetime
# from datetime import date,time
import requests
from configparser import ConfigParser,ExtendedInterpolation
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler
import redis
from azure.storage.blob import BlockBlobService
import pandas as pd


config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
#config.read(config_path[0] + "/config.ini")
#print(config_path[0] + "/config.ini")
config.read("/opt/sparkjobs/source/observations/config.ini")

# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'observation_streaming_success_error')
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

domArr = []


#db production
client = MongoClient(config.get('MONGO', 'url'))
db = client[config.get('MONGO', 'database_name')]
obsSubCollec = db[config.get('MONGO', 'observation_sub_collection')]
solCollec = db[config.get('MONGO', 'solutions_collection')]
obsCollec = db[config.get('MONGO', 'observations_collection')]
questionsCollec = db[config.get('MONGO', 'questions_collection')]
entitiesCollec = db[config.get('MONGO', 'entities_collection')]
criteriaQuestionsCollec = db[config.get('MONGO', 'criteria_questions_collection')]
criteriaCollec = db[config.get('MONGO', 'criteria_collection')]
programsCollec = db[config.get('MONGO', 'programs_collection')]
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
    urlEntityRelated = config.get("ML_SURVEY_SERVICE_URL", "url") + "/" + config.get("ML_SURVEY_SERVICE_URL", "entity_related_end_point") + str(entityId)
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
    urlUserRoles = config.get("ML_SURVEY_SERVICE_URL", "url") + "/" + config.get("ML_SURVEY_SERVICE_URL", "user_profile_end_point") + str(userId)
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
     if 'isAPrivateProgram' in obSub :
      completedDate = None
      try:
        completedDate = str(obSub['completedDate'])
      except KeyError:
        pass
      createdAt = str(obSub['createdAt'])
      updatedAt = str(obSub['updatedAt'])
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
   
       entitiesArrIds = []
       for userRoleKey, userRoleVal in obSub["userRoleInformation"].items():
          if userRoleKey != "role" :
             entitiesArrIds.append(userRoleVal)
       for entUR in entitiesCollec.find({"$or":[{"registryDetails.locationId":
         {"$in":entitiesArrIds}},{"registryDetails.code":{"$in":entitiesArrIds}}]},
         {"metaInformation.name":1,"entityType":1,"registryDetails":1}):
         if entUR["entityType"] == "state":
             stateName = entUR["metaInformation"]["name"]
         if entUR["entityType"] == "block":
             blockName = entUR["metaInformation"]["name"]
         if entUR["entityType"] == "district":
             districtName = entUR["metaInformation"]["name"]
         if entUR["entityType"] == "cluster":
             clusterName = entUR["metaInformation"]["name"]
         if entUR["entityType"] == "school":
             userSchool = str(entUR["_id"])
             if "code" in entUR["registryDetails"] and entUR["registryDetails"]["code"] :
                userSchoolUDISE = entUR["registryDetails"]["code"]
             else :
                userSchoolUDISE = entUR["registryDetails"]["locationId"]
             userSchoolName = entUR["metaInformation"]["name"]
       userSubType = obSub["userRoleInformation"]["role"]
       userObj = datastore.hgetall("user:" + obSub["createdBy"])
       if userObj :
        rootOrgId = None
        orgName = None
        boardName = None
        try:
          rootOrgId = userObj["rootorgid"]
        except KeyError :
          rootOrgId = ''
        try:
          orgName = userObj["orgname"]
        except KeyError:
          orgName = ''

        try:
          boardName = userObj["board"]
        except KeyError:
          boardName = ''
        
          
        userRoles = {}
        obsAppName = None
        try :
          obsAppName = obSub["appInformation"]["appName"].lower()
        except KeyError :
          obsAppName = config.get("ML_APP_NAME", "survey_app")
        userRolesArrUnique = []
        if obsAppName == config.get("ML_APP_NAME", "survey_app") : 
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
                      roleObj["organisation_name"] = orgName
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
        else:
          roleObj = {}
          roleObj["role_title"] = userSubType
          roleObj["user_stateName"] = stateName
          roleObj["user_blockName"] = blockName
          roleObj["user_districtName"] = districtName
          roleObj["user_clusterName"] = clusterName
          roleObj["user_schoolName"] = userSchoolName
          roleObj["user_schoolId"] = userSchool
          roleObj["user_schoolUDISE_code"] = userSchoolUDISE
          roleObj["organisation_name"] = orgName
          roleObj["user_boardName"] = boardName
          userRolesArrUnique.append(roleObj)

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
              try:
                if obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == True:
                  observationSubQuestionsObj['solution_type'] = "observation_with_rubric"
                elif obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == False:
                  observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
                else:
                  observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
              except KeyError:
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

              entityType =obSub['entityType']
              observationSubQuestionsObj[entityType] = str(obSub['entityId'])
              observationSubQuestionsObj[entityType+'Name'] = obSub['entityInformation']['name']
              observationSubQuestionsObj[entityType+'ExternalId'] = obSub['entityInformation']['externalId']        

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
                  observationSubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
              except KeyError:
                  observationSubQuestionsObj['isAPrivateProgram'] = True

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
                      if str(quesCQ["_id"]) == str(answer["qid"]) :
                        observationSubQuestionsObj['section'] = secCQ["code"]
              solutionObj = {}
              for solu in solCollec.find({'_id':ObjectId(obSub['solutionId'])}):
                solutionObj = solu

              if solutionObj:
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

              if 'observationInformation' in obSub :
               if 'name' in obSub['observationInformation']:
                 observationSubQuestionsObj['observationName'] = obSub['observationInformation']['name']
               else :
                 try:
                  for ob in obsCollec.find({'_id':obSub['observationId']},{'name':1}):
                   observationSubQuestionsObj['observationName'] = ob['name']
                 except KeyError :
                  observationSubQuestionsObj['observationName'] = ''
              else :
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
              try:
               if answer['payload']['labels']:
                 observationSubQuestionsObj['questionResponseLabel'] = responseLabel
               else:
                 observationSubQuestionsObj['questionResponseLabel'] = ''
              except KeyError :
                 observationSubQuestionsObj['questionResponseLabel'] = ''
              observationSubQuestionsObj['questionExternalId'] = quesexternalId
              observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]
              for ques in questionsCollec.find({'_id':ObjectId(ans['qid'])}):
                  observationSubQuestionsObj['sectionHeader'] = ques['sectionHeader']
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
                    multipleFiles = config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + filedetail['sourcePath']
                    fileCnt = fileCnt + 1
                  else:
                    multipleFiles = multipleFiles + ' ; ' + config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + filedetail['sourcePath']
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
                        if str(quesCQInst["_id"]) == str(ans["qid"]) :
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
                    if type(ansFn['payload']['labels']) == list:
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
                            json.dump(finalObj, f)
                            f.write("\n")
                            successLogger.debug("Send Obj to Azure")
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
                          json.dump(finalObj, f)
                          f.write("\n")
                          successLogger.debug("Send Obj to Azure")
                    else:
                      if (len(userRolesArrUnique)) > 0:
                        for usrRol in userRolesArrUnique:
                          finalObj = {}
                          finalObj = creatingObj(
                            ansFn,
                            ques['externalId'],
                            ansFn['value'],
                            instNumber,
                            ansFn['payload']['labels'],
                            entityLatitudeQuesFn,
                            entityLongitudeQuesFn,
                            usrRol
                          )
                          if finalObj["completedDate"]:
                            json.dump(finalObj, f)
                            f.write("\n")
                            successLogger.debug("Send Obj to Azure")
                      else:
                        finalObj = {}
                        finalObj = creatingObj(
                          ansFn,
                          ques['externalId'],
                          ansFn['value'],
                          instNumber,
                          ansFn['payload']['labels'],
                          entityLatitudeQuesFn,
                          entityLongitudeQuesFn,
                          None
                        )
                        if finalObj["completedDate"]:
                          json.dump(finalObj, f)
                          f.write("\n")
                          successLogger.debug("Send Obj to Azure")
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
                                json.dump(finalObj, f)
                                f.write("\n")
                                successLogger.debug("Send Obj to Azure")
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
                              json.dump(finalObj, f)
                              f.write("\n")
                              successLogger.debug("Send Obj to Azure") 
                            
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
                                  json.dump(finalObj, f)
                                  f.write("\n")
                                  successLogger.debug("Send Obj to Azure")
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
                                json.dump(finalObj, f)
                                f.write("\n")
                                successLogger.debug("Send Obj to Azure")
                            labelIndex = labelIndex + 1
                    except KeyError:
                      pass
            try:
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
                if type(instances) == list :
                   for instance in instances:
                    fetchingQuestiondetails(instance, inst_cnt, entityLatitude, entityLongitude)
                else :
                 for instance in instances.values():
                  fetchingQuestiondetails(instance, inst_cnt, entityLatitude, entityLongitude)
            except KeyError:
              pass
    cursorMongo.close()
except Exception as e:
  errorLogger.error(e, exc_info=True)

with open('sl_observation.json', 'w') as f:
 valuee = obsSubCollec.find({'$and' : [{"status":"completed"},{"solutionId":ObjectId("612cf271f1d64e7b26786524")},{"programId":ObjectId("60e7d35bf2b6e70788065fd2")}]}).count()
 print(valuee)
 for msg_data in obsSubCollec.find({'$and' : [{"status":"completed"},{"solutionId":ObjectId("612cf271f1d64e7b26786524")},{"programId":ObjectId("60e7d35bf2b6e70788065fd2")}]}):
    obj_arr = obj_creation(msg_data['_id'])
    



import pandas as pd
df = pd.read_json ('sl_observation.json',lines=True)
#dff = df[['createdBy','role_title','user_stateName','user_districtName','user_blockName','user_schoolUDISE_code','user_schoolName','organisation_name','programName','programExternalId','solutionName','solutionExternalId','domainName','sectionHeader','observationSubmissionId','questionExternalId','questionName','questionResponseLabel','minScore','evidences','remarks']]
#

df = df[df["solution_type"] == "observation_with_rubric"]

dff = df[['createdBy','role_title','user_stateName','user_districtName','user_blockName','user_schoolUDISE_code','user_schoolName','organisation_name','programName','programExternalId','solutionName','solutionExternalId','observationSubmissionId','domainName','sectionHeader','questionExternalId','questionName','questionResponseLabel','minScore','evidences','remarks','solutionId']] 
df1 = dff.rename(columns={"questionName":"Question","user_districtName":"District","evidences":"Evidences","questionResponseLabel":"Question_response_label","solutionExternalId":"Observation ID","user_schoolUDISE_code":"School ID","role_title":"User Sub Type","minScore":"Question score","programName":"Program Name","questionExternalId":"Question_external_id","organisation_name":"Org Name","createdBy":"UUID","remarks":"Remarks","user_blockName":"Block","solutionName":"Observation Name","user_schoolName":"School Name","programExternalId":"Program ID","user_stateName":"Declared State","observationSubmissionId":"observation_submission_id","domainName":"Domain Name","sectionHeader":"Criteria Name"})


df1 = df1.drop_duplicates()
df1.to_csv ('questio_report.csv', index = None)

