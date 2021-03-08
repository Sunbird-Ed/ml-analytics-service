# -----------------------------------------------------------------
# Name : sl_py_observation_streaming.py
# Author : Ashwini.E , Shakthieshwari.A
# Description :
#        This is streaming program
#        Reads the data from Kafka topic process the observation submitted data
# -----------------------------------------------------------------

# Program to read data from one kafka topic and produce it to another kafka topic
import faust
from pymongo import MongoClient
from bson.objectid import ObjectId
import csv,os
import json
import boto3
import datetime
from datetime import date,time
import requests
import argparse
from kafka import KafkaConsumer
from kafka import KafkaProducer
from configparser import ConfigParser,ExtendedInterpolation
import logging
import logging.handlers
import time
from logging.handlers import TimedRotatingFileHandler

config_path = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')


successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.\
    RotatingFileHandler(config.get('LOGS','observation_streaming_success_log_filename'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_success_log_filename'),
                                                when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.\
    RotatingFileHandler(config.get('LOGS','observation_streaming_error_log_filename'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','observation_streaming_error_log_filename'),
                                              when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
 kafka_url = (config.get("KAFKA","kafka_url"))
 #consume the message from kafka topic
 app = faust.App('sl_observation_diksha_faust',broker='kafka://'+kafka_url,value_serializer='raw',
                 web_port=7001,broker_max_poll_records=500)
 rawTopicName = app.topic(config.get("KAFKA","kafka_raw_data_topic"))
 producer = KafkaProducer(bootstrap_servers=[kafka_url])

 #db production
 clientdev = MongoClient(config.get('MONGO','mongo_url'))
 dbdev = clientdev[config.get('MONGO','database_name')]

 observationSubmissionsDevCollec = dbdev[config.get('MONGO','observation_sub_collec')]
 solutionsDevCollec = dbdev[config.get('MONGO','solutions_collec')]
 observationDevCollec = dbdev[config.get('MONGO','observations_collec')]
 entityTypeDevCollec = dbdev[config.get('MONGO','entity_type_collec')]
 questionsDevCollec = dbdev[config.get('MONGO','questions_collec')]
 criteriaDevCollec = dbdev[config.get('MONGO','criteria_collec')]
 entitiesDevCollec = dbdev[config.get('MONGO','entities_collec')]
 programsDevCollec = dbdev[config.get('MONGO','programs_collec')]
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def removeduplicate(it):
     seen = []
     for x in it:
        if x not in seen:
             yield x
             seen.append(x)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 #getKeyclock api to  generate authentication token
 def get_keyclock_accesstoken():
    url_getkeyclock = config.get("URL","url_getkeyclock")
    headers_getkeyclock = {'Content-Type': 'application/x-www-form-urlencoded'}
    body_getkeyclock = {"grant_type":config.get("API_HEADESRS","grant_type"),
                        "client_id":config.get("API_HEADESRS","client_id"),
                        "refresh_token":config.get("API_HEADESRS","refresh_token")}

    responsegetkeyclock = requests.post(url_getkeyclock, data=body_getkeyclock,headers=headers_getkeyclock)
    if responsegetkeyclock.status_code == 200:
        successLogger.debug("getkeyclock api")
        return responsegetkeyclock.json()
    else:
        errorLogger.error("Failure in getkeyclock API")
        errorLogger.error(responsegetkeyclock)
        errorLogger.error(responsegetkeyclock.text)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def getRelatedEntity(entityId,accessToken):
          urlEntityRelated = config.get("URL","base_url") + "/" + config.get("URL","url_entity_related") + str(entityId)
          headersEntityRelated ={
          'Content-Type': config.get("API_HEADERS","content_type"),
          'Authorization': "Bearer "+ config.get("API_HEADESRS","authorization"),
          'X-authenticated-user-token': accessToken,
          'X-Channel-id' : config.get("API_HEADESRS","channel-id")
          }
          responseEntityRelated = requests.get(urlEntityRelated, headers=headersEntityRelated)
          if responseEntityRelated.status_code == 200 :
              successLogger.debug("entityRelated api")
              return responseEntityRelated.json()
          else:
              errorLogger.error(" Failure in EntityRelatedApi ")
              errorLogger.error(responseEntityRelated)
              errorLogger.error(responseEntityRelated.text)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def syncUser(userId,accessToken):
    urlSyncUser = config.get("URL","sunbird_api_base_url_ip") + "/" + config.get("URL","sunbrid_api_url_syncuser")
    headersSyncUser ={
          'Content-Type': config.get("API_HEADERS","content_type"),
          'Authorization': "Bearer "+ config.get("API_HEADESRS","authorization"),
          'X-authenticated-user-token': accessToken,
          'X-Channel-id' : config.get("API_HEADESRS","channel-id")
    }
    body_sync_user = {"params": {},"request": {"objectType": "user","objectIds": [userId]}}
    responseSyncUser = requests.post(urlSyncUser, headers=headersSyncUser,data=json.dumps(body_sync_user))
    if responseSyncUser.status_code == 200 :
        successLogger.debug("user sync api")
        return True
    else :
         errorLogger.error("user sync api failed")
         errorLogger.error(responseSyncUser)
         errorLogger.error(responseSyncUser.text)
except Exception as e:
  errorLogger.error(e,exc_info=True)
         
try:
 def readUser(userId,accessToken,userSyncCnt):
    queryStringReadUser = "?fields=completeness%2CmissingFields%2ClastLoginTime%2Ctopics%2Corganisations%2Croles%2Clocations%2Cdeclarations"
    urlReadUser = config.get("URL","sunbird_api_base_url_ip") + "/" + config.get("URL","sunbrid_api_url_readuser") \
                  + "/" + str(userId) + queryStringReadUser
    headersReadUser ={
          'Content-Type': config.get("API_HEADERS","content_type"),
          'Authorization': "Bearer "+ config.get("API_HEADESRS","authorization"),
          'X-authenticated-user-token': accessToken,
          'X-Channel-id' : config.get("API_HEADESRS","channel-id")
    }
    responseReadUser = requests.get(urlReadUser, headers=headersReadUser)
    if responseReadUser.status_code == 200 :
        successLogger.debug("read user api")
        return responseReadUser.json()
    else:
        errorLogger.error("read user api failed")
        errorLogger.error(responseReadUser)
        errorLogger.error(responseReadUser.text)        
        if responseReadUser.status_code == 404 :
              responseReadUser = responseReadUser.json()
              if responseReadUser["params"]["status"] == "USER_NOT_FOUND":
                 syncUserStatus = syncUser(userId,accessToken)
                 if syncUserStatus == True and userSyncCnt == 1:
                    userSyncCnt = userSyncCnt + 1
                    readUser(userId,accessToken,userSyncCnt)
except Exception as e:
  errorLogger.error(e,exc_info=True)

try:
 def getUserRoles(userId,accessToken):
          urlUserRoles = config.get("URL","base_url") + "/" + config.get("URL","url_user_profile_api") + str(userId)
          headersUserRoles ={
          'Content-Type': config.get("API_HEADESRS","content_type"),
          'Authorization': "Bearer "+ config.get("API_HEADESRS","authorization"),
          'X-authenticated-user-token': accessToken,
          'X-Channel-id' : config.get("API_HEADESRS","channel-id")
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
  errorLogger.error(e,exc_info=True)

try:
 def obj_creation(msg_id):
  data_keyclock = {}
  data_keyclock = get_keyclock_accesstoken()
  tokenKeyCheck = None
  tokenKeyCheck = "access_token" in data_keyclock 
  if tokenKeyCheck == True :
   accessToken= data_keyclock['access_token']
   successLogger.debug("Observation Submission Id : " + str(msg_id))
   cursorMongo = observationSubmissionsDevCollec.find({'_id':ObjectId(msg_id)}, no_cursor_timeout=True)
   for obSub in cursorMongo :
    observationSubQuestionsArr = []
    completedDate = str(datetime.datetime.date(obSub['completedDate'])) + 'T' + \
                    str(datetime.datetime.time(obSub['completedDate'])) + 'Z'
    createdAt = str(datetime.datetime.date(obSub['createdAt'])) + 'T' + \
                str(datetime.datetime.time(obSub['createdAt'])) + 'Z'
    updatedAt = str(datetime.datetime.date(obSub['updatedAt'])) + 'T' + \
                str(datetime.datetime.time(obSub['updatedAt'])) + 'Z'
    evidencesArr = [ v for v in obSub['evidences'].values() ]
    evidence_sub_count = 0
    entityId = obSub['entityId']

    # fetch entity latitude and longitude from the database
    entityLatitude = None
    entityLongitude = None
    for ent in entitiesDevCollec.find({'_id':ObjectId(entityId)}):
         try :
          if ent['metaInformation']['gpsLocation'] :
              gpsLocation = (ent['metaInformation']['gpsLocation']).split(',')
              entityLatitude = gpsLocation[0]
              entityLongitude = gpsLocation[1]
         except KeyError :
             entityLatitude = None
             entityLongitude = None
             pass
    userSyncCnt = 1
    # fetch user name from postgres with the help of keycloak id
    queryJsonOutput = {}
    queryJsonOutput = readUser(obSub["createdBy"],accessToken,userSyncCnt)
    if queryJsonOutput :
     if queryJsonOutput["result"]["response"]["userName"] :
      userRoles = {}
      obsAppName = None
      try :
        obsAppName = obSub["appInformation"]["appName"].lower()
      except KeyError :
        obsAppName = config.get("COMMON","diksha_survey_app_name")
      if obsAppName == config.get("COMMON","diksha_survey_app_name") :
       userRoles = getUserRoles(obSub["createdBy"],accessToken)
       userRolesArr = []
       if userRoles:
        userRoleKeyCheck = "result" in userRoles
        if userRoleKeyCheck == True :
         try :
          if len(userRoles["result"]["roles"]) > 0 :
           for rol in userRoles["result"]["roles"] :
            for ent in rol["entities"]:
             userEntityRelated = None
             userEntityRelated = getRelatedEntity(ent["_id"],accessToken)
             userEntityRelatedResultKeyCheck = None
             roleObj = {}
             roleObj["role_id"] = rol["_id"]
             roleObj["role_externalId"] = rol["code"]
             roleObj["role_title"] = rol["title"]
             if userEntityRelated:
              userEntityRelatedResultKeyCheck = "result" in userEntityRelated
              if userEntityRelatedResultKeyCheck == True:
                 if userEntityRelated["result"]:
                    if (userEntityRelated["result"]["entityType"] == "district") or \
                            (userEntityRelated["result"]["entityType"] == "block") or \
                            (userEntityRelated["result"]["entityType"] == "cluster"):
                       roleObj['user_'+userEntityRelated["result"]["entityType"]+'Name'] = userEntityRelated["result"]["metaInformation"]["name"]
                    for usrEntityData in userEntityRelated["result"]["relatedEntities"]:
                        if (usrEntityData['entityType'] == "district") or \
                                (usrEntityData['entityType'] == "block") or \
                                (usrEntityData['entityType'] == "cluster") :
                           roleObj['user_'+usrEntityData['entityType']+'Name'] = usrEntityData['metaInformation']['name']
             userRolesArr.append(roleObj)
         except KeyError :
          pass
       userRolesArrUnique = []
       if len(userRolesArr) > 0:
        userRolesArrUnique = list(removeduplicate(userRolesArr))
      elif obsAppName == config.get("COMMON","diksha_integrated_app_name") :
           userRolesArrUnique = []
           roleObj = {}
           roleObj["role_id"] = "integrated_app"
           roleObj["role_externalId"] = "integrated_app"
           roleObj["role_title"] = queryJsonOutput["result"]["response"]["userSubType"]
           try :
            for usrLoc in queryJsonOutput["result"]["response"]["userLocations"]:
                roleObj['user_'+usrLoc["type"]+'Name'] = usrLoc["name"]
            userRolesArrUnique.append(roleObj)
           except KeyError :
            pass      
      entityRelated = None
      entityRelated = getRelatedEntity(entityId,accessToken)
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
                          pass
          for ans in answersArr:
                      def sequenceNumber(externalId,answer):
                         for solu in solutionsDevCollec.find({'externalId':obSub['solutionExternalId']}):
                             section =  [k for k in solu['sections'].keys()]
                             #parsing through questionSequencebyecm to get the sequence number
                             try:
                                 for num in range(len(solu['questionSequenceByEcm'][answer['evidenceMethod']][section[0]])):
                                     if solu['questionSequenceByEcm'][answer['evidenceMethod']][section[0]][num] == externalId:
                                         return num + 1
                             except KeyError:
                                 pass

                      def creatingObj(answer,quesexternalId,ans_val,instNumber,responseLabel,entityLatitudeCreateObjFn,
                                      entityLongitudeCreateObjFn,usrRolFn):
                            observationSubQuestionsObj = {}
                            observationSubQuestionsObj['userName'] = obSub['evidencesStatus'][0]['submissions'][0]['submittedByName']
                            observationSubQuestionsObj['userName'] = observationSubQuestionsObj['userName'].replace("null","")
                            observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
                            observationSubQuestionsObj['appName'] = obsAppName
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
                                 pass
                               
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
                                   observationSubQuestionsObj['distance_in_meters'] = None
                            else :
                               observationSubQuestionsObj['location_validated_with_geotag'] = 'gps location not found for school'
                               observationSubQuestionsObj['distance_in_meters'] = None                            


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
                              observationSubQuestionsObj['administrationTypes'] = obSub['entityInformation']['administrationTypes']
                            except KeyError:
                              pass
                            observationSubQuestionsObj['createdBy'] = obSub['createdBy']

                            try:
                               if obSub['isAPrivateProgram']:
                                  observationSubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                               else:
                                  observationSubQuestionsObj['isAPrivateProgram'] = False
                            except KeyError:
                                  observationSubQuestionsObj['isAPrivateProgram'] = False
                                  pass

                            try:
                             observationSubQuestionsObj['programExternalId'] = obSub['programExternalId']
                            except KeyError :
                             observationSubQuestionsObj['programExternalId'] = None
                            try:
                             observationSubQuestionsObj['programId'] = str(obSub['programId'])
                            except KeyError :
                             observationSubQuestionsObj['programId'] = None
                            try:
                             for program in programsDevCollec.find({'externalId':obSub['programExternalId']}):
                                observationSubQuestionsObj['programName'] = program['name']
                            except KeyError :
                             observationSubQuestionsObj['programName'] = None

                            observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
                            observationSubQuestionsObj['observationId'] = str(obSub['observationId'])
                            for solu in solutionsDevCollec.find({'externalId':obSub['solutionExternalId']}):
                                observationSubQuestionsObj['solutionName'] = solu['name']
                                section =  [k for k in solu['sections'].keys()]
                                observationSubQuestionsObj['section'] = section[0]
                                observationSubQuestionsObj['questionSequenceByEcm']= sequenceNumber(quesexternalId,answer)

                                try:
                                  if solu['scoringSystem'] == 'pointsBasedScoring':
                                    observationSubQuestionsObj['totalScore'] = obSub['pointsBasedMaxScore']
                                    observationSubQuestionsObj['scoreAchieved'] = obSub['pointsBasedScoreAchieved']
                                    observationSubQuestionsObj['totalpercentage'] = obSub['pointsBasedPercentageScore']
                                    observationSubQuestionsObj['maxScore'] = answer['maxScore']
                                    observationSubQuestionsObj['minScore'] = answer['scoreAchieved']
                                    observationSubQuestionsObj['percentageScore'] = answer['percentageScore']
                                    observationSubQuestionsObj['pointsBasedScoreInParent'] = answer['pointsBasedScoreInParent']
                                except KeyError:
                                  pass

                            for entTy in entityTypeDevCollec.find({'_id':obSub['entityTypeId']},{'name':1}):
                                observationSubQuestionsObj['entityType'] = entTy['name']
                            for ob in observationDevCollec.find({'_id':obSub['observationId']}):
                                observationSubQuestionsObj['observationName'] = ob['name']
                            observationSubQuestionsObj['questionId'] = str(answer['qid'])
                            observationSubQuestionsObj['questionAnswer'] = ans_val
                            observationSubQuestionsObj['questionResponseType'] = answer['responseType']
                            if answer['responseType'] == 'number':
                                if answer['payload']['labels']:
                                 observationSubQuestionsObj['questionResponseLabel_number'] = responseLabel
                                else:
                                 observationSubQuestionsObj['questionResponseLabel_number'] = ''
                            if answer['payload']['labels']:
                              observationSubQuestionsObj['questionResponseLabel'] = responseLabel
                            else:
                              observationSubQuestionsObj['questionResponseLabel'] = ''
                            observationSubQuestionsObj['questionExternalId'] = quesexternalId
                            observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]
                            observationSubQuestionsObj['questionECM'] = answer['evidenceMethod']
                            observationSubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
                            for crit in obSub["criteria"] :
                                if str(answer['criteriaId']) == str(crit["_id"]) :
                                   try:
                                    observationSubQuestionsObj['criteriaLevel'] = crit["score"]
                                   except KeyError :
                                    observationSubQuestionsObj['criteriaLevel'] = ''
                                   try:
                                    observationSubQuestionsObj['criteriaScore'] = crit["scoreAchieved"]
                                   except KeyError :
                                    observationSubQuestionsObj['criteriaScore'] = ''
                            for crit in criteriaDevCollec.find({'_id':ObjectId(answer['criteriaId'])}):
                                observationSubQuestionsObj['criteriaExternalId'] = crit['externalId']
                                observationSubQuestionsObj['criteriaName'] = crit['name']
                            observationSubQuestionsObj['completedDate'] = completedDate
                            observationSubQuestionsObj['createdAt'] = createdAt
                            observationSubQuestionsObj['updatedAt'] = updatedAt
                            observationSubQuestionsObj['remarks'] = answer['remarks']
                            if len(answer['fileName']):
                              multipleFiles = None
                              fileCnt = 1
                              for filedetail in answer['fileName']:
                                  if fileCnt == 1:
                                     multipleFiles = 'https://samikshaprod.blob.core.windows.net/samiksha/' + filedetail['sourcePath']
                                     fileCnt = fileCnt + 1
                                  else:
                                     multipleFiles = multipleFiles + ' , ' + 'https://samikshaprod.blob.core.windows.net/samiksha/' + \
                                                     filedetail['sourcePath']
                              observationSubQuestionsObj['evidences'] = multipleFiles                                  
                              observationSubQuestionsObj['evidence_count'] = len(answer['fileName'])
                            observationSubQuestionsObj['total_evidences'] = evidence_sub_count
                            # to fetch the parent question of matrix
                            if ans['responseType']=='matrix':
                               observationSubQuestionsObj['instanceParentQuestion'] = ans['payload']['question'][0]
                               observationSubQuestionsObj['instanceParentId'] = ans['qid']
                               observationSubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                               observationSubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                               for crit in criteriaDevCollec.find({'_id':ObjectId(ans['criteriaId'])}):
                                  observationSubQuestionsObj['instanceParentCriteriaExternalId'] = crit['externalId']
                                  observationSubQuestionsObj['instanceParentCriteriaName'] = crit['name']
                               observationSubQuestionsObj['instanceId'] = instNumber
                               for ques in questionsDevCollec.find({'_id':ObjectId(ans['qid'])}):
                                 observationSubQuestionsObj['instanceParentExternalId'] = ques['externalId']
                               observationSubQuestionsObj['instanceParentEcmSequence']= sequenceNumber(observationSubQuestionsObj['instanceParentExternalId'],answer)
                            else:
                               observationSubQuestionsObj['instanceParentQuestion'] = ''
                               observationSubQuestionsObj['instanceParentId'] = ''
                               observationSubQuestionsObj['instanceParentResponsetype'] =''
                               observationSubQuestionsObj['instanceId'] = instNumber
                               observationSubQuestionsObj['instanceParentExternalId'] = ''
                               observationSubQuestionsObj['instanceParentEcmSequence'] = '' 
                            observationSubQuestionsObj['user_id'] = queryJsonOutput["result"]["response"]["userName"]
                            observationSubQuestionsObj['channel'] = queryJsonOutput["result"]["response"]["rootOrgId"]
                            observationSubQuestionsObj['parent_channel'] = "SHIKSHALOKAM"
                            if usrRolFn :
                             observationSubQuestionsObj = { **usrRolFn , **observationSubQuestionsObj} 
                            observationSubQuestionsObj["submissionNumber"] = obSub["submissionNumber"]
                            observationSubQuestionsObj["submissionTitle"] = obSub["title"] 


                            return observationSubQuestionsObj
                      # fetching the question details from questions collection
                      def fetchingQuestiondetails(ansFn,instNumber,entityLatitudeQuesFn,entityLongitudeQuesFn):        
                         for ques in questionsDevCollec.find({'_id':ObjectId(ansFn['qid'])}):                           
                            if len(ques['options']) == 0:
                              try:
                               if len(ansFn['payload']['labels']) > 0:
                                if(len(userRolesArrUnique)) > 0:
                                  for usrRol in userRolesArrUnique :
                                      finalObj = {}
                                      finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],instNumber,
                                                              ansFn['payload']['labels'][0],
                                                              entityLatitudeQuesFn,entityLongitudeQuesFn,usrRol)
                                      producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                    .encode('utf-8'))
                                      producer.flush()
                                      successLogger.debug("Send Obj to Kafka")
                                else :
                                    finalObj = {}
                                    finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],instNumber,
                                                            ansFn['payload']['labels'][0],
                                                            entityLatitudeQuesFn,entityLongitudeQuesFn,None)
                                    producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                  .encode('utf-8'))
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
                                            finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],
                                                                    instNumber,ansFn['payload']['labels'][0],
                                                                    entityLatitudeQuesFn,entityLongitudeQuesFn,usrRol)
                                            producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                          .encode('utf-8'))
                                            producer.flush()
                                            successLogger.debug("Send Obj to Kafka")
                                      else :
                                          finalObj = {}
                                          finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],
                                                                  instNumber,ansFn['payload']['labels'][0],
                                                                  entityLatitudeQuesFn,entityLongitudeQuesFn,None)
                                          producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                        .encode('utf-8'))
                                          producer.flush()     
                                          successLogger.debug("Send Obj to Kafka") 
                                        
                                  elif type(ansFn['value']) == list:
                                     for ansArr in ansFn['value']:
                                         if quesOpt['value'] == ansArr:
                                            if(len(userRolesArrUnique)) > 0:
                                              for usrRol in userRolesArrUnique :
                                                finalObj = {}
                                                finalObj =  creatingObj(ansFn,ques['externalId'],ansArr,instNumber,
                                                                        quesOpt['label'],
                                                                        entityLatitudeQuesFn,
                                                                        entityLongitudeQuesFn,usrRol)
                                                producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                              .encode('utf-8'))
                                                producer.flush() 
                                                successLogger.debug("Send Obj to Kafka")
                                            else :
                                              finalObj = {}
                                              finalObj =  creatingObj(ansFn,ques['externalId'],ansArr,instNumber,
                                                                      quesOpt['label'],
                                                                      entityLatitudeQuesFn,
                                                                      entityLongitudeQuesFn,None)
                                              producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                            .encode('utf-8'))
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
                                     finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],instNumber,None,
                                                             entityLatitudeQuesFn,
                                                             entityLongitudeQuesFn,usrRol)
                                     producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                   .encode('utf-8'))
                                     producer.flush() 
                                     successLogger.debug("Send Obj to Kafka")
                               else :
                                     finalObj = {}
                                     finalObj =  creatingObj(ansFn,ques['externalId'],ansFn['value'],instNumber,None,
                                                             entityLatitudeQuesFn,entityLongitudeQuesFn,None)
                                     producer.send((config.get("KAFKA","kafka_druid_topic")), json.dumps(finalObj)
                                                   .encode('utf-8'))
                                     producer.flush()
                                     successLogger.debug("Send Obj to Kafka")
                            except KeyError:
                              pass
 
                      if ans['responseType'] == 'text' or ans['responseType'] == 'radio' \
                              or ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' \
                              or ans['responseType'] == 'number' or ans['responseType'] == 'date':
                          inst_cnt = ''
                          fetchingQuestiondetails(ans,inst_cnt,entityLatitude,entityLongitude)
                      elif ans['responseType'] == 'matrix' and len(ans['value'])>0:
                          inst_cnt =0
                          for instances in ans['value']:
                             inst_cnt = inst_cnt + 1
                             for instance in instances.values():
                                 fetchingQuestiondetails(instance,inst_cnt,entityLatitude,entityLongitude)

   cursorMongo.close()
except Exception as e:
  errorLogger.error(e,exc_info=True)

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
  errorLogger.error(e,exc_info=True)

if __name__ == '__main__':
   app.main()
