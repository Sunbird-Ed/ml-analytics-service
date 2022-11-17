# -----------------------------------------------------------------
# Name : sl_py_observation_streaming.py
# Author : Ashwini.E , Shakthieshwari.A, Snehangsu De
# Description : Program to read data from one kafka topic and 
#   produce it to another kafka topic 
# -----------------------------------------------------------------

import time
import logging
import os, json
import argparse
import datetime
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId
from azure.storage.blob import BlockBlobService
from configparser import ConfigParser,ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

parser = argparse.ArgumentParser()
parser.add_argument('--solution_id', help='Please enter solution id')
args = parser.parse_args()

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = RotatingFileHandler(config.get('LOGS', 'observation_streaming_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'observation_streaming_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS', 'observation_streaming_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'observation_streaming_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

domArr = []


#db production
client = MongoClient(config.get('MONGO', 'mongo_url'))
db = client[config.get('MONGO', 'database_name')]
obsSubCollec = db[config.get('MONGO', 'observation_sub_collection')]
solCollec = db[config.get('MONGO', 'solutions_collection')]
obsCollec = db[config.get('MONGO', 'observations_collection')]
questionsCollec = db[config.get('MONGO', 'questions_collection')]
criteriaQuestionsCollec = db[config.get('MONGO', 'criteria_questions_collection')]
criteriaCollec = db[config.get('MONGO', 'criteria_collection')]
programsCollec = db[config.get('MONGO', 'programs_collection')]

try:
  def removeduplicate(it):
    seen = []
    for x in it:
      if x not in seen:
        yield x
        seen.append(x)
except Exception as e:
  errorLogger.error(e, exc_info=True)

def orgName(val):
  orgarr = []
  if val is not None:
    for org in val:
        orgObj = {}
        if org["isSchool"] == False:
            orgObj['orgId'] = org['organisationId']
            orgObj['orgName'] = org["orgName"]
            orgarr.append(orgObj)
  return orgarr

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

      userSubType = None
      if 'userRoleInformation' in obSub:
       userSubType = obSub["userRoleInformation"]["role"]  
      
      rootOrgId = None
      boardName = None
      user_type = None

      try:
          if obSub["userProfile"] :
              if "rootOrgId" in obSub["userProfile"] and obSub["userProfile"]["rootOrgId"]:
                  rootOrgId = obSub["userProfile"]["rootOrgId"]
              if "framework" in obSub["userProfile"] and obSub["userProfile"]["framework"]:
                 if "board" in obSub["userProfile"]["framework"] and len(obSub["userProfile"]["framework"]["board"]) > 0:
                  boardName = ",".join(obSub["userProfile"]["framework"]["board"])
              try:
                temp_userType = set([types["type"] for types in obSub["userProfile"]["profileUserTypes"]])
                user_type = ", ".join(temp_userType)
              except KeyError:
                pass
      except KeyError :
          pass

      obsAppName = None
      try :
        obsAppName = obSub["appInformation"]["appName"].lower()
      except KeyError :
        obsAppName = config.get("ML_APP_NAME", "survey_app")
      userRolesArrUnique = []
      roleObj = {}
      roleObj["role_title"] = userSubType
      roleObj["user_boardName"] = boardName
      roleObj["user_type"] = user_type
      if "userProfile" in obSub and len(obSub["userProfile"]["userLocations"])>0:
       for ent in obSub["userProfile"]["userLocations"]:
          roleObj["user_"+ent["type"]+"Name"] = ent["name"]
          roleObj[ent["type"]+"_externalId"] = ent["id"]
          roleObj[ent["type"]+"_code"] = ent["code"]
      userRolesArrUnique.append(roleObj)

      try:
        orgArr = orgName(obSub["userProfile"]["organisations"])
        if len(orgArr) >0:
          for org in orgArr:
            for obj in userRolesArrUnique:
              obj["organisation_id"] = org["orgId"]
              obj["organisation_name"] = org["orgName"]
      except KeyError:
          pass

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
              answer, quesexternalId, ans_val, instNumber, responseLabel, usrRolFn
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

              observationSubQuestionsObj['entity'] = str(obSub['entityId'])
              observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
              observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name'] 

              entityType =obSub['entityType']
              observationSubQuestionsObj[entityType] = str(obSub['entityId'])
              observationSubQuestionsObj[entityType+'Name'] = obSub['entityInformation']['name']
              observationSubQuestionsObj[entityType+'ExternalId'] = obSub['entityInformation']['externalId']        


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
                 if answer['responseType'] == 'text':
                  observationSubQuestionsObj['questionResponseLabel'] = "'"+ re.sub("\n|\"","",responseLabel) +"'"
                 else :
                  observationSubQuestionsObj['questionResponseLabel'] = responseLabel
               else:
                 observationSubQuestionsObj['questionResponseLabel'] = ''
              except KeyError :
                 observationSubQuestionsObj['questionResponseLabel'] = ''
              observationSubQuestionsObj['questionExternalId'] = quesexternalId
              observationSubQuestionsObj['questionName'] = answer['payload']['question'][0]
              observationSubQuestionsObj['questionECM'] = answer['evidenceMethod']
              observationSubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
              observationSubQuestionsObj['completedDate'] = completedDate
              observationSubQuestionsObj['createdAt'] = createdAt
              observationSubQuestionsObj['updatedAt'] = updatedAt
              if answer['remarks'] :
               observationSubQuestionsObj['remarks'] = "'"+ re.sub("\n|\"","",answer['remarks']) +"'"
              else :
               observationSubQuestionsObj['remarks'] = None
              if len(answer['fileName']):
                multipleFiles = None
                fileCnt = 1
                for filedetail in answer['fileName']:
                  if fileCnt == 1:
                    multipleFiles = config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + filedetail['sourcePath']
                    fileCnt = fileCnt + 1
                  else:
                    multipleFiles = multipleFiles + ' , ' + config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + filedetail['sourcePath']
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

            def fetchingQuestiondetails(ansFn, instNumber):        
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
              fetchingQuestiondetails(ans,inst_cnt)
             elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
              inst_cnt =0
              for instances in ans['value']:
                inst_cnt = inst_cnt + 1
                if type(instances) == list :
                   for instance in instances:
                    fetchingQuestiondetails(instance, inst_cnt)
                else :
                 for instance in instances.values():
                  fetchingQuestiondetails(instance, inst_cnt)
            except KeyError:
              pass
    cursorMongo.close()
except Exception as e:
  errorLogger.error(e, exc_info=True)

with open('sl_observation.json', 'w') as f:
  for counter, msg_data in enumerate(obsSubCollec.find({"status":"completed", "solutionId": ObjectId(args.solution_id)})): 
    print(f"Count: {counter} ---- ID: {msg_data['_id']}")
    obj_arr = obj_creation(msg_data['_id'])

container_name = config.get("AZURE", "container_name")
storage_account = config.get("AZURE", "account_name")
token = config.get("AZURE", "sas_token")
local_path = config.get("OUTPUT_DIR", "observation")
blob_path = config.get("AZURE", "observations_blob_path")

blob_service_client = BlockBlobService(
    account_name=config.get("AZURE", "account_name"),
    sas_token=config.get("AZURE", "sas_token")
)


for files in os.listdir(local_path):
    if "sl_observation.json" in files:
        blob_service_client.create_blob_from_path(
            container_name,
            os.path.join(blob_path,files),
            local_path + "/" + files
        )
        
payload = {}
payload = json.loads(config.get("DRUID","observation_injestion_spec"))
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

