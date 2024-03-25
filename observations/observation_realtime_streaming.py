# -----------------------------------------------------------------
# Name : observation_realtime_streaming.py
# Author : 
# Description : Program to read data from one kafka topic and 
# produce it to another kafka topic 
# -----------------------------------------------------------------

import faust
import time, re
import logging
import os, json
import datetime
import requests
from pymongo import MongoClient
from bson.objectid import ObjectId
from kafka import KafkaConsumer, KafkaProducer
from configparser import ConfigParser,ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from pydruid.client import *
from pydruid.db import connect
from pydruid.query import QueryBuilder
from pydruid.utils.aggregators import *
from pydruid.utils.filters import Dimension
from urllib.parse import urlparse

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

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

kafka_url = config.get("KAFKA", "url")
#consume the message from kafka topic
app = faust.App(
  'ml_observation_faust',
  broker='kafka://'+kafka_url,
  value_serializer='raw',
  web_port=7001,
  broker_max_poll_records=500
)
rawTopicName = app.topic(config.get("KAFKA", "observation_raw_topic"))
producer = KafkaProducer(bootstrap_servers=[kafka_url])

# # Define function to check if observation submission Id exists in Druid
def check_observation_submission_id_existance(key,column_name,table_name):
  try:
      # Establish connection to Druid
      url = config.get("DRUID","sql_url")
      url = str(url)
      parsed_url = urlparse(url)

      host = parsed_url.hostname
      port = int(parsed_url.port)
      path = parsed_url.path
      scheme = parsed_url.scheme

      conn = connect(host=host, port=port, path=path, scheme=scheme)
      cur = conn.cursor()
      response = check_datasource_existence(table_name)
      if response == True:
          # Query to check existence of observation submission Id in Druid table
          query = f"SELECT COUNT(*) FROM \"{table_name}\" WHERE \"{column_name}\" = '{key}'"
          cur.execute(query)
          result = cur.fetchone()
          count = result[0]
          infoLogger.info(f"Found {count} entires in {table_name}")
          if count == 0:
              return True
          else:
              return False
      else:
          # Since the table doesn't exist, return True to allow data insertion initially 
          return True             
  except Exception as e:
      # Log any errors that occur during Druid query execution
      errorLogger.error(f"Error checking observation_submission_id existence in Druid: {e}")
   
def check_datasource_existence(datasource_name):
  try : 
      host = config.get('DRUID', 'datasource_url')
      response = requests.get(host)
      if response.status_code == 200:
        datasources = response.json()
      if datasource_name in datasources : 
        return True
      else : 
        return False
  except requests.RequestException as e:
      errorLogger.error(f"Error fetching datasources: {e}")


def flatten_json(y):
  out = {}

  def flatten(x, name=''):
    # If the Nested key-value pair is of dict type
    if isinstance(x, dict):
        for a in x:
            flatten(x[a], name + a + '-')

    # If the Nested key-value pair is of list type
    elif isinstance(x, list):
        if not x:  # Check if the list is empty
            out[name[:-1]] = "null"
        else:
            for i, a in enumerate(x):
                flatten(a, name + str(i) + '-')

    # If the Nested key-value pair is of other types
    else:
        # Replace None, empty string, or empty list with "null"
        if x is None or x == '' or x == []:
            out[name[:-1]] = "null"
        else:
            out[name[:-1]] = x

  flatten(y)
  return out

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
  def obj_creation(obSub):
    # Debug log for survey submission ID
    infoLogger.info(f"Started to process kafka event for the observation Submission Id : {obSub['_id']}. For Observation Question report")
    observation_submission_id =  str(obSub['_id'])  
    if check_observation_submission_id_existance(observation_submission_id,"observationSubmissionId","sl-observation"):
      infoLogger.info(f"No data duplection for the Submission ID : {observation_submission_id} in sl-observation ")  
      if obSub['status'] == 'completed': 
        if 'isAPrivateProgram' in obSub :
          completedDate = None
          try:
            completedDate = obSub['completedDate']
          except KeyError:
            pass
          createdAt = obSub['createdAt']
          updatedAt = obSub['updatedAt']
          evidencesArr = [ v for v in obSub['evidences'].values() ]
          evidence_sub_count = 0
          entityId = obSub['entityId']

          userSubType = None
          try :
            if 'userRoleInformation' in obSub:
              userSubType = obSub["userRoleInformation"]["role"]
          except KeyError:
            userSubType = ''

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
            obsAppName = ''
          userRolesArrUnique = []
          roleObj = {}
          roleObj["role_title"] = userSubType
          roleObj["user_boardName"] = boardName
          roleObj["user_type"] = user_type
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
                    for num in range(len(solutionObj['questionSequenceByEcm']['OB']['S1'])):
                      if solutionObj['questionSequenceByEcm']['OB']['S1'][num] == externalId:
                        return num + 1
                  except KeyError:
                    return ''

                def creatingObj(answer, quesexternalId, ans_val, instNumber, responseLabel, usrRolFn):
                  observationSubQuestionsObj = {}
                  observationSubQuestionsObj['observationSubmissionId'] = str(obSub['_id'])
                  observationSubQuestionsObj['appName'] = obsAppName
                  try:
                    if obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == True:
                      observationSubQuestionsObj['solution_type'] = "observation_with_rubric"
                    elif obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == False:
                      observationSubQuestionsObj['solution_type'] = "observation_with_rubric_no_criteria_level_report"
                    else:
                      observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
                  except KeyError:
                    observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
                  observationSubQuestionsObj['entity'] = str(obSub['entityId'])
                  observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
                  # observationSubQuestionsObj['entityName'] = obSub['entityInformation']['name'] 

                  entityType =obSub['entityType']
                  observationSubQuestionsObj[entityType] = str(obSub['entityId'])
                  observed_entities = {}
                  try:
                    for values in observed_entities:
                        observationSubQuestionsObj[f'{values["type"]}Name'] = values['name']
                        observationSubQuestionsObj[f'{values["type"]}ExternalId'] = values['code']
                        observationSubQuestionsObj[f'{values["type"]}'] = values['id']
                  except KeyError:
                    pass 

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
                    observationSubQuestionsObj['programName'] = obSub['programInfo']['name']
                    observationSubQuestionsObj['programDescription'] = obSub['programInfo']['description']
                  except KeyError :
                    observationSubQuestionsObj['programName'] = ''
                    observationSubQuestionsObj['programDescription'] = ''
                  observationSubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
                  observationSubQuestionsObj['solutionId'] = str(obSub['solutionId'])
                  observationSubQuestionsObj['observationId'] = str(obSub['observationId'])
                  for critQues in obSub['criteria']:
                    if critQues['_id'] == answer['criteriaId'] :
                      observationSubQuestionsObj['criteriaExternalId'] = critQues['externalId']
                      observationSubQuestionsObj['criteriaName'] = critQues['name']
                      observationSubQuestionsObj['criteriaDescription'] = critQues['description']
                      observationSubQuestionsObj['section'] = ''
                  solutionObj = {}
                  try : 
                    if 'solutionInfo' in obSub.keys():
                      solutionObj = obSub['solutionInfo']
                      observationSubQuestionsObj['solutionName'] = str(solutionObj.get('name',''))
                      observationSubQuestionsObj['scoringSystem'] = str(solutionObj.get('scoringSystem',''))
                      observationSubQuestionsObj['solutionDescription'] = str(solutionObj.get('description',''))
                      observationSubQuestionsObj['questionSequenceByEcm'] = sequenceNumber(quesexternalId,answer,observationSubQuestionsObj['section'],obSub['solutionInfo'])
                  except KeyError:
                    observationSubQuestionsObj['solutionName'] = ''
                    observationSubQuestionsObj['scoringSystem'] = ''
                    observationSubQuestionsObj['solutionDescription'] = ''
                    observationSubQuestionsObj['questionSequenceByEcm'] = ''

                  solutionObj = obSub['solutionInfo']
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
                      observationSubQuestionsObj['observationName'] = ''
                  else :
                    observationSubQuestionsObj['observationName'] = ''

                  observationSubQuestionsObj['questionId'] = str(answer['qid'])
                  observationSubQuestionsObj['questionAnswer'] = ans_val
                  observationSubQuestionsObj['questionResponseType'] = answer['responseType']
                  if answer['responseType'] == 'number':
                    if responseLabel:
                      observationSubQuestionsObj['questionResponseLabel_number'] = responseLabel
                    else:
                      observationSubQuestionsObj['questionResponseLabel_number'] = 0
                  try:
                    if responseLabel:
                      if answer['responseType'] == 'text':
                        observationSubQuestionsObj['questionResponseLabel'] = "'"+ re.sub("\n|\"","",responseLabel) +"'"
                      else :
                        observationSubQuestionsObj['questionResponseLabel'] = responseLabel
                    else:
                      observationSubQuestionsObj['questionResponseLabel'] = ''
                  except KeyError :
                    observationSubQuestionsObj['questionResponseLabel'] = ''
                  observationSubQuestionsObj['questionExternalId'] = quesexternalId
                  observationSubQuestionsObj['questionName'] = answer['question'][0]
                  observationSubQuestionsObj['questionECM'] = answer['evidenceMethod']
                  observationSubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
                  observationSubQuestionsObj['completedDate'] = completedDate
                  observationSubQuestionsObj['createdAt'] = createdAt
                  observationSubQuestionsObj['updatedAt'] = updatedAt
                  if answer['remarks'] :
                    observationSubQuestionsObj['remarks'] = "'"+ re.sub("\n|\"","",answer['remarks']) +"'"
                  else :
                    observationSubQuestionsObj['remarks'] = ''
                  if len(answer['fileName']):
                    multipleFiles = None
                    fileCnt = 1
                    for filedetail in answer['fileName']:
                      if fileCnt == 1:
                        multipleFiles = filedetail['sourcePath']
                        fileCnt = fileCnt + 1
                      else:
                        multipleFiles = multipleFiles + ' , ' + filedetail['sourcePath']
                    observationSubQuestionsObj['evidences'] = multipleFiles                                  
                    observationSubQuestionsObj['evidence_count'] = str(len(answer['fileName']))
                  observationSubQuestionsObj['total_evidences'] = evidence_sub_count
                  # to fetch the parent question of matrix
                  if ans['responseType']=='matrix':
                    observationSubQuestionsObj['instanceParentQuestion'] = ans['question'][0]
                    observationSubQuestionsObj['instanceParentId'] = ans['qid']
                    observationSubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                    observationSubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                    #here ans[criteriaId] == criteria['criteriaId]
                    for critQuesInst in obSub['criteria']:
                      if critQuesInst['_id'] == ans['criteriaId']:
                        observationSubQuestionsObj['instanceParentCriteriaExternalId'] = critQuesInst['externalId']
                        observationSubQuestionsObj['instanceParentCriteriaExternalId'] = critQuesInst['name']
                        observationSubQuestionsObj['instanceParentSection'] = ''
                        observationSubQuestionsObj['instanceId'] = instNumber
                        observationSubQuestionsObj['instanceParentExternalId'] = quesexternalId
                      observationSubQuestionsObj['instanceParentEcmSequence']= sequenceNumber(observationSubQuestionsObj['instanceParentExternalId'], answer,
                        observationSubQuestionsObj['instanceParentSection'], obSub['solutionInfo'])
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
                  for domain in obSub['themes']:
                    parent = None
                    builder = None
                    parent = domain['name']
                    builder = implementation()
                    domObj = {}
                    try :
                      domObj['name'] = domain['name']
                      domObj['type'] = domain['type']
                      domObj['externalId']=str(domain['externalId'])
                    except KeyError:
                      domObj['name'] = ''
                      domObj['type'] = ''
                      domObj['externalId']=''
                
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

                                    try:
                                      for prj in criteria['improvement-projects']:
                                        try:
                                          prj_id.append(str(prj['_id']))
                                        except:
                                          prj_id.append("")
                                        try:
                                          title.append(prj['title'])
                                        except:
                                          title.append("")
                                        try:
                                          goal.append(prj['goal'])
                                        except:
                                          goal.append("")
                                        try:
                                          externalId.append(prj['externalId'])
                                        except:
                                          externalId.append("")
                                    except:
                                      prj_id = []
                                      title = []
                                      goal = []
                                      externalId =[]

                                    try:
                                      obj['imp_project_id'] = prj_id
                                    except KeyError:
                                      obj['imp_project_id'] = []
                                    try:
                                      obj['imp_project_title'] = title
                                    except KeyError:
                                      obj['imp_project_title'] = []
                                    try :
                                      obj['imp_project_goal'] = goal
                                    except KeyError:
                                      obj['imp_project_goal'] = []
                                    try:
                                      obj['imp_project_externalId'] = externalId
                                    except KeyError:
                                      obj['imp_project_externalId'] = []
                                  except KeyError:
                                    pass
                              if type(obj['externalId']) != str:
                                for cri in obSub['criteria']:
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
                        try :
                          for theme in obSub['themes']:
                            observationSubQuestionsObj['childName'] = theme['name']
                            observationSubQuestionsObj['ancestorName'] = theme['parent']
                            observationSubQuestionsObj['childType'] = theme['type']
                            observationSubQuestionsObj['childExternalid'] = theme['externalId']
                        except KeyError :
                          observationSubQuestionsObj['childName'] = ''
                          observationSubQuestionsObj['ancestorName'] = ''
                          observationSubQuestionsObj['childType'] = ''
                          observationSubQuestionsObj['childExternalid'] = ''

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
                    observationSubQuestionsObj["criteriaLevelReport"] = str(obSub["criteriaLevelReport"])
                  except KeyError :
                    observationSubQuestionsObj["criteriaLevelReport"] = False
                    
                  try:
                    observationSubQuestionsObj["isRubricDriven"] = obSub["isRubricDriven"]
                  except KeyError :
                    observationSubQuestionsObj["isRubricDriven"] = False

                  flatten_userprofile = flatten_json(obSub['userProfile'])
                  new_dict = {}
                  for key in flatten_userprofile:
                    string_without_integer = re.sub(r'\d+', '', key)
                    updated_string = string_without_integer.replace("--", "-")
                    # Check if the value associated with the key is not None
                    if flatten_userprofile[key] is not None:
                        if updated_string in new_dict:
                            # Perform addition only if both values are not None
                            if new_dict[updated_string] is not None:
                                new_dict[updated_string] += "," + str(flatten_userprofile[key])
                            else:
                                new_dict[updated_string] = str(flatten_userprofile[key])
                        else:
                            new_dict[updated_string] = str(flatten_userprofile[key])

                  observationSubQuestionsObj['userProfile'] = str(new_dict)
                  return observationSubQuestionsObj

                def fetchingQuestiondetails(ansFn, instNumber):        
                    if (len(ansFn['options']) == 0) or (('options' in ansFn.keys()) == False):
                      try:
                          if(len(userRolesArrUnique)) > 0:
                            for usrRol in userRolesArrUnique :
                              finalObj = {}
                              finalObj =  creatingObj(
                                ansFn,ansFn['externalId'],
                                ansFn['value'],instNumber,
                                ansFn['value'],
                                usrRol
                              )
                              if finalObj["completedDate"]:
                                producer.send(
                                  (config.get("KAFKA", "observation_druid_topic")), 
                                  json.dumps(finalObj).encode('utf-8')
                                )
                                producer.flush()
                                infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                          else :
                            finalObj = {}
                            finalObj =  creatingObj(
                              ansFn,ansFn['externalId'],
                              ansFn['value'],
                              instNumber,
                              ansFn['value'],
                              None
                            ) 
                            if finalObj["completedDate"]:
                              producer.send(
                                (config.get("KAFKA", "observation_druid_topic")), 
                                json.dumps(finalObj).encode('utf-8')
                              )
                              producer.flush()
                              infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                      except KeyError:
                        pass
                    else:
                      labelIndex = 0
                      for quesOpt in ansFn['options']:
                        try:
                          if type(ansFn['value']) == str or type(ansFn['value']) == int:
                            if quesOpt['value'] == ansFn['value'] :
                              if(len(userRolesArrUnique)) > 0:
                                for usrRol in userRolesArrUnique :
                                  finalObj = {}
                                  finalObj =  creatingObj(
                                    ansFn,
                                    ansFn['externalId'],
                                    ansFn['value'],
                                    instNumber,
                                    quesOpt['label'],
                                    usrRol
                                  )
                                  if finalObj["completedDate"]:
                                    producer.send(
                                      (config.get("KAFKA", "observation_druid_topic")), 
                                      json.dumps(finalObj).encode('utf-8')
                                    )
                                    producer.flush()
                                    infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                              else :
                                finalObj = {}
                                finalObj =  creatingObj(
                                  ansFn,ansFn['externalId'],
                                  ansFn['value'],
                                  instNumber,
                                  quesOpt['label'],
                                  None
                                )
                                if finalObj["completedDate"]:
                                  producer.send(
                                    (config.get("KAFKA", "observation_druid_topic")), 
                                    json.dumps(finalObj).encode('utf-8')
                                  )
                                  producer.flush()
                                  infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                              
                          elif type(ansFn['value']) == list:
                            for ansArr in ansFn['value']:
                              if quesOpt['value'] == ansArr:
                                if(len(userRolesArrUnique)) > 0:
                                  for usrRol in userRolesArrUnique :
                                    finalObj = {}
                                    finalObj =  creatingObj(
                                      ansFn,
                                      ansFn['externalId'],
                                      ansArr,
                                      instNumber,
                                      quesOpt['label'],
                                      usrRol
                                    )
                                    if finalObj["completedDate"]:
                                      producer.send(
                                        (config.get("KAFKA", "observation_druid_topic")), 
                                        json.dumps(finalObj).encode('utf-8')
                                      )
                                      producer.flush()
                                      infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                else :
                                  finalObj = {}
                                  finalObj =  creatingObj(
                                    ansFn,
                                    ansFn['externalId'],
                                    ansArr,
                                    instNumber,
                                    quesOpt['label'],
                                    None
                                  )
                                  if finalObj["completedDate"]:
                                    producer.send(
                                      (config.get("KAFKA", "observation_druid_topic")), 
                                      json.dumps(finalObj).encode('utf-8')
                                    )
                                    producer.flush()
                                    infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                labelIndex = labelIndex + 1
                        except KeyError:
                          pass
                try:
                  if (
                    ans['responseType'] == 'text' or ans['responseType'] == 'radio' or 
                    ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or 
                    ans['responseType'] == 'number' or ans['responseType'] == 'date'):   
                    inst_cnt = ''
                    fetchingQuestiondetails(ans,inst_cnt)
                  elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
                    inst_cnt = 0
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
      else:
          infoLogger.info(f"Observation Submission is not in completed status" )
    else:
       infoLogger.info(f"observation_Submission_id {observation_submission_id} is already exists in the sl-observation datasource.") 
    infoLogger.info(f"Completed processing kafka event for the Observation Submission Id : {obSub['_id']}. For Observation Question report ")                    
except Exception as e:
  errorLogger.error(e, exc_info=True)

# Main data extraction function
try:
  def main_data_extraction(obSub):
    '''Function to process observation submission data before sending it to Kafka topics'''
    try:
      infoLogger.info(f"Starting to process kafka event for the observation Submission Id : {obSub['_id']}. For Observation Status report")
      # Initialize dictionary for storing observation submission data
      observationSubQuestionsObj = {}
      observation_status = {}
      
      # Extract various attributes from observation submission object
      observationSubQuestionsObj['observationId'] = str(obSub.get('observationId', ''))
      observationSubQuestionsObj['observation_name'] = str(obSub.get('observationInformation', {}).get('name', ''))
      observationSubQuestionsObj['observation_submission_id'] = obSub.get('_id', '')
      try:
        observationSubQuestionsObj['createdBy'] = obSub['createdBy']
      except KeyError:
        observationSubQuestionsObj['createdBy'] = ''
      observationSubQuestionsObj['entity'] = str(obSub['entityId'])
      observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
      observationSubQuestionsObj['entityType'] =obSub['entityType']
      observationSubQuestionsObj["solutionId"] = obSub["solutionId"],
      observationSubQuestionsObj["solutionExternalId"] = obSub["solutionExternalId"]
      try:
        if obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == True:
          observationSubQuestionsObj['solution_type'] = "observation_with_rubric"
        elif obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == False:
          observationSubQuestionsObj['solution_type'] = "observation_with_rubric_no_criteria_level_report"
        else:
          observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"
      except KeyError:
        observationSubQuestionsObj['solution_type'] = "observation_with_out_rubric"

      try:
        observationSubQuestionsObj['completedDate'] = obSub['completedDate']
      except KeyError:
          observationSubQuestionsObj['completedDate'] = obSub['createdAt']
      # Check if 'isAPrivateProgram' key exists
      try:
          observationSubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
      except KeyError:
          observationSubQuestionsObj['isAPrivateProgram'] = True
      # user profile creation
      flatten_userprofile = flatten_json(obSub['userProfile'])
      new_dict = {}
      for key in flatten_userprofile:
          string_without_integer = re.sub(r'\d+', '', key)
          updated_string = string_without_integer.replace("--", "-")
          # Check if the value associated with the key is not None
          if flatten_userprofile[key] is not None:
              if updated_string in new_dict:
                  # Perform addition only if both values are not None
                  if new_dict[updated_string] is not None:
                      new_dict[updated_string] += "," + str(flatten_userprofile[key])
                  else:
                      new_dict[updated_string] = str(flatten_userprofile[key])
              else:
                  new_dict[updated_string] = str(flatten_userprofile[key])

      observationSubQuestionsObj['userProfile'] = str(new_dict)

      # Before attempting to access the list, check if it is non-empty
      profile_user_types = obSub.get('userProfile', {}).get('profileUserTypes', [])
      if profile_user_types:
          # Access the first element of the list if it exists
          user_type = profile_user_types[0].get('type', None)
      else:
          # Handle the case when the list is empty
          user_type = None
      observationSubQuestionsObj['user_type'] = user_type

      observationSubQuestionsObj['solutionExternalId'] = obSub.get('solutionExternalId', '')
      observationSubQuestionsObj['solutionId'] = obSub.get('solutionId', '')

      for location in obSub.get('userProfile', {}).get('userLocations', []):
          name = location.get('name')
          type_ = location.get('type')
          if name and type_:
              observationSubQuestionsObj[type_] = name
      

      orgArr = orgName(obSub.get('userProfile', {}).get('organisations',None))
      if orgArr:
          # observationSubQuestionsObj['schoolId'] = orgArr[0].get("organisation_id")
          observationSubQuestionsObj['organisation_name'] = orgArr[0].get("organisation_name")
      else:
          # observationSubQuestionsObj['schoolId'] = None
          observationSubQuestionsObj['organisation_name'] = None
      
      # Insert data to sl-observation-meta druid datasource if status is anything 
      _id = observationSubQuestionsObj.get('observation_submission_id', None)
      try:
          if _id:
                if check_observation_submission_id_existance(_id,"observation_submission_id","sl-observation-meta"):
                    infoLogger.info(f"No data duplection for the Submission ID : {_id} in sl-observation-meta datasource")
                    # Upload observation submission data to Druid topic
                    producer.send((config.get("KAFKA", "observation_meta_druid_topic")), json.dumps(observationSubQuestionsObj).encode('utf-8'))  
                    producer.flush()
                    infoLogger.info(f"Data with submission_id {_id} is being inserted into the sl-observation-meta datasource.")
                else:
                    infoLogger.info(f"Data with submission_id {_id} is already exists in the sl-observation-meta datasource.")
      except Exception as e :
          # Log any errors that occur during data ingestion
          errorLogger.error("====== An error was found during data ingestion in the sl-observation-meta datasource ======")
          errorLogger.error(e,exc_info=True)


      # Insert data to sl-observation-status-started druid datasource if status is started
      if obSub['status'] == 'started':
          observation_status['observation_submission_id'] = obSub['_id']
          try:
            observation_status['started_at'] = obSub['completedDate']
          except KeyError:
            observation_status['started_at'] = ''
          _id = observation_status.get('observation_submission_id', None) 
          try : 
              if _id:
                  if check_observation_submission_id_existance(_id,"observation_submission_id","sl-observation-status-started"):
                      infoLogger.info(f"No data duplection for the Submission ID : {_id} in sl-observation-status-started datasource")
                      # Upload observation status data to Druid topic
                      producer.send((config.get("KAFKA", "observation_started_druid_topic")), json.dumps(observation_status).encode('utf-8'))
                      producer.flush()
                      infoLogger.info(f"Data with submission_id {_id} is being inserted into the sl-observation-status-started datasource.")
                  else:       
                      infoLogger.info(f"Data with submission_id {_id} is already exists in the sl-observation-status-started datasource.")
          except Exception as e :
              # Log any errors that occur during data ingestion
              errorLogger.error("====== An error was found during data ingestion in the sl-observation-status-started datasource ======")
              errorLogger.error(e,exc_info=True)  

      
      # Insert data to sl-observation-status-started druid datasource if status is inprogress
      elif obSub['status'] == 'inprogress':
          observation_status['observation_submission_id'] = obSub['_id']
          observation_status['inprogress_at'] = obSub['completedDate']
          _id = observation_status.get('observation_submission_id', None) 
          try : 
              if _id:
                  if check_observation_submission_id_existance(_id,"observation_submission_id","sl-observation-status-inprogress"):
                      infoLogger.info(f"No data duplection for the Submission ID : {_id} in sl-observation-status-inprogress datasource")
                      # Upload observation status data to Druid topic
                      producer.send((config.get("KAFKA", "observation_inprogress_druid_topic")), json.dumps(observation_status).encode('utf-8'))
                      producer.flush()
                      infoLogger.info(f"Data with submission_id {_id} is being inserted into the sl-observation-status-inprogress datasource.")
                  else:       
                      infoLogger.info(f"Data with submission_id {_id} is already exists in the sl-observation-status-inprogress datasource.")
          except Exception as e :
              # Log any errors that occur during data ingestion
              errorLogger.error("====== An error was found during data ingestion in the sl-observation-status-inprogress datasource ======")
              errorLogger.error(e,exc_info=True)  


      elif obSub['status'] == 'completed':
          observation_status['observation_submission_id'] = obSub['_id']
          observation_status['completed_at'] = obSub['completedDate']
          _id = observation_status.get('observation_submission_id', None) 
          try : 
              if _id:
                  if check_observation_submission_id_existance(_id,"observation_submission_id","sl-observation-status-completed"):
                      infoLogger.info(f"No data duplection for the Submission ID : {_id} in sl-observation-status-completed datasource")
                      # Upload observation status data to Druid topic
                      producer.send((config.get("KAFKA", "observation_completed_druid_topic")), json.dumps(observation_status).encode('utf-8'))
                      producer.flush()
                      infoLogger.info(f"Data with submission_id {_id} is being inserted into the sl-observation-status-completed datasource")
                  else:       
                      infoLogger.info(f"Data with submission_id {_id} is already exists in the sl-observation-status-completed datasource")
          except Exception as e :
              # Log any errors that occur during data ingestion
              errorLogger.error("====== An error was found during data ingestion in the sl-observation-status-inprogress datasource ======")
              errorLogger.error(e,exc_info=True)  

      infoLogger.info(f"Completed processing kafka event for the observation Submission Id : {obSub['_id']}. For observation Status report")
    except Exception as e:
        # Log any errors that occur during data extraction
        errorLogger.error(e, exc_info=True)
except Exception as e:
    # Log any errors that occur during data extraction
    errorLogger.error(e, exc_info=True)

    
try:
    @app.agent(rawTopicName)
    async def surveyFaust(consumer):
        '''Faust agent to consume messages from Kafka and process them'''
        async for msg in consumer:
            try:
              msg_val = msg.decode('utf-8')
              msg_data = json.loads(msg_val)
              infoLogger.info("========== START OF OBSERVATION SUBMISSION EVENT PROCESSING ==========")
              obj_creation(msg_data)
              main_data_extraction(msg_data)
              infoLogger.info("********** END OF OBSERVATION SUBMISSION EVENT PROCESSING **********")
            except KeyError as ke:
                # Log KeyError
                errorLogger.error(f"KeyError occurred: {ke}")
except Exception as e:
    # Log any other exceptions
    errorLogger.error(f"Error in observationFaust function: {e}")

if __name__ == '__main__':
  app.main()

