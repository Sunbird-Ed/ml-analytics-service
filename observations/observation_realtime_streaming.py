# -----------------------------------------------------------------
# Name : observation_realtime_streaming.py
# Author : Prashant
# Description : Program to read data from one kafka topic and 
# produce it to another kafka topic 
# -----------------------------------------------------------------

import faust
import time, re
import logging
import os, json
import datetime
import requests
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
from pymongo import MongoClient
from bson.objectid import ObjectId 

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

# Initialize Mongo db collection 

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
observationSubCollec = db[config.get('MONGO', 'observation_sub_collection')]

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
def check_observation_submission_id_existance(observationId,column_name,table_name):
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
    # Query to check existence of observation submission Id in Druid table
    query = f"SELECT COUNT(*) FROM \"{table_name}\" WHERE \"{column_name}\" = '{observationId}'"
    cur.execute(query)
    result = cur.fetchone()
    count = result[0]
    infoLogger.info(f"Found {count} entires in {table_name}")
    # if count == 0 means observation_submission_id not exits in the datasource
    # if count > 0 means observation_submission_id exits in datasource 
    if count == 0:
        return False
    else:
        return True 
  except Exception as e:
    # Log any errors that occur during Druid query execution
    errorLogger.error(e,exc_info=True)

def set_null_value(data):
  if ("userProfile" in data) and ("organisationName" in data):
    if config.get("OUTPUT_DIR","CAPTURE_USER_PROFILE") == "False":
      data['userProfile'] = ''
      data['organisationName'] = ''        
  return data

def send_data_to_kafka(data,topic):
  modified_data = set_null_value(data)
  future = producer.send(topic, json.dumps(modified_data).encode('utf-8'))
  producer.flush()
  record_metadata = future.get()
  message_id = record_metadata.offset
  return message_id

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
    list_message_id_ext = []
    flag_count_ext = 0
    if obSub['status'] == 'completed': 
      observationSubmissionId =  str(obSub['_id'])  
      submission_id_exits = check_observation_submission_id_existance(observationSubmissionId,"observationSubmissionId","sl-observation")
      if submission_id_exits == False:
        infoLogger.info(f"No data duplection for the Submission ID : {observationSubmissionId} in sl-observation ")  
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
          roleObj["roleTitle"] = userSubType
          roleObj["userBoardName"] = boardName
          roleObj["userType"] = user_type
          userRolesArrUnique.append(roleObj)
          try:
            orgArr = orgName(obSub["userProfile"]["organisations"])
            if len(orgArr) >0:
              for org in orgArr:
                for obj in userRolesArrUnique:
                  obj["organisationId"] = org["orgId"]
                  obj["organisationName"] = org["orgName"]
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
                      observationSubQuestionsObj['solutionType'] = "observation_with_rubric"
                    elif obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == False:
                      observationSubQuestionsObj['solutionType'] = "observation_with_rubric_no_criteria_level_report"
                    else:
                      observationSubQuestionsObj['solutionType'] = "observation_with_out_rubric"
                  except KeyError:
                    observationSubQuestionsObj['solutionType'] = "observation_with_out_rubric"
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
                      observationSubQuestionsObj['questionResponseLabelNumber'] = responseLabel
                    else:
                      observationSubQuestionsObj['questionResponseLabelNumber'] = 0
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
                    observationSubQuestionsObj['evidenceCount'] = str(len(answer['fileName']))
                  observationSubQuestionsObj['totalEvidences'] = evidence_sub_count
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
                    list_message_id = []
                    flag_count = 0      
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
                                flag_count += 1
                                try :
                                  message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                  list_message_id.append(message_id)
                                  infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                except Exception as e :
                                  infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                  errorLogger.error(e,exc_info=True)
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
                              flag_count += 1
                              try : 
                                message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                list_message_id.append(message_id)
                                infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")  
                              except Exception as e :
                                infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                errorLogger.error(e,exc_info=True)                           
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
                                    flag_count += 1
                                    try : 
                                      message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                      list_message_id.append(message_id)
                                      infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                    except Exception as e :
                                      infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                      errorLogger.error(e,exc_info=True)
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
                                  flag_count += 1
                                  try : 
                                    message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                    list_message_id.append(message_id)
                                    infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                  except Exception as e :
                                    infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                    errorLogger.error(e,exc_info=True)
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
                                      flag_count += 1
                                      try :
                                        message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                        list_message_id.append(message_id)
                                        infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                      except Exception as e:
                                        infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                        errorLogger.error(e,exc_info=True)
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
                                    flag_count += 1
                                    try: 
                                      message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "observation_druid_topic"))
                                      list_message_id.append(message_id)
                                      infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) inserted into sl-observation datasource")
                                    except Exception as e :
                                      infoLogger.info(f"Data for observationId ({finalObj['observationId']}) and questionId ({finalObj['questionId']}) Not inserted into sl-observation datasource")
                                      errorLogger.error(e,exc_info=True)
                                labelIndex = labelIndex + 1
                        except KeyError:
                          pass
                    return list_message_id,flag_count
                try:
                  if (
                    ans['responseType'] == 'text' or ans['responseType'] == 'radio' or 
                    ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or 
                    ans['responseType'] == 'number' or ans['responseType'] == 'date'):   
                    inst_cnt = ''
                    list_message_id_fetch,flag_count_fetch = fetchingQuestiondetails(ans,inst_cnt)
                    list_message_id_ext.extend(list_message_id_fetch)
                    flag_count_ext = flag_count_ext + flag_count_fetch
                  elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
                    inst_cnt = 0
                    for instances in ans['value']:
                      inst_cnt = inst_cnt + 1
                      if type(instances) == list :
                        for instance in instances:
                          list_message_id_fetch,flag_count_fetch = fetchingQuestiondetails(instance, inst_cnt)
                          list_message_id_ext.extend(list_message_id_fetch)
                          flag_count_ext = flag_count_ext + flag_count_fetch
                      else :
                        for instance in instances.values():
                          list_message_id_fetch,flag_count_fetch = fetchingQuestiondetails(instance, inst_cnt)  
                          list_message_id_ext.extend(list_message_id_fetch)
                          flag_count_ext = flag_count_ext + flag_count_fetch               
                except KeyError:
                  pass   
      else:
        infoLogger.info(f"observation_Submission_id {observationSubmissionId} is already exists in the sl-observation datasource.") 
    else:
      infoLogger.info(f"Observation Submission is not in completed status" )
    infoLogger.info(f"Completed processing kafka event for the Observation Submission Id : {obSub['_id']}. For Observation Question report ") 
    return list_message_id_ext,flag_count_ext                   
except Exception as e:
  infoLogger.info(f"Failed to process obj_creation function")
  errorLogger.error(f"Failed to process obj_creation function{e}", exc_info=True)

# Main data extraction function
try:
  def main_data_extraction(obSub):
    '''Function to process observation submission data before sending it to Kafka topics'''
    infoLogger.info(f"Starting to process kafka event for the observation Submission Id : {obSub['_id']}. For Observation Status report")
    try:
      list_message_id = []
      flag_count = 0
      observationSubmissionId =  str(obSub['_id'])
      submission_exits_in_meta = check_observation_submission_id_existance(observationSubmissionId,"observationSubmissionId","sl-observation-meta")
      if submission_exits_in_meta == False:
        infoLogger.info(f"No data duplection for the Submission ID : {observationSubmissionId} in sl-observation-meta ")  
        # Initialize dictionary for storing observation submission data
        observationSubQuestionsObj = {}
        # Extract various attributes from observation submission object
        observationSubQuestionsObj['observationId'] = str(obSub.get('observationId', ''))
        observationSubQuestionsObj['observationName'] = str(obSub.get('observationInformation', {}).get('name', ''))
        observationSubQuestionsObj['observationSubmissionId'] = obSub.get('_id', '')
        observationSubQuestionsObj['createdAt'] = obSub.get('createdAt', '')
        try:
          observationSubQuestionsObj['createdBy'] = obSub['createdBy']
        except KeyError:
          observationSubQuestionsObj['createdBy'] = ''
        observationSubQuestionsObj['entity'] = str(obSub['entityId'])
        observationSubQuestionsObj['entityExternalId'] = obSub['entityExternalId']
        observationSubQuestionsObj['entityType'] =obSub['entityType']
        observationSubQuestionsObj["solutionId"] = obSub["solutionId"],
        observationSubQuestionsObj["solutionExternalId"] = obSub["solutionExternalId"]
        try : 
          if 'solutionInfo' in obSub.keys():
            solutionObj = obSub['solutionInfo']
            observationSubQuestionsObj['solutionName'] = str(solutionObj.get('name',''))
        except KeyError:
          observationSubQuestionsObj['solutionName'] = ''
        try:
          if obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == True:
            observationSubQuestionsObj['solutionType'] = "observation_with_rubric"
          elif obSub["isRubricDriven"] == True and obSub["criteriaLevelReport"] == False:
            observationSubQuestionsObj['solutionType'] = "observation_with_rubric_no_criteria_level_report"
          else:
            observationSubQuestionsObj['solutionType'] = "observation_with_out_rubric"
        except KeyError:
          observationSubQuestionsObj['solutionType'] = "observation_with_out_rubric"

        try:
          observationSubQuestionsObj['completedDate'] = obSub['completedDate']
        except KeyError:
            observationSubQuestionsObj['completedDate'] = obSub['createdAt']
        # Check if 'isAPrivateProgram' key exists
        try:
            observationSubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
        except KeyError:
            observationSubQuestionsObj['isAPrivateProgram'] = True

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

        observationSubQuestionsObj['userType'] = user_type
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
            observationSubQuestionsObj['organisationName'] = orgArr[0].get("orgName")
        else:
            # observationSubQuestionsObj['schoolId'] = None
            observationSubQuestionsObj['organisationName'] = None
        flag_count += 1
        try : 
        # Insert data to sl-observation-meta druid datasource if status is anything 
          message_id = send_data_to_kafka(observationSubQuestionsObj,config.get("KAFKA", "observation_meta_druid_topic"))
          list_message_id.append(message_id)
          infoLogger.info(f"Data with submission_id {observationSubmissionId} is being inserted into the sl-observation-meta datasource.")
        except Exception as e:
          errorLogger.error(f"Error sending data for observationId ({observationSubmissionId}) to sl-observation-meta datasource: {e}", exc_info=True)
      else:
        infoLogger.info(f"Data with submission_id {observationSubmissionId} is already exists in the sl-observation-meta datasource.")


      # Insert data to sl-observation-status-started druid datasource if status is started
      if obSub['status'] == 'started':
        try :
          submission_exits_in_started = check_observation_submission_id_existance(observationSubmissionId,"observationSubmissionId","sl-observation-status-started")
          if submission_exits_in_started == False:
            infoLogger.info(f"No data duplection for the Submission ID : {observationSubmissionId} in sl-observation-status-started ")  
            observation_status = {}
            observation_status['observationSubmissionId'] = obSub['_id']
            try:
              observation_status['startedAt'] = obSub['createdAt']
            except KeyError:
              observation_status['startedAt'] = ''
            flag_count += 1
            try : 
              message_id = send_data_to_kafka(observation_status,config.get("KAFKA", "observation_started_druid_topic"))
              list_message_id.append(message_id)
              infoLogger.info(f"Data with submission_id {observationSubmissionId} is being inserted into the sl-observation-status-started datasource.")
            except Exception as e :
              errorLogger.error(f"Error sending data for observationId ({observationSubmissionId}) to sl-observation-status-started datasource: {e}", exc_info=True)
          else :
            infoLogger.info(f"Data with submission_id {observationSubmissionId} is already exists in the sl-observation-status-started datasource.") 
        except Exception as e :
          infoLogger.info(f"failed to ingest data to sl-observation-status-started datasource")
      # Insert data to sl-observation-status-started druid datasource if status is inprogress
      elif obSub['status'] == 'inprogress':
        try: 
          submission_exits_in_inprogress = check_observation_submission_id_existance(observationSubmissionId,"observationSubmissionId","sl-observation-status-inprogress")
          if submission_exits_in_inprogress == False:
            infoLogger.info(f"No data duplection for the Submission ID : {observationSubmissionId} in sl-observation-status-inprogress ")  
            observation_status = {}
            observation_status['observationSubmissionId'] = obSub['_id']
            observation_status['inprogressAt'] = obSub['updatedAt']
            flag_count += 1
            try : 
              message_id = send_data_to_kafka(observation_status,config.get("KAFKA", "observation_inprogress_druid_topic"))
              list_message_id.append(message_id)
              infoLogger.info(f"Data with submission_id {observationSubmissionId} is being inserted into the sl-observation-status-inprogress datasource.")
            except Exception as e:
              errorLogger.error(f"Error sending data for observationId ({observationSubmissionId}) to sl-observation-status-inprogress datasource: {e}", exc_info=True)
          else:       
            infoLogger.info(f"Data with submission_id {observationSubmissionId} is already exists in the sl-observation-status-inprogress datasource.")
        except Exception as e :
          infoLogger.info(f"failed to ingest data to sl-observation-status-inprogress datasource")
          errorLogger.error(e,exc_info=True)

      elif obSub['status'] == 'completed':
        try :
          submission_exits_in_completed = check_observation_submission_id_existance(observationSubmissionId,"observationSubmissionId","sl-observation-status-completed")
          if submission_exits_in_completed == False:
            infoLogger.info(f"No data duplection for the Submission ID : {observationSubmissionId} in sl-observation-status-completed")  
            observation_status = {}
            observation_status['observationSubmissionId'] = obSub['_id']
            observation_status['completedAt'] = obSub['completedDate']
            flag_count += 1
            try : 
              message_id = send_data_to_kafka(observation_status,config.get("KAFKA", "observation_completed_druid_topic"))
              list_message_id.append(message_id)
              infoLogger.info(f"Data with submission_id {observationSubmissionId} is being inserted into the sl-observation-status-completed datasource")
            except Exception as e :
              errorLogger.error(f"Error sending data for observationId ({observationSubmissionId}) to sl-observation-status-completed datasource: {e}", exc_info=True)
          else:       
            infoLogger.info(f"Data with submission_id {observationSubmissionId} is already exists in the sl-observation-status-completed datasource")
        except Exception as e :
          infoLogger.info(f"failed to ingest data to sl-observation-status-inprogress datasource")
          errorLogger.error(e,exc_info=True)     
      infoLogger.info(f"Completed processing kafka event for the observation Submission Id : {obSub['_id']}. For observation Status report")
    except Exception as e:
        # Log any errors that occur during data extraction
        errorLogger.error(e, exc_info=True)
    return list_message_id,flag_count
except Exception as e:
    infoLogger.info(f"Failed to process main_data_extraction function")
    # Log any errors that occur during data extraction
    errorLogger.error(f"Failed to process main_data_extraction function{e}", exc_info=True)

    
try:
  @app.agent(rawTopicName)
  async def surveyFaust(consumer):
    '''Faust agent to consume messages from Kafka and process them'''
    async for msg in consumer:
      msg_val = msg.decode('utf-8')
      msg_data = json.loads(msg_val)
      infoLogger.info(f"========== START OF OBSERVATION SUBMISSION EVENT PROCESSING - {datetime.datetime.now()} ==========")
      list_message_id = []
      flag_count = 0
      list_message_id_obj, flag_count_obj = obj_creation(msg_data)
      list_message_id.extend(list_message_id_obj)
      flag_count = flag_count + flag_count_obj
      list_message_id_main , flag_count_main = main_data_extraction(msg_data)
      list_message_id.extend(list_message_id_main)
      flag_count = flag_count + flag_count_main
      #updating the mongo collection
      has_null_values = any(value is None for value in list_message_id)
      if (has_null_values == False) : 
        if (len(list_message_id) != 0) and (flag_count != 0):
          if len(list_message_id) == flag_count:
            try : 
              observationSubCollec.update_one(
                  {"_id": ObjectId(msg_data['_id'])},
                  {"$set": {"datapipeline.processed_date": datetime.datetime.now()}}
              )
              infoLogger.info("Updated the Mongo observation submission collection after inserting data into kafka topic")
            except Exception as e :
              infoLogger.info("Failed to update the Mongo observation submission collection")
              errorLogger.error(f"Failed to update the Mongo observation submission collection{e}",exc_info=True)
          else:
            infoLogger.info("As the number of Kafka message IDs did not align with the number of ingestions, the Mongo observation submission collection was not updated.")
        else:
          infoLogger.info("Since both Kafka ID count and flag count are zero, the MongoDB observation submission collection will not be updated")
      else:
        infoLogger.info("As list_message_id contains either duplicate value or null values hence the MongoDB observation submission collection will not be updated ")    
      infoLogger.info(f"********** END OF OBSERVATION SUBMISSION EVENT PROCESSING - {datetime.datetime.now()}**********")
except Exception as e:
    # Log any other exceptions
    errorLogger.error(f"Error in observationFaust function: {e}",exc_info=True)

if __name__ == '__main__':
  app.main()

