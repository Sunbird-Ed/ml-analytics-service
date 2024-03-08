# -----------------------------------------------------------------
# Name : survey_realtime_streaming.py
# Author :prashanth@shikshalokam.org
# Description : Program to read data from one kafka topic and 
# produce it to another kafka topic
# -----------------------------------------------------------------

# Import necessary libraries
import sys, os, json, re
import datetime
from datetime import date
import kafka
import faust
import logging
import time, re
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
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
file_path_for_output_and_debug_log = config.get('LOGS', 'survey_streaming_success_error')
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

# Add loggers
formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# handler for output and debug Log
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


# Initialize Kafka producer and Faust app
try:
    kafka_url = (config.get("KAFKA", "url"))
    app = faust.App(
        'ml_survey_faust',
        broker='kafka://'+kafka_url,
        value_serializer='raw',
        web_port=7003,
        broker_max_poll_records=500
    )
    rawTopicName = app.topic(config.get("KAFKA", "survey_raw_topic"))
    producer = KafkaProducer(bootstrap_servers=[config.get("KAFKA", "url")])

except Exception as e:
    errorLogger.error(e, exc_info=True)

# Function to extract user data
def userDataCollector(val):
    '''Finds the Profile type, locations and framework(board) of an user'''
    if val is not None:
        dataobj = {}
        # Get user Sub type
        if val["userRoleInformation"]:
            try:
                dataobj["user_subtype"] = val["userRoleInformation"]["role"]
            except KeyError:
                pass
        # Get user type
        if val["userProfile"]["profileUserTypes"]:
            try:
                temp_userType = set([types["type"] for types in val["userProfile"]["profileUserTypes"]])
                dataobj["user_type"] = ", ".join(temp_userType)
            except KeyError:
                pass
        # Get locations
        if val["userProfile"]["userLocations"]:
            for loc in val["userProfile"]["userLocations"]:
                dataobj[f'{loc["type"]}_code'] = loc["code"]
                dataobj[f'{loc["type"]}_name'] = loc["name"]
                dataobj[f'{loc["type"]}_externalId'] = loc["id"]
        # Get board
        if "framework" in val["userProfile"] and val["userProfile"]["framework"]:
           if "board" in val["userProfile"]["framework"] and len(val["userProfile"]["framework"]["board"]) > 0:
               boardName = ",".join(val["userProfile"]["framework"]["board"])
               dataobj["board_name"] = boardName
    return dataobj

# Function to create organization data
def orgCreator(val):
    '''Finds the data for organisation'''
    orgarr = []
    if val is not None:
        for org in val:
            orgObj = {}
            if org["isSchool"] == False:
                orgObj['organisation_id'] = org['organisationId']
                orgObj['organisation_name'] = org["orgName"]
                orgarr.append(orgObj)
    return orgarr

# Worker class to send data to Kafka
class FinalWorker:
    '''Class that takes necessary inputs and sends the correct object into Kafka'''
    def __init__(self, answer, quesexternalId, ans_val, instNumber, responseLabel, orgarr, createObj):
        self.answer = answer
        self.quesexternalId = quesexternalId
        self.ans_val = ans_val
        self.instNum = instNumber
        self.responseLabel = responseLabel
        self.orgArr = orgarr
        self.creatingObj = createObj

    def run(self):
        if len(self.orgArr) >0:
            for org in range(len(self.orgArr)):
                finalObj = {}
                finalObj =  self.creatingObj(self.answer,self.quesexternalId,self.ans_val,self.instNum,self.responseLabel)
                finalObj.update(self.orgArr[org])
                survey_id = finalObj["surveyId"]
                question_id = finalObj["questionId"]
                producer.send((config.get("KAFKA", "survey_druid_topic")), json.dumps(finalObj).encode('utf-8'))
                producer.flush()
                successLogger.debug(f"data for surveyId ({survey_id}) and questionId ({question_id}) inserted into sl_survey datasource")
        else:
            finalObj = {}
            finalObj =  self.creatingObj(self.answer,self.quesexternalId,self.ans_val,self.instNum,self.responseLabel)
            survey_id = finalObj["surveyId"]
            question_id = finalObj["questionId"]
            producer.send((config.get("KAFKA", "survey_druid_topic")), json.dumps(finalObj).encode('utf-8'))
            producer.flush()
            successLogger.debug(f"data for surveyId ({survey_id}) and questionId ({question_id}) inserted into sl_survey datasource")

try:
    def obj_creation(obSub):
        '''Function to process survey submission data before sending it to Kafka'''
        try:
                # Debug log for survey submission ID
                successLogger.debug(f"Survey Submission Id : {obSub['_id']}")
                survey_submission_id =  str(obSub['_id'])
                if check_survey_submission_id_existance(survey_submission_id,"surveySubmissionId","sl-survey"):
                    # successLogger.debug(f"survey_Submission_id {survey_submission_id} is exists in sl-survey datasource.")
                    # pass
                    # Check if survey status is completed
                    if obSub['status'] == 'completed':
                        # Initialize variables for data extraction
                        surveySubQuestionsArr = []
                        completedDate = str(obSub['completedDate'])
                        createdAt = str(obSub['createdAt'])
                        updatedAt = str(obSub['updatedAt'])
                        evidencesArr = [v for v in obSub['evidences'].values()]
                        evidence_sub_count = 0
                        rootOrgId = None

                        # Extract root organization ID from user profile if available
                        try:
                            if obSub["userProfile"]:
                                if "rootOrgId" in obSub["userProfile"] and obSub["userProfile"]["rootOrgId"]:
                                    rootOrgId = obSub["userProfile"]["rootOrgId"]
                        except KeyError:
                            pass

                        # Check if 'answers' key exists in submission data
                        if 'answers' in obSub.keys():
                            answersArr = [v for v in obSub['answers'].values()]

                            # Extract data for each answer
                            for ans in answersArr:

                                # Function to get sequence number
                                def sequenceNumber(externalId,answer):
                                    if 'solutions' in obSub.keys():
                                        solutionsArr = [v for v in obSub['solutions'].values()]
                                        for solu in solutionsArr:
                                            section = [k for k in solu['sections'].keys()]
                                        try:
                                            for num in range(
                                                len(solu['questionSequenceByEcm'][answer['evidenceMethod']][section[0]])
                                            ):
                                                if solu['questionSequenceByEcm'][answer['evidenceMethod']][section[0]][num] == externalId:
                                                    return num + 1
                                        except KeyError:
                                            pass

                                # Function to create object for each answer
                                def creatingObj(answer,quesexternalId,ans_val,instNumber,responseLabel):
                                    surveySubQuestionsObj = {}

                                    # Extracting various attributes from submission object
                                    try:
                                        surveySubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
                                    except KeyError :
                                        surveySubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")

                                    surveySubQuestionsObj['surveySubmissionId'] = str(obSub['_id'])
                                    surveySubQuestionsObj['createdBy'] = obSub['createdBy']

                                    # Check if 'isAPrivateProgram' key exists
                                    try:
                                        surveySubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                                    except KeyError:
                                        surveySubQuestionsObj['isAPrivateProgram'] = True

                                    # Extract program related information
                                    try:
                                        surveySubQuestionsObj['programExternalId'] = obSub['programExternalId']
                                    except KeyError :
                                        surveySubQuestionsObj['programExternalId'] = None
                                    try:
                                        surveySubQuestionsObj['programId'] = str(obSub['programId'])
                                    except KeyError :
                                        surveySubQuestionsObj['programId'] = None
                                    try:
                                        if 'programInfo' in obSub:
                                            surveySubQuestionsObj['programName'] = obSub['programInfo']['name']
                                        else:
                                            surveySubQuestionsObj['programName'] = ''
                                    except KeyError:
                                        surveySubQuestionsObj['programName'] = ''

                                    # Extract solution related information
                                    surveySubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
                                    surveySubQuestionsObj['surveyId'] = str(obSub['surveyId'])
                                    surveySubQuestionsObj['solutionId'] = str(obSub["solutionId"])
                                    try:
                                        if 'solutionInfo' in obSub:
                                            surveySubQuestionsObj['solutionName'] = obSub['solutionInfo']['name']
                                        else:
                                            surveySubQuestionsObj['solutionName'] = ''
                                    except KeyError:
                                        surveySubQuestionsObj['solutionName'] = ''

                                    # Extract section information
                                    try:
                                        section = [k for k in obSub['solutionInfo']['sections'].keys()]
                                        surveySubQuestionsObj['section'] = section[0]
                                    except KeyError:
                                        surveySubQuestionsObj['section'] = ''

                                    # Get sequence number for the question
                                    surveySubQuestionsObj['questionSequenceByEcm'] = sequenceNumber(quesexternalId, answer)

                                    # Extract scoring related information
                                    try:
                                        if obSub['solutionInformation']['scoringSystem'] == 'pointsBasedScoring':
                                            try:
                                                surveySubQuestionsObj['totalScore'] = obSub['pointsBasedMaxScore']
                                            except KeyError :
                                                surveySubQuestionsObj['totalScore'] = ''
                                            try:
                                                surveySubQuestionsObj['scoreAchieved'] = obSub['pointsBasedScoreAchieved']
                                            except KeyError :
                                                surveySubQuestionsObj['scoreAchieved'] = ''
                                            try:
                                                surveySubQuestionsObj['totalpercentage'] = obSub['pointsBasedPercentageScore']
                                            except KeyError :
                                                surveySubQuestionsObj['totalpercentage'] = ''
                                            try:
                                                surveySubQuestionsObj['maxScore'] = answer['maxScore']
                                            except KeyError :
                                                surveySubQuestionsObj['maxScore'] = ''
                                            try:
                                                surveySubQuestionsObj['minScore'] = answer['scoreAchieved']
                                            except KeyError :
                                                surveySubQuestionsObj['minScore'] = ''
                                            try:
                                                surveySubQuestionsObj['percentageScore'] = answer['percentageScore']
                                            except KeyError :
                                                surveySubQuestionsObj['percentageScore'] = ''
                                            try:
                                                surveySubQuestionsObj['pointsBasedScoreInParent'] = answer['pointsBasedScoreInParent']
                                            except KeyError :
                                                surveySubQuestionsObj['pointsBasedScoreInParent'] = ''
                                    except KeyError:
                                        surveySubQuestionsObj['totalScore'] = ''
                                        surveySubQuestionsObj['scoreAchieved'] = ''
                                        surveySubQuestionsObj['totalpercentage'] = ''
                                        surveySubQuestionsObj['maxScore'] = ''
                                        surveySubQuestionsObj['minScore'] = ''
                                        surveySubQuestionsObj['percentageScore'] = ''
                                        surveySubQuestionsObj['pointsBasedScoreInParent'] = ''

                                    # Extract survey name
                                    if 'surveyInformation' in obSub :
                                        if 'name' in obSub['surveyInformation']:
                                            surveySubQuestionsObj['surveyName'] = obSub['surveyInformation']['name']
                                        else:
                                            surveySubQuestionsObj['surveyName'] = ''

                                    # Extract question related information
                                    surveySubQuestionsObj['questionId'] = str(answer['qid'])
                                    surveySubQuestionsObj['questionAnswer'] = ans_val
                                    surveySubQuestionsObj['questionResponseType'] = answer['responseType']

                                    # Extract response label for number response type
                                    if answer['responseType'] == 'number':
                                        if responseLabel:
                                            surveySubQuestionsObj['questionResponseLabel_number'] = responseLabel
                                        else:
                                            surveySubQuestionsObj['questionResponseLabel_number'] = 0

                                    # Extract response label for other response types
                                    try:
                                        if responseLabel:
                                            if answer['responseType'] == 'text':
                                                surveySubQuestionsObj['questionResponseLabel'] = "'"+ re.sub("\n|\"","",responseLabel) +"'"
                                            else:
                                                surveySubQuestionsObj['questionResponseLabel'] = responseLabel
                                        else:
                                            surveySubQuestionsObj['questionResponseLabel'] = ''
                                    except KeyError :
                                        surveySubQuestionsObj['questionResponseLabel'] = ''

                                    # Extract question details
                                    surveySubQuestionsObj['questionExternalId'] = quesexternalId
                                    surveySubQuestionsObj['questionName'] = answer['question'][0]
                                    surveySubQuestionsObj['questionECM'] = answer['evidenceMethod']
                                    surveySubQuestionsObj['criteriaId'] = str(answer['criteriaId'])

                                    # Extract criteria details
                                    try:
                                        if 'criteria' in obSub.keys():
                                            for criteria in obSub['criteria']:
                                                surveySubQuestionsObj['criteriaExternalId'] = criteria['externalId']
                                                surveySubQuestionsObj['criteriaName'] = criteria['name']
                                        else:
                                            surveySubQuestionsObj['criteriaExternalId'] = ''
                                            surveySubQuestionsObj['criteriaName'] = ''

                                    except KeyError:
                                        surveySubQuestionsObj['criteriaExternalId'] = ''
                                        surveySubQuestionsObj['criteriaName'] = ''

                                    # Extract completion dates
                                    surveySubQuestionsObj['completedDate'] = completedDate
                                    surveySubQuestionsObj['createdAt'] = createdAt
                                    surveySubQuestionsObj['updatedAt'] = updatedAt

                                    # Extract remarks and evidence details
                                    if answer['remarks'] :
                                        surveySubQuestionsObj['remarks'] = "'"+ re.sub("\n|\"","",answer['remarks']) +"'"
                                    else :
                                        surveySubQuestionsObj['remarks'] = None
                                    if len(answer['fileName']):
                                        multipleFiles = None
                                        fileCnt = 1
                                        for filedetail in answer['fileName']:
                                            if fileCnt == 1:
                                                multipleFiles = filedetail['sourcePath']
                                                fileCnt = fileCnt + 1
                                            else:
                                                multipleFiles = multipleFiles + ' , ' + filedetail['sourcePath']
                                        surveySubQuestionsObj['evidences'] = multipleFiles                                  
                                        surveySubQuestionsObj['evidence_count'] = len(answer['fileName'])
                                    surveySubQuestionsObj['total_evidences'] = evidence_sub_count

                                    # Extract parent question details for matrix response type
                                    if ans['responseType']=='matrix':
                                        surveySubQuestionsObj['instanceParentQuestion'] = ans['question'][0]
                                        surveySubQuestionsObj['instanceParentId'] = ans['qid']
                                        surveySubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                                        surveySubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                                        surveySubQuestionsObj['instanceParentCriteriaExternalId'] = ans['criteriaId']
                                        surveySubQuestionsObj['instanceParentCriteriaName'] = None
                                        surveySubQuestionsObj['instanceId'] = instNumber
                                        surveySubQuestionsObj['instanceParentExternalId'] = quesexternalId
                                        surveySubQuestionsObj['instanceParentEcmSequence']= sequenceNumber(
                                            surveySubQuestionsObj['instanceParentExternalId'], answer
                                        )
                                    else:
                                        surveySubQuestionsObj['instanceParentQuestion'] = ''
                                        surveySubQuestionsObj['instanceParentId'] = ''
                                        surveySubQuestionsObj['instanceParentResponsetype'] =''
                                        surveySubQuestionsObj['instanceId'] = instNumber
                                        surveySubQuestionsObj['instanceParentExternalId'] = ''
                                        surveySubQuestionsObj['instanceParentEcmSequence'] = '' 

                                    # Extract channel and parent channel
                                    surveySubQuestionsObj['channel'] = rootOrgId 
                                    surveySubQuestionsObj['parent_channel'] = "SHIKSHALOKAM"

                                    # Update object with additional user data
                                    surveySubQuestionsObj.update(userDataCollector(obSub))
                                    return surveySubQuestionsObj

                                # Function to fetch question details
                                def fetchingQuestiondetails(ansFn,instNumber):        
                                    try:
                                        if (len(ansFn['options']) == 0) or (('options' in ansFn.keys()) == False):
                                            try:
                                                orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                                final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, ansFn['value'], orgArr, creatingObj)
                                                final_worker.run()
                                            except KeyError :
                                                pass 
                                        else:
                                            labelIndex = 0
                                            for quesOpt in ansFn['options']:
                                                try:
                                                    if type(ansFn['value']) == str or type(ansFn['value']) == int:
                                                        if quesOpt['value'] == ansFn['value'] :
                                                            orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                                            final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, quesOpt['label'], orgArr, creatingObj)
                                                            final_worker.run()
                                                    elif type(ansFn['value']) == list:
                                                        for ansArr in ansFn['value']:
                                                            if quesOpt['value'] == ansArr:
                                                                orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                                                final_worker = FinalWorker(ansFn,ansFn['externalId'], ansArr, instNumber, quesOpt['label'], orgArr, creatingObj)
                                                                final_worker.run()
                                                except KeyError:
                                                    pass
                                    except KeyError:
                                        pass

                                # Check response type and call function to fetch question details
                                if (
                                    ans['responseType'] == 'text' or ans['responseType'] == 'radio' or 
                                    ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or 
                                    ans['responseType'] == 'number' or ans['responseType'] == 'date'
                                ):   
                                    inst_cnt = ''
                                    fetchingQuestiondetails(ans, inst_cnt)
                                elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
                                    inst_cnt =0
                                    for instances in ans['value']:
                                        inst_cnt = inst_cnt + 1
                                        for instance in instances.values():
                                            fetchingQuestiondetails(instance,inst_cnt)
                else:
                    successLogger.debug(f"survey_Submission_id {survey_submission_id} is already exists in the sl-survey datasource or the datasource is down.")                    
        except Exception as e:
            # Log any errors that occur during processing
            errorLogger.error(e, exc_info=True)
except Exception as e:
    # Log any errors that occur during processing
    errorLogger.error(e, exc_info=True)

# Main data extraction function
try:
    # Define function to check if survey submission Id exists in Druid
    def check_survey_submission_id_existance(key,column_name,table_name):
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
            # Query to check existence of survey submission Id in Druid table
            query = f"SELECT COUNT(*) FROM \"{table_name}\" WHERE \"{column_name}\" = '{key}'"
            infoLogger.info(query)
            cur.execute(query)
            result = cur.fetchone()
            count = result[0]
            infoLogger.info(f"count:{count}")
            if count == 0 :
                return True
            else :
                return False
        except Exception as e:
            # Log any errors that occur during Druid query execution
            errorLogger.error(f"Error checking survey_submission_id existence in Druid: {e}")

    def main_data_extraction(obSub):
        '''Function to extract main data from survey submission and upload it to Druid'''
        
        # Initialize dictionary for storing survey submission data
        surveySubQuestionsObj = {}
        survey_status = {}
        
        # Extract various attributes from survey submission object
        surveySubQuestionsObj['surveyId'] = str(obSub.get('surveyId', ''))
        surveySubQuestionsObj['survey_name'] = str(obSub.get('surveyInformation', {}).get('name', ''))
        surveySubQuestionsObj['survey_submission_id'] = obSub.get('_id', '')
        surveySubQuestionsObj['UUID'] = obSub.get('createdBy', '')
        surveySubQuestionsObj['programId'] = obSub.get('programInfo', {}).get('_id', '')
        surveySubQuestionsObj['program_name'] = obSub.get('programInfo', {}).get('name', '')
        
        # Before attempting to access the list, check if it is non-empty
        profile_user_types = obSub.get('userProfile', {}).get('profileUserTypes', [])
        if profile_user_types:
            # Access the first element of the list if it exists
            user_type = profile_user_types[0].get('type', None)
        else:
            # Handle the case when the list is empty
            user_type = None
        surveySubQuestionsObj['user_type'] = user_type

        surveySubQuestionsObj['solution_externalId'] = obSub.get('solutionExternalId')
        surveySubQuestionsObj['solution_id'] = obSub.get('solutionId')

        for location in obSub.get('userProfile', {}).get('userLocations', []):
            name = location.get('name')
            type_ = location.get('type')
            if name and type_:
                surveySubQuestionsObj[type_] = name
        
        surveySubQuestionsObj['board_name'] = obSub.get('userProfile', {}).get('framework', {}).get('board', [''])[0]

        orgArr = orgCreator(obSub.get('userProfile', {}).get('organisations',None))
        if orgArr:
            surveySubQuestionsObj['schoolId'] = orgArr[0].get("organisation_id")
            surveySubQuestionsObj['org_name'] = orgArr[0].get("organisation_name")
        else:
            surveySubQuestionsObj['schoolId'] = None
            surveySubQuestionsObj['org_name'] = None

        _id = surveySubQuestionsObj.get('survey_submission_id', None)
        try:
            if _id:
                    if check_survey_submission_id_existance(_id,"survey_submission_id","sl_survey_meta"):
                        # Upload survey submission data to Druid topic
                        producer.send((config.get("KAFKA", "survey_meta_druid_topic")), json.dumps(surveySubQuestionsObj).encode('utf-8'))  
                        producer.flush()
                        successLogger.debug(f"Data with submission_id ({_id}) is being inserted into the sl_survey_meta datasource.")
                    else:
                        successLogger.debug(f"Data with submission_id ({_id}) is already present in the sl_survey_meta datasource or the datasource is down.")
        except Exception as e :
            # Log any errors that occur during data ingestion
            errorLogger.error("======An error was found during data ingestion in the sl_survey_meta datasource========")
            errorLogger.error(e,exc_info=True)

        # upload the survey_submission_id and date in druid if status is started
        
        if obSub['status'] == 'started':
            survey_status['survey_submission_id'] = obSub['_id']
            survey_status['started_at'] = obSub['completedDate']
           
            survey_id = survey_status.get('survey_submission_id', None) 
            try : 
                if survey_id:
                    if check_survey_submission_id_existance(survey_id,"survey_submission_id","sl_survey_status_started"):
                        # Upload survey status data to Druid topic
                        producer.send((config.get("KAFKA", "survey_started_druid_topic")), json.dumps(survey_status).encode('utf-8'))
                        producer.flush()
                        successLogger.debug(f"Data with submission_id ({_id}) is being inserted into the sl_survey_status_started datasource.")
                    else:       
                        successLogger.debug(f"Data with submission_id ({_id}) is already present in the sl_survey_status_started datasource or the datasource is down")
            except Exception as e :
                # Log any errors that occur during data ingestion
                errorLogger.error("======An error was found during data ingestion in the sl_survey_status_completed datasource========")
                errorLogger.error(e,exc_info=True)  

        if obSub['status'] == 'completed':
            survey_status['completed_at'] = obSub['completedDate']
            survey_id = survey_status.get('survey_submission_id', None) 
            try : 
                if survey_id:
                    if check_survey_submission_id_existance(survey_id,"survey_submission_id","sl_survey_status_completed"):
                        # Upload survey status data to Druid topic
                        producer.send((config.get("KAFKA", "survey_completed_druid_topic")), json.dumps(survey_status).encode('utf-8'))
                        producer.flush()
                        successLogger.debug(f"Data with submission_id ({_id}) is being inserted into the sl_survey_status_completed datasource.")
                    else:       
                        successLogger.debug(f"Data with submission_id ({_id}) is already present in the sl_survey_status_completed datasource or the datasource is down.")
            except Exception as e :
                # Log any errors that occur during data ingestion
                errorLogger.error("======An error was found during data ingestion in the sl_survey_status_completed datasource========")
                errorLogger.error(e,exc_info=True)  
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
                
                successLogger.debug("========== START OF SURVEY SUBMISSION ========")
                obj_creation(msg_data)
                main_data_extraction(msg_data)
                successLogger.debug("********* END OF SURVEY SUBMISSION ***********")
            except KeyError as ke:
                # Log KeyError
                errorLogger.error(f"KeyError occurred: {ke}")
except Exception as e:
    # Log any other exceptions
    errorLogger.error(f"Error in surveyFaust function: {e}")


if __name__ == '__main__':
    app.main()

