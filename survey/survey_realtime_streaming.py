# -----------------------------------------------------------------
# Name : survey_realtime_streaming.py
# Author : Prashanth, Vivek 
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
import requests
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

# Initialize Mongo db collection 

clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
surveySubCollec = db[config.get('MONGO', 'survey_submissions_collection')]

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
                orgObj['organisationId'] = org['organisationId']
                orgObj['organisationName'] = org["orgName"]
                orgarr.append(orgObj)
    return orgarr

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

# # Define function to check if survey submission Id exists in Druid
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
        cur.execute(query)
        result = cur.fetchone()
        count = result[0]
        infoLogger.info(f"Found {count} entires in {table_name}")
        if count == 0:
            return False
        else:
            return True        
    except Exception as e:
        infoLogger.info(f"Failed to check the {table_name} datasource")
        # Log any errors that occur during Druid query execution
        errorLogger.error(f"Failed to check the {table_name} datasource {e}",exc_info=True)

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
        list_message_id = []
        flag_count = 0
        if len(self.orgArr) >0:
            for org in range(len(self.orgArr)):
                finalObj = {}
                finalObj =  self.creatingObj(self.answer,self.quesexternalId,self.ans_val,self.instNum,self.responseLabel)
                finalObj.update(self.orgArr[org])
                survey_id = finalObj["surveyId"]
                question_id = finalObj["questionId"]
                flag_count += 1
                try :
                    message_id  = send_data_to_kafka(finalObj,config.get("KAFKA", "survey_druid_topic"))
                    list_message_id.append(message_id)
                    infoLogger.info(f"Data for surveyId ({survey_id}) and questionId ({question_id}) inserted into sl-survey datasource")
                except Exception as e :
                    infoLogger.info(f"Failed to insert data for surveyId ({survey_id}) and questionId ({question_id}) into sl-survey datasource ")
                    errorLogger.error(f"Data for surveyId ({survey_id}) and questionId ({question_id}) not inserted into sl-survey datasource{e}",exc_info=True)
            return list_message_id,flag_count
        else:
            finalObj = {}
            finalObj =  self.creatingObj(self.answer,self.quesexternalId,self.ans_val,self.instNum,self.responseLabel)
            survey_id = finalObj["surveyId"]
            question_id = finalObj["questionId"]
            flag_count += 1
            try:
                message_id = send_data_to_kafka(finalObj,config.get("KAFKA", "survey_druid_topic"))
                list_message_id.append(message_id)
                infoLogger.info(f"Data for surveyId ({survey_id}) and questionId ({question_id}) inserted into sl-survey datasource")
            except Exception as e :
                infoLogger.info(f"Failed to insert data for surveyId ({survey_id}) and questionId ({question_id}) into sl-survey datasource ")
                errorLogger.error(f"Data for surveyId ({survey_id}) and questionId ({question_id}) not inserted into sl-survey datasource{e}",exc_info=True)
            return list_message_id,flag_count
        


def obj_creation(obSub):
    '''Function to process survey submission data before sending it to Kafka'''
    try:
        # Debug log for survey submission ID
        infoLogger.info(f"Started to process kafka event for the Survey Submission Id : {obSub['_id']}. For Survey Question report")
        list_message_id_ext = []
        flag_count_ext = 0
        if obSub['status'] == 'completed': 
            surveySubmissionId =  str(obSub['_id'])
            submission_exits = check_survey_submission_id_existance(surveySubmissionId,"surveySubmissionId","sl-survey")
            if submission_exits == False:
                infoLogger.info(f"No data duplection for the Submission ID : {surveySubmissionId} in sl-survey ")  
                if 'isAPrivateProgram' in obSub :
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
                    if 'answers' in obSub.keys() :  
                        answersArr = [v for v in obSub['answers'].values()]
                        for ans in answersArr:
                            try:
                                if len(ans['fileName']):
                                    evidence_sub_count = evidence_sub_count + len(ans['fileName'])
                            except KeyError:
                                pass
                        for ans in answersArr:
                            def sequenceNumber(externalId,answer):
                                if 'solutions' in obSub.keys():
                                    solutionsArr = [v for v in obSub['solutions'].values()]
                                    for solu in solutionsArr:
                                        section = [k for k in solu['sections'].keys()]
                                    # parsing through questionSequencebyecm to get the sequence number
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

                                surveySubQuestionsObj['surveySubmissionId'] = str(obSub['_id'])
                                surveySubQuestionsObj['createdBy'] = obSub['createdBy']

                                # Check if 'isAPrivateProgram' key exists
                                try:
                                    surveySubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                                except KeyError:
                                    surveySubQuestionsObj['isAPrivateProgram'] = True

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
                                        surveySubQuestionsObj['questionResponseLabelNumber'] = responseLabel
                                    else:
                                        surveySubQuestionsObj['questionResponseLabelNumber'] = 0
                                else:
                                    surveySubQuestionsObj['questionResponseLabelNumber'] = 0

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
                                    surveySubQuestionsObj['evidenceCount'] = len(answer['fileName'])
                                else:
                                    surveySubQuestionsObj['evidences'] = ''                                
                                    surveySubQuestionsObj['evidenceCount'] = 0
                                surveySubQuestionsObj['totalEvidences'] = evidence_sub_count
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

                                surveySubQuestionsObj['userProfile'] = str(new_dict)
                        
                                # Extract parent question details for matrix response type
                                # if ans['responseType']=='matrix':
                                #     surveySubQuestionsObj['instanceParentQuestion'] = ans['question'][0]
                                #     surveySubQuestionsObj['instanceParentId'] = ans['qid']
                                #     surveySubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                                #     surveySubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                                #     surveySubQuestionsObj['instanceParentCriteriaExternalId'] = ans['criteriaId']
                                #     surveySubQuestionsObj['instanceParentCriteriaName'] = None
                                #     surveySubQuestionsObj['instanceId'] = instNumber
                                #     surveySubQuestionsObj['instanceParentExternalId'] = quesexternalId
                                #     surveySubQuestionsObj['instanceParentEcmSequence']= sequenceNumber(
                                #         surveySubQuestionsObj['instanceParentExternalId'], answer
                                #     )
                                # else:
                                #     surveySubQuestionsObj['instanceParentQuestion'] = ''
                                #     surveySubQuestionsObj['instanceParentId'] = ''
                                #     surveySubQuestionsObj['instanceParentResponsetype'] =''
                                #     surveySubQuestionsObj['instanceId'] = instNumber
                                #     surveySubQuestionsObj['instanceParentExternalId'] = ''
                                #     surveySubQuestionsObj['instanceParentEcmSequence'] = '' 

                                # Update object with additional user data
                                # Commented the bellow line as we don't need userRoleInso in KB
                                # surveySubQuestionsObj.update(userDataCollector(obSub))
                                return surveySubQuestionsObj

                            # Function to fetch question details
                            def fetchingQuestiondetails(ansFn,instNumber):        
                                try:
                                    list_message_id_fetch = []
                                    flag_count_fetch = 0
                                    # if (len(ansFn['options']) == 0) or (('options' in ansFn.keys()) == False):
                                    if (len(ansFn['options']) == 0) or (('options' not in ansFn.keys())):
                                        try:
                                            orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                            final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, ansFn['value'], orgArr, creatingObj)
                                            message_id,count = final_worker.run()
                                            list_message_id_fetch.extend(message_id)
                                            flag_count_fetch = flag_count_fetch + count
                                        except Exception as e :
                                            errorLogger.error(e, exc_info=True)
                                        return list_message_id_fetch,flag_count_fetch
                                    else:
                                        labelIndex = 0
                                        for quesOpt in ansFn['options']:
                                            try:
                                                if type(ansFn['value']) == str or type(ansFn['value']) == int:
                                                    if quesOpt['value'] == ansFn['value'] :
                                                        orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                                        final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, quesOpt['label'], orgArr, creatingObj)
                                                        message_id,count = final_worker.run() 
                                                        list_message_id_fetch.extend(message_id)
                                                        flag_count_fetch = flag_count_fetch + count    
                                                elif type(ansFn['value']) == list:
                                                    for ansArr in ansFn['value']:
                                                        if quesOpt['value'] == ansArr:
                                                            orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                                            final_worker = FinalWorker(ansFn,ansFn['externalId'], ansArr, instNumber, quesOpt['label'], orgArr, creatingObj)
                                                            message_id,count = final_worker.run()
                                                            list_message_id_fetch.extend(message_id)
                                                            flag_count_fetch = flag_count_fetch + count
                                            except KeyError:
                                                pass
                                    return list_message_id_fetch,flag_count_fetch
                                except Exception as e:
                                    errorLogger.error(e, exc_info=True)
                                    # Ensure the function returns even in case of an exception
                                    return [], 0

                            # Check response type and call function to fetch question details
                            if (
                                ans['responseType'] == 'text' or ans['responseType'] == 'radio' or 
                                ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or 
                                ans['responseType'] == 'number' or ans['responseType'] == 'date'
                            ):   
                                inst_cnt = ''
                                message_id,flag_count = fetchingQuestiondetails(ans, inst_cnt)
                                list_message_id_ext.extend(message_id)
                                flag_count_ext = flag_count_ext + flag_count

                            elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
                                inst_cnt =0
                                for instances in ans['value']:
                                    inst_cnt = inst_cnt + 1
                                    for instance in instances.values():
                                        message_id,flag_count = fetchingQuestiondetails(instance,inst_cnt)    
                                        list_message_id_ext.extend(message_id)
                                        flag_count_ext = flag_count_ext + flag_count       
            else:
                infoLogger.info(f"survey_Submission_id {surveySubmissionId} is already exists in the sl-survey datasource.")        
        else:                        
            infoLogger.info(f"Survey Submission is not in completed status" )

        infoLogger.info(f"Completed processing kafka event for the Survey Submission Id : {obSub['_id']}. For Survey Question report ")  
        return list_message_id_ext,flag_count_ext             
    except Exception as e:
        # Log any errors that occur during processing
        infoLogger.info(f"Failed to process obj_creation function")
        errorLogger.error(f"Failed to process obj_creation function{e}", exc_info=True)

# Main data extraction function

def main_data_extraction(obSub):
    '''Function to process survey submission data before sending it to Kafka topics'''
    try :
        infoLogger.info(f"Starting to process kafka event for the Survey Submission Id : {obSub['_id']}. For Survey Status report")
        list_message_id = []
        flag_count = 0
        #processing for sl-survey-meta datsource 
        surveySubmissionId =  str(obSub['_id'])
        submission_exits_in_meta = check_survey_submission_id_existance(surveySubmissionId,"surveySubmissionId","sl-survey-meta")
        if submission_exits_in_meta == False : 
            infoLogger.info(f"No data duplection for the Submission ID : {surveySubmissionId} in sl-survey-meta ")
            # Initialize dictionary for storing survey submission data
            surveySubQuestionsObj = {}
            
            # Extract various attributes from survey submission object
            surveySubQuestionsObj['surveyId'] = str(obSub.get('surveyId', ''))
            surveySubQuestionsObj['surveyName'] = str(obSub.get('surveyInformation', {}).get('name', ''))
            surveySubQuestionsObj['surveySubmissionId'] = obSub.get('_id', '')
            surveySubQuestionsObj['createdAt'] = obSub.get('createdAt', '')
            try:
                if 'solutionInfo' in obSub:
                    surveySubQuestionsObj['solutionName'] = obSub['solutionInfo']['name']
                else:
                    surveySubQuestionsObj['solutionName'] = ''
            except KeyError:
                surveySubQuestionsObj['solutionName'] = ''

            surveySubQuestionsObj['createdBy'] = obSub['createdBy']
            surveySubQuestionsObj['completedDate'] = obSub['completedDate']
            # Check if 'isAPrivateProgram' key exists
            try:
                surveySubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
            except KeyError:
                surveySubQuestionsObj['isAPrivateProgram'] = True

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

            surveySubQuestionsObj['userProfile'] = str(new_dict)    

            surveySubQuestionsObj['solutionExternalId'] = obSub.get('solutionExternalId', '')
            surveySubQuestionsObj['solutionId'] = obSub.get('solutionId', '')

            orgArr = orgCreator(obSub.get('userProfile', {}).get('organisations',None))
            if orgArr:
                surveySubQuestionsObj['organisationName'] = orgArr[0].get("organisationName")
            else:
                surveySubQuestionsObj['organisationName'] = None
            
            flag_count += 1
            try : 
            # Insert data to sl-observation-meta druid datasource if status is anything 
                message_id = send_data_to_kafka(surveySubQuestionsObj,config.get("KAFKA", "survey_meta_druid_topic"))
                list_message_id.append(message_id)
                infoLogger.info(f"Data with submission_id {surveySubmissionId} is being inserted into the sl-survey-meta datasource.")
            except Exception as e:
                errorLogger.error(f"Error sending data for submission_id ({surveySubmissionId}) to sl-survey-meta datasource: {e}", exc_info=True)
            # Insert data to sl-survey-meta druid datasource if status is anything 
        else :
            infoLogger.info(f"Data with submission_id {surveySubmissionId} is already exists in the sl-survey-meta datasource.")    

        # Insert data to sl-survey-status-started druid datasource if status is started
        if obSub['status'] == 'started':
            infoLogger.info(f"started extracting keys for sl-survey-status-started datasource")
            try : 
                submission_exits_in_started = check_survey_submission_id_existance(surveySubmissionId,"surveySubmissionId","sl-survey-status-started")
                if submission_exits_in_started == False :
                    infoLogger.info(f"No data duplection for the Submission ID : {surveySubmissionId} in sl-survey-status-started ")
                    survey_status = {}
                    survey_status['surveySubmissionId'] = obSub['_id']
                    survey_status['startedAt'] = obSub['createdAt']
                    flag_count += 1
                    try : 
                        # Insert data to sl-observation-status-started druid datasource if status is started 
                        message_id = send_data_to_kafka(survey_status,config.get("KAFKA", "survey_started_druid_topic"))
                        list_message_id.append(message_id)
                        infoLogger.info(f"Data with submission_id {surveySubmissionId} is being inserted into the sl-survey-status-started datasource.")
                    except Exception as e:
                        errorLogger.error(f"Error sending data for submission_id ({surveySubmissionId}) to sl-survey-status-started datasource: {e}", exc_info=True)
                else:
                    infoLogger.info(f"Data with submission_id {surveySubmissionId} is already exists in the sl-survey-status-started datasource.")          
            except Exception as e:
                infoLogger.info("failed to ingest data into sl-surevy-status-started datasource")

        elif obSub['status'] == 'inprogress':
            infoLogger.info(f"started extracting keys for sl-survey-status-inprogress datasource")
            try : 
                submission_exits_in_inprogress = check_survey_submission_id_existance(surveySubmissionId,"surveySubmissionId","sl-survey-status-inprogress")
                if submission_exits_in_inprogress == False : 
                    infoLogger.info(f"No data duplection for the Submission ID : {surveySubmissionId} in sl-survey-status-inprogress ")
                    survey_status['surveySubmissionId'] = obSub['_id']
                    survey_status['inprogressAt'] = obSub['updatedAt']
                    flag_count += 1
                    try : 
                        # Insert data to sl-observation-status-inprogress druid datasource if status is inprogress 
                        message_id = send_data_to_kafka(survey_status,config.get("KAFKA", "survey_inprogress_druid_topic"))
                        list_message_id.append(message_id)
                        infoLogger.info(f"Data with submission_id {surveySubmissionId} is being inserted into the sl-survey-status-inprogress datasource.")
                    except Exception as e:
                        errorLogger.error(f"Error sending data for submission_id ({surveySubmissionId}) to sl-survey-status-inprogress datasource: {e}", exc_info=True)
                else:
                    infoLogger.info(f"Data with submission_id {surveySubmissionId} is already exists in the sl-survey-status-inprogress datasource.")
            except Exception as e :
                infoLogger.info("failed to update the sl-survey-status-inprogress datasource")
                errorLogger.error(e,exc_info=True)

        elif obSub['status'] == 'completed':
            infoLogger.info(f"started extracting keys for sl-survey-status-completed datasource")
            try : 
                submission_exits_in_completed = check_survey_submission_id_existance(surveySubmissionId,"surveySubmissionId","sl-survey-status-completed")
                if submission_exits_in_completed == False : 
                    infoLogger.info(f"No data duplection for the Submission ID : {surveySubmissionId} in sl-survey-status-completed")
                    survey_status = {}
                    survey_status['surveySubmissionId'] = obSub['_id']
                    survey_status['completedAt'] = obSub['completedDate']
                    flag_count += 1
                    try : 
                        # Insert data to sl-observation-status-completed druid datasource if status is completed
                        message_id = send_data_to_kafka(survey_status,config.get("KAFKA", "survey_completed_druid_topic"))
                        list_message_id.append(message_id)
                        infoLogger.info(f"Data with submission_id {surveySubmissionId} is being inserted into the sl-survey-status-completed datasource.")
                    except Exception as e:
                        errorLogger.error(f"Error sending data for submission_id ({surveySubmissionId}) to sl-survey-status-completed datasource: {e}", exc_info=True)
                else:
                    infoLogger.info(f"Data with submission_id {surveySubmissionId} is already exists in the sl-survey-status-completed datasource")
            except Exception as e :
                infoLogger.info("failed to ingest data to sl-survey-status-completed datasourec")
                errorLogger.error(e,exc_info=True)

        infoLogger.info(f"Completed processing kafka event for the Survey Submission Id : {surveySubmissionId}. For Survey Status report")
        return list_message_id,flag_count
    except Exception as e:
    # Log any other exceptions
        infoLogger.info("Failed to process main_data_extraction function")
        errorLogger.error("Failed to process main_data_extraction function {e}",exc_info=True)


@app.agent(rawTopicName)
async def surveyFaust(consumer):
    '''Faust agent to consume messages from Kafka and process them'''
    async for msg in consumer:
        try:
            msg_val = msg.decode('utf-8')
            msg_data = json.loads(msg_val)
            
            infoLogger.info(f"========== START OF SURVEY SUBMISSION EVENT PROCESSING - {datetime.datetime.now()} ==========")
            list_message_id = []
            flag_count = 0
            list_message_id_obj, flag_count_obj = obj_creation(msg_data)
            list_message_id.extend(list_message_id_obj)
            flag_count = flag_count + flag_count_obj
            list_message_id_main , flag_count_main = main_data_extraction(msg_data)
            list_message_id.extend(list_message_id_main)
            flag_count = flag_count + flag_count_main 
            has_null_values = any(value is None for value in list_message_id)
            if (has_null_values == False) : 
                if (len(list_message_id) != 0) and (flag_count != 0):
                    if len(list_message_id) == flag_count:
                        try : 
                            surveySubCollec.update_one(
                                {"_id": ObjectId(msg_data['_id'])},
                                {"$set": {"datapipeline.processed_date": datetime.datetime.now()}}
                                )
                            infoLogger.info("Updated the Mongo survey submission collection after inserting data into to kafka topic")
                        except Exception as e :
                            infoLogger.info(f"Failed to update the Mongo survey submission collection for submissionId {msg_data['_id']}")
                            errorLogger.error(f"Failed to update the Mongo survey submission collection for submissionId {msg_data['_id']}{e}",exc_info=True)
                    else:
                        infoLogger.info("As the number of Kafka message IDs did not align with the number of ingestions, the Mongo survey submission collection was not updated.") 
                else:
                    infoLogger.info("Since both Kafka ID count and flag count are zero, the MongoDB observation submission collection will not be updated")    
            else:
                infoLogger.info("As list_message_id contains either duplicate value or null values hence the MongoDB observation submission collection will not be updated ")
            infoLogger.info(f"********** END OF SURVEY SUBMISSION EVENT PROCESSING - {datetime.datetime.now()} **********")
        except Exception as e:
            # Log KeyError
            errorLogger.error(e,exc_info=True)


if __name__ == '__main__':
    app.main()
