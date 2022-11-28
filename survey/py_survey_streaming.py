# -----------------------------------------------------------------
# Name : py_survey_streaming.py
# Author :
# Description : Program to read data from one kafka topic and 
# produce it to another kafka topic
# -----------------------------------------------------------------


import sys, os, json
import datetime
import kafka
import faust
import logging
import time, re
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from configparser import ConfigParser,ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Handles the success logs
successHandler = RotatingFileHandler(config.get('LOGS', 'survey_streaming_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'survey_streaming_success'),when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# Handles the error logs
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = RotatingFileHandler(config.get('LOGS', 'survey_streaming_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS', 'survey_streaming_error'),when="w0",backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

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
                producer.send((config.get("KAFKA", "survey_druid_topic")), json.dumps(finalObj).encode('utf-8'))
                producer.flush()
                successLogger.debug("Send Obj to Kafka")
        else:
            finalObj = {}
            finalObj =  self.creatingObj(self.answer,self.quesexternalId,self.ans_val,self.instNum,self.responseLabel)
            producer.send((config.get("KAFKA", "survey_druid_topic")), json.dumps(finalObj).encode('utf-8'))
            producer.flush()
            successLogger.debug("Send Obj to Kafka")

try:
    def obj_creation(obSub):
        '''Captures the data and pre-process it before sending to another kafka topic'''
        successLogger.debug(f"Survey Submission Id : {obSub['_id']}")
        if obSub['status'] == 'completed':
            if 'isAPrivateProgram' in obSub :
                surveySubQuestionsArr = []
                completedDate = str(obSub['completedDate'])
                createdAt = str(obSub['createdAt'])
                updatedAt = str(obSub['updatedAt'])
                evidencesArr = [v for v in obSub['evidences'].values()]
                evidence_sub_count = 0
                rootOrgId = None
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
                        
                        def creatingObj(answer,quesexternalId,ans_val,instNumber,responseLabel):
                            surveySubQuestionsObj = {}
                            try:
                                surveySubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
                            except KeyError :
                                surveySubQuestionsObj['appName'] = config.get("ML_APP_NAME", "survey_app")

                            surveySubQuestionsObj['surveySubmissionId'] = str(obSub['_id'])

                            surveySubQuestionsObj['createdBy'] = obSub['createdBy']

                            try:
                                surveySubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                            except KeyError:
                                surveySubQuestionsObj['isAPrivateProgram'] = True

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

                            try:
                                section = [k for k in obSub['solutionInfo']['sections'].keys()]
                                surveySubQuestionsObj['section'] = section[0]
                            except KeyError:
                                surveySubQuestionsObj['section'] = ''

                            surveySubQuestionsObj['questionSequenceByEcm'] = sequenceNumber(quesexternalId, answer)
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

                            if 'surveyInformation' in obSub :
                                if 'name' in obSub['surveyInformation']:
                                    surveySubQuestionsObj['surveyName'] = obSub['surveyInformation']['name']
                                else:
                                    surveySubQuestionsObj['surveyName'] = ''
                            
                            surveySubQuestionsObj['questionId'] = str(answer['qid'])
                            surveySubQuestionsObj['questionAnswer'] = ans_val
                            surveySubQuestionsObj['questionResponseType'] = answer['responseType']

                            if answer['responseType'] == 'number':
                                if answer['payload']['labels']:
                                    surveySubQuestionsObj['questionResponseLabel_number'] = responseLabel
                                else:
                                    surveySubQuestionsObj['questionResponseLabel_number'] = ''
                            try:
                             if answer['payload']['labels']:
                                if answer['responseType'] == 'text':
                                 surveySubQuestionsObj['questionResponseLabel'] = "'"+ re.sub("\n|\"","",responseLabel) +"'"
                                else:
                                 surveySubQuestionsObj['questionResponseLabel'] = responseLabel
                             else:
                                surveySubQuestionsObj['questionResponseLabel'] = ''
                            except KeyError :
                                surveySubQuestionsObj['questionResponseLabel'] = ''
                            surveySubQuestionsObj['questionExternalId'] = quesexternalId
                            surveySubQuestionsObj['questionName'] = answer['payload']['question'][0]
                            surveySubQuestionsObj['questionECM'] = answer['evidenceMethod']
                            surveySubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
                            
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


                            surveySubQuestionsObj['completedDate'] = completedDate
                            surveySubQuestionsObj['createdAt'] = createdAt
                            surveySubQuestionsObj['updatedAt'] = updatedAt
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
                                        multipleFiles = multipleFiles + ' , ' +  filedetail['sourcePath']
                                surveySubQuestionsObj['evidences'] = multipleFiles                                  
                                surveySubQuestionsObj['evidence_count'] = len(answer['fileName'])
                            surveySubQuestionsObj['total_evidences'] = evidence_sub_count
                            # to fetch the parent question of matrix
                            if ans['responseType']=='matrix':
                                surveySubQuestionsObj['instanceParentQuestion'] = ans['payload']['question'][0]
                                surveySubQuestionsObj['instanceParentId'] = ans['qid']
                                surveySubQuestionsObj['instanceParentResponsetype'] =ans['responseType']
                                surveySubQuestionsObj['instanceParentCriteriaId'] =ans['criteriaId']
                                surveySubQuestionsObj['instanceParentCriteriaExternalId'] = ans['criteriaId']
                                surveySubQuestionsObj['instanceParentCriteriaName'] = None
                                surveySubQuestionsObj['instanceId'] = instNumber
                                surveySubQuestionsObj['instanceParentExternalId'] = ans['qid']
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
                            surveySubQuestionsObj['channel'] = rootOrgId 
                            surveySubQuestionsObj['parent_channel'] = "SHIKSHALOKAM"
                            surveySubQuestionsObj.update(userDataCollector(obSub))
                            return surveySubQuestionsObj

                        # fetching the question details from questions collection
                        def fetchingQuestiondetails(ansFn,instNumber):        
                               try:
                                if len(ansFn['options']) == 0:
                                    try:
                                        if len(ansFn['payload']['labels']) > 0:
                                            orgArr = orgCreator(obSub["userProfile"]["organisations"])
                                            final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, ansFn['payload']['labels'][0], orgArr, creatingObj)
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
                                                    final_worker = FinalWorker(ansFn,ansFn['externalId'], ansFn['value'], instNumber, ansFn['payload']['labels'][0], orgArr, creatingObj)
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
                                # #to check the value is null ie is not answered
                                # try:
                                #     if type(ansFn['value']) == str and ansFn['value'] == '':
                                #         finalObj = {}
                                #         finalObj =  creatingObj(
                                #             ansFn,ques['externalId'], ansFn['value'], instNumber, None
                                #         )
                                #         producer.send(
                                #             (config.get("KAFKA", "survey_druid_topic")), 
                                #             json.dumps(finalObj).encode('utf-8')
                                #         )
                                #         producer.flush()
                                #         successLogger.debug("Send Obj to Kafka")
                                # except KeyError:
                                #     pass

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

        
except Exception as e:
    errorLogger.error(e, exc_info=True)

try :
    @app.agent(rawTopicName)
    async def surveyFaust(consumer) :
        async for msg in consumer :
            msg_val = msg.decode('utf-8')
            msg_data = json.loads(msg_val)
            successLogger.debug("========== START OF SURVEY SUBMISSION ========")
            obj_creation(msg_data)
            successLogger.debug("********* END OF SURVEY SUBMISSION ***********")
except Exception as e:
    errorLogger.error(e, exc_info=True)


if __name__ == '__main__':
    app.main()
