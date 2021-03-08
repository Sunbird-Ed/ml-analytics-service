# -----------------------------------------------------------------
# Name : py_survey_streaming.py
# Author :
# Description : Program to read data from one kafka topic and produce it
# to another kafka topic
# -----------------------------------------------------------------
import datetime
import json
import logging.handlers
import os
from configparser import ConfigParser, ExtendedInterpolation
from logging.handlers import TimedRotatingFileHandler

import faust
import requests
from bson.objectid import ObjectId
from kafka import KafkaProducer
from pymongo import MongoClient

config_path = os.path.dirname(os.path.abspath(__file__))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)

# Add the log message handler to the logger
successHandler = logging.handlers.RotatingFileHandler(
    config.get('LOG_FILE', 'survey_streaming_success_log_filename')
)
successBackuphandler = TimedRotatingFileHandler(
    config.get('LOG_FILE', 'survey_streaming_success_log_filename'),
    when="w0",
    backupCount=1
)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(
    config.get('LOG_FILE', 'survey_streaming_error_log_filename')
)
errorBackuphandler = TimedRotatingFileHandler(
    config.get('LOG_FILE', 'survey_streaming_error_log_filename'),
    when="w0",
    backupCount=1
)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

try:
    kafka_url = (config.get("KAFKA", "url"))

    app = faust.App(
        'sl_survey_prod_faust',
        broker='kafka://' + kafka_url,
        value_serializer='raw',
        web_port=7004,
        broker_max_poll_records=500
    )
    rawTopicName = app.topic(config.get("KAFKA", "raw_data_topic"))
    producer = KafkaProducer(bootstrap_servers=[config.get("KAFKA", "url")])
    # db production
    clientqa = MongoClient(config.get('MONGO', 'url'))
    dbqa = clientqa[config.get('MONGO', 'db')]

    surveySubmissionsQACollec = dbqa[config.get('MONGO', 'surveySubmissionsCollec')]
    solutionsQACollec = dbqa[config.get('MONGO', 'solutionsCollec')]
    surveyQACollec = dbqa[config.get('MONGO', 'surveysCollec')]
    entityTypeQACollec = dbqa[config.get('MONGO', 'entityTypeCollec')]
    questionsQACollec = dbqa[config.get('MONGO', 'questionsCollec')]
    criteriaQACollec = dbqa[config.get('MONGO', 'criteriaCollec')]
    entitiesQACollec = dbqa[config.get('MONGO', 'entitiesCollec')]
    programsQACollec = dbqa[config.get('MONGO', 'programsCollec')]
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def get_keycloak_access_token():
        url = config.get("KEYCLOAK", "url")
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {
            "grant_type": config.get("KEYCLOAK", "grant_type"),
            "client_id": config.get("KEYCLOAK", "client_id"),
            "refresh_token": config.get("KEYCLOAK", "refresh_token")
        }
        response = requests.post(
            url, data=body, headers=headers
        )
        if response.status_code == 200:
            successLogger.debug("getKeycloak api")
            return response.json()
        else:
            errorLogger.error("Failure in getKeycloak API")
            errorLogger.error(response)
            errorLogger.error(response.text)

except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def read_user(user_id, access_token):
        urlReadUser = config.get("SUNBIRD", "base_url_ip") + "/" + \
                      config.get("ENDPOINT", "read_user") + "/" + \
                      str(user_id)
        headersReadUser = {
            'Content-Type': config.get("COMMON", "content_type"),
            'Authorization': "Bearer " + config.get("COMMON", "authorization"),
            'X-authenticated-user-token': access_token,
            'X-Channel-id': config.get("COMMON", "channel-id")
        }
        responseReadUser = requests.get(urlReadUser, headers=headersReadUser)
        if responseReadUser.status_code == 200:
            successLogger.debug("read user api")
            return responseReadUser.json()
        else:
            errorLogger.error("Failure in read user api")
            errorLogger.error(responseReadUser)
            errorLogger.error(responseReadUser.text)
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    def obj_creation(msg_id):
        data_keycloak = get_keycloak_access_token()
        tokenKeyCheck = "access_token" in data_keycloak
        if tokenKeyCheck:
            accessToken = data_keycloak['access_token']
            successLogger.debug("Survey Submission Id : " + str(msg_id))
            cursorMongo = surveySubmissionsQACollec.find({'_id': ObjectId(msg_id)}, no_cursor_timeout=True)
            for obSub in cursorMongo:
                completedDate = str(datetime.datetime.date(obSub['completedDate'])) + 'T' + str(
                    datetime.datetime.time(obSub['completedDate'])) + 'Z'
                createdAt = str(datetime.datetime.date(obSub['createdAt'])) + 'T' + str(
                    datetime.datetime.time(obSub['createdAt'])) + 'Z'
                updatedAt = str(datetime.datetime.date(obSub['updatedAt'])) + 'T' + str(
                    datetime.datetime.time(obSub['updatedAt'])) + 'Z'
                evidencesArr = [v for v in obSub['evidences'].values()]
                evidence_sub_count = 0

                # fetch user name from postgres with the help of keycloak id
                queryJsonOutput = {}
                queryJsonOutput = read_user(obSub["createdBy"], accessToken)
                if queryJsonOutput["result"]["response"]["userName"]:
                    if 'answers' in obSub.keys():
                        answersArr = [v for v in obSub['answers'].values()]
                        for ans in answersArr:
                            try:
                                if len(ans['fileName']):
                                    evidence_sub_count = evidence_sub_count + len(ans['fileName'])
                            except KeyError:
                                pass
                        for ans in answersArr:
                            def sequence_number(external_id, answer):
                                for sol in solutionsQACollec.find({'externalId': obSub['solutionExternalId']}):
                                    section = [k for k in sol['sections'].keys()]
                                    # parsing through questionSequencebyecm to get the sequence number
                                    try:
                                        for num in range(len(
                                                sol['questionSequenceByEcm'][answer['evidenceMethod']][section[0]])):
                                            if sol['questionSequenceByEcm'][answer['evidenceMethod']][section[0]][num] == external_id:
                                                return num + 1
                                    except KeyError:
                                        pass

                            def creatingObj(answer, quesexternalId, ans_val, instNumber, responseLabel):
                                surveySubQuestionsObj = {
                                    'userName': obSub['evidencesStatus'][0]['submissions'][0]['submittedByName']
                                }
                                surveySubQuestionsObj['userName'] = surveySubQuestionsObj['userName'].replace("null",
                                                                                                              "")
                                try:
                                    surveySubQuestionsObj['appName'] = obSub["appInformation"]["appName"].lower()
                                except KeyError:
                                    surveySubQuestionsObj['appName'] = config.get("COMMON", "diksha_survey_app_name")
                                surveySubQuestionsObj['surveySubmissionId'] = str(obSub['_id'])

                                surveySubQuestionsObj['createdBy'] = obSub['createdBy']

                                try:
                                    if obSub['isAPrivateProgram']:
                                        surveySubQuestionsObj['isAPrivateProgram'] = obSub['isAPrivateProgram']
                                    else:
                                        surveySubQuestionsObj['isAPrivateProgram'] = False
                                except KeyError:
                                    surveySubQuestionsObj['isAPrivateProgram'] = False
                                    pass

                                try:
                                    surveySubQuestionsObj['programExternalId'] = obSub['programExternalId']
                                except KeyError:
                                    surveySubQuestionsObj['programExternalId'] = None
                                try:
                                    surveySubQuestionsObj['programId'] = str(obSub['programId'])
                                except KeyError:
                                    surveySubQuestionsObj['programId'] = None
                                try:
                                    for program in programsQACollec.find({'externalId': obSub['programExternalId']}):
                                        surveySubQuestionsObj['programName'] = program['name']
                                except KeyError:
                                    surveySubQuestionsObj['programName'] = None

                                surveySubQuestionsObj['solutionExternalId'] = obSub['solutionExternalId']
                                surveySubQuestionsObj['surveyId'] = str(obSub['surveyId'])
                                for solu in solutionsQACollec.find({'externalId': obSub['solutionExternalId']}):
                                    surveySubQuestionsObj['solutionId'] = str(solu["_id"])
                                    surveySubQuestionsObj['solutionName'] = solu['name']
                                    section = [k for k in solu['sections'].keys()]
                                    surveySubQuestionsObj['section'] = section[0]
                                    surveySubQuestionsObj['questionSequenceByEcm'] = sequence_number(
                                        quesexternalId, answer
                                    )
                                    try:
                                        if solu['scoringSystem'] == 'pointsBasedScoring':
                                            surveySubQuestionsObj['totalScore'] = obSub['pointsBasedMaxScore']
                                            surveySubQuestionsObj['scoreAchieved'] = obSub['pointsBasedScoreAchieved']
                                            surveySubQuestionsObj['totalpercentage'] = obSub[
                                                'pointsBasedPercentageScore']
                                            surveySubQuestionsObj['maxScore'] = answer['maxScore']
                                            surveySubQuestionsObj['minScore'] = answer['scoreAchieved']
                                            surveySubQuestionsObj['percentageScore'] = answer['percentageScore']
                                            surveySubQuestionsObj['pointsBasedScoreInParent'] = answer[
                                                'pointsBasedScoreInParent']
                                    except KeyError:
                                        pass

                                for ob in surveyQACollec.find({'_id': obSub['surveyId']}):
                                    surveySubQuestionsObj['surveyName'] = ob['name']
                                surveySubQuestionsObj['questionId'] = str(answer['qid'])
                                surveySubQuestionsObj['questionAnswer'] = ans_val
                                surveySubQuestionsObj['questionResponseType'] = answer['responseType']
                                if answer['responseType'] == 'number':
                                    if answer['payload']['labels']:
                                        surveySubQuestionsObj['questionResponseLabel_number'] = responseLabel
                                    else:
                                        surveySubQuestionsObj['questionResponseLabel_number'] = ''
                                if answer['payload']['labels']:
                                    surveySubQuestionsObj['questionResponseLabel'] = responseLabel
                                else:
                                    surveySubQuestionsObj['questionResponseLabel'] = ''
                                surveySubQuestionsObj['questionExternalId'] = quesexternalId
                                surveySubQuestionsObj['questionName'] = answer['payload']['question'][0]
                                surveySubQuestionsObj['questionECM'] = answer['evidenceMethod']
                                surveySubQuestionsObj['criteriaId'] = str(answer['criteriaId'])
                                for crit in criteriaQACollec.find({'_id': ObjectId(answer['criteriaId'])}):
                                    surveySubQuestionsObj['criteriaExternalId'] = crit['externalId']
                                    surveySubQuestionsObj['criteriaName'] = crit['name']
                                surveySubQuestionsObj['completedDate'] = completedDate
                                surveySubQuestionsObj['createdAt'] = createdAt
                                surveySubQuestionsObj['updatedAt'] = updatedAt
                                surveySubQuestionsObj['remarks'] = answer['remarks']
                                if len(answer['fileName']):
                                    multipleFiles = None
                                    fileCnt = 1
                                    for filedetail in answer['fileName']:
                                        if fileCnt == 1:
                                            multipleFiles = config.get('STORAGE', 'base_url') + filedetail['sourcePath']
                                            fileCnt = fileCnt + 1
                                        else:
                                            multipleFiles = multipleFiles + ' , ' + config.get('STORAGE', 'base_url') + \
                                                            filedetail['sourcePath']
                                    surveySubQuestionsObj['evidences'] = multipleFiles
                                    surveySubQuestionsObj['evidence_count'] = len(answer['fileName'])
                                surveySubQuestionsObj['total_evidences'] = evidence_sub_count
                                # to fetch the parent question of matrix
                                if ans['responseType'] == 'matrix':
                                    surveySubQuestionsObj['instanceParentQuestion'] = ans['payload']['question'][0]
                                    surveySubQuestionsObj['instanceParentId'] = ans['qid']
                                    surveySubQuestionsObj['instanceParentResponsetype'] = ans['responseType']
                                    surveySubQuestionsObj['instanceParentCriteriaId'] = ans['criteriaId']
                                    for crit in criteriaQACollec.find({'_id': ObjectId(ans['criteriaId'])}):
                                        surveySubQuestionsObj['instanceParentCriteriaExternalId'] = crit['externalId']
                                        surveySubQuestionsObj['instanceParentCriteriaName'] = crit['name']
                                    surveySubQuestionsObj['instanceId'] = instNumber
                                    for ques in questionsQACollec.find({'_id': ObjectId(ans['qid'])}):
                                        surveySubQuestionsObj['instanceParentExternalId'] = ques['externalId']
                                    surveySubQuestionsObj['instanceParentEcmSequence'] = sequence_number(
                                        observationSubQuestionsObj['instanceParentExternalId'], answer
                                    )
                                else:
                                    surveySubQuestionsObj['instanceParentQuestion'] = ''
                                    surveySubQuestionsObj['instanceParentId'] = ''
                                    surveySubQuestionsObj['instanceParentResponsetype'] = ''
                                    surveySubQuestionsObj['instanceId'] = instNumber
                                    surveySubQuestionsObj['instanceParentExternalId'] = ''
                                    surveySubQuestionsObj['instanceParentEcmSequence'] = ''
                                surveySubQuestionsObj['user_id'] = queryJsonOutput["result"]["response"]["userName"]
                                surveySubQuestionsObj['channel'] = queryJsonOutput["result"]["response"]["rootOrgId"]
                                surveySubQuestionsObj['parent_channel'] = config.get('COMMON', 'parent_channel')
                                return surveySubQuestionsObj

                            # fetching the question details from questions collection
                            def fetching_question_details(ansFn, instNumber):
                                for ques in questionsQACollec.find({'_id': ObjectId(ansFn['qid'])}):
                                    # surveySubQuestionsArr.append('t')
                                    if len(ques['options']) == 0:
                                        try:
                                            if len(ansFn['payload']['labels']) > 0:
                                                finalObj = {}
                                                finalObj = creatingObj(
                                                    ansFn, ques['externalId'], ansFn['value'],
                                                    instNumber, ansFn['payload']['labels'][0]
                                                )
                                                producer.send(
                                                    (config.get("KAFKA", "druid_topic")),
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
                                                    if quesOpt['value'] == ansFn['value']:
                                                        finalObj = {}
                                                        finalObj = creatingObj(
                                                            ansFn, ques['externalId'],
                                                            ansFn['value'], instNumber,
                                                            ansFn['payload']['labels'][0]
                                                        )
                                                        producer.send(
                                                            (config.get("KAFKA", "druid_topic")),
                                                            json.dumps(finalObj).encode('utf-8')
                                                        )
                                                        producer.flush()
                                                        successLogger.debug("Send Obj to Kafka")
                                                elif type(ansFn['value']) == list:
                                                    for ansArr in ansFn['value']:
                                                        if quesOpt['value'] == ansArr:
                                                            finalObj = {}
                                                            finalObj = creatingObj(
                                                                ansFn, ques['externalId'], ansArr,
                                                                instNumber, quesOpt['label']
                                                            )
                                                            producer.send(
                                                                (config.get("KAFKA", "druid_topic")),
                                                                json.dumps(finalObj).encode('utf-8')
                                                            )
                                                            producer.flush()
                                                            successLogger.debug("Send Obj to Kafka")
                                            except KeyError:
                                                pass
                                    # to check the value is null ie is not answered
                                    try:
                                        if type(ansFn['value']) == str and ansFn['value'] == '':
                                            finalObj = {}
                                            finalObj = creatingObj(
                                                ansFn, ques['externalId'], ansFn['value'],
                                                instNumber, None
                                            )
                                            producer.send(
                                                (config.get("KAFKA", "druid_topic")),
                                                json.dumps(finalObj).encode('utf-8')
                                            )
                                            producer.flush()
                                            successLogger.debug("Send Obj to Kafka")
                                    except KeyError:
                                        pass

                            if ans['responseType'] == 'text' or ans['responseType'] == 'radio' or \
                                    ans['responseType'] == 'multiselect' or ans['responseType'] == 'slider' or \
                                    ans['responseType'] == 'number' or ans['responseType'] == 'date':
                                inst_cnt = ''
                                fetching_question_details(ans, inst_cnt)
                            elif ans['responseType'] == 'matrix' and len(ans['value']) > 0:
                                inst_cnt = 0
                                for instances in ans['value']:
                                    inst_cnt = inst_cnt + 1
                                    for instance in instances.values():
                                        fetching_question_details(instance, inst_cnt)

            cursorMongo.close()
except Exception as e:
    errorLogger.error(e, exc_info=True)

try:
    @app.agent(rawTopicName)
    async def surveyFaust(consumer):
        async for msg in consumer:
            msg_val = msg.decode('utf-8')
            msg_data = json.loads(msg_val)
            successLogger.debug("========== START OF SURVEY SUBMISSION ========")
            obj_creation(msg_data['_id'])
            successLogger.debug("********* END OF SURVEY SUBMISSION ***********")
except Exception as e:
    errorLogger.error(e, exc_info=True)

if __name__ == '__main__':
    app.main()
