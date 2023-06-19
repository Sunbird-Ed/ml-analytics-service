# ----------------------------------- #
# Script to delete the project datasources 
# prior to batch ingestion
# ----------------------------------- #


import json, sys, time
from configparser import ConfigParser,ExtendedInterpolation
import os
import requests
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import datetime
from datetime import date

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


# date formating
current_date = datetime.date.today()
formatted_current_date = current_date.strftime("%d-%B-%Y")
number_of_days_logs_kept = current_date - datetime.timedelta(days=7)
number_of_days_logs_kept = number_of_days_logs_kept.strftime("%d-%B-%Y")

# file path for log
file_path_for_output_and_debug_log = config.get('LOGS', 'project_success_error')
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


payload = json.loads(config.get("DRUID","project_injestion_spec"))
datasources = payload["spec"]["dataSchema"]["dataSource"]
ingestion_specs = [json.dumps(payload)]
headers = {'Content-Type': 'application/json'}

# bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** STARTED DELETION: {datetime.datetime.now()} ***********\n")
infoLogger.info(f"*********** STARTED DELETION: {datetime.datetime.now()} ***********\n")
druid_end_point = config.get("DRUID", "metadata_url") + datasources
get_timestamp = requests.get(druid_end_point, headers=headers)
if get_timestamp.status_code == 200:
    # bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Fetched Timestamp of {datasources} | Waiting for 50s")
    infoLogger.info(f"Fetched Timestamp of {datasources} | Waiting for 50s")
    successLogger.debug("Successfully fetched time stamp of the datasource " + datasources )
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
    successLogger.debug(f"sleep 50s")

    disable_datasource = requests.delete(druid_end_point, headers=headers)

    if disable_datasource.status_code == 200:
        successLogger.debug("successfully disabled the datasource " + datasources)
        time.sleep(300)
        successLogger.debug(f"sleep 300s")

        delete_segments = requests.delete(
            druid_end_point + "/intervals/" + interval, headers=headers
        )
        if delete_segments.status_code == 200:
            successLogger.debug("successfully deleted the segments " + datasources)
            # bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Deletion check successfull for {datasources}")
            infoLogger.info(f"Deletion check successfull for {datasources}")
            time.sleep(600)
            successLogger.debug(f"sleep 300s")

            enable_datasource = requests.get(druid_end_point, headers=headers)
            if enable_datasource.status_code == 200 or enable_datasource.status_code == 204:
                successLogger.debug("successfully enabled the datasource " + datasources)
                time.sleep(600)
                successLogger.debug(f"sleep 600s")
            else:
                errorLogger.error("failed to enable the datasource " + datasources)
                errorLogger.error("failed to enable the datasource " + str(enable_datasource.status_code))
                errorLogger.error(enable_datasource.text)
                infoLogger.info(f"Failed to enable {datasources} | Error: {enable_datasource.status_code}")
                # bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"Failed to enable {datasources} | Error: {enable_datasource.status_code}")
        else:
            errorLogger.error("failed to delete the segments of the datasource " + datasources)
            errorLogger.error("failed to delete the segments of the datasource " + str(delete_segments.status_code))
            errorLogger.error(delete_segments.text)
            infoLogger.info(f"failed to delete the {datasources}")
            # bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to delete the {datasources}")
    else:
        errorLogger.error("failed to disable the datasource " + datasources)
        errorLogger.error("failed to disable the datasource " + str(disable_datasource.status_code))
        # bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"failed to disable the {datasources}")
        infoLogger.info(f"failed to disable the {datasources}")
        errorLogger.error(disable_datasource.text)
else:
    errorLogger.error("failed to get the timestamp of the datasource " + datasources)
    errorLogger.error("failed to get the timestamp of the datasource " + str(get_timestamp.status_code))
    errorLogger.error(get_timestamp.text)

# bot.api_call("chat.postMessage",channel=config.get("SLACK","channel"),text=f"*********** COMPLETED DELETION: {datetime.datetime.now()} ***********\n")
infoLogger.info(f"*********** COMPLETED DELETION: {datetime.datetime.now()} ***********\n")
successLogger.debug(f"Ingestion end for raw {datetime.datetime.now()}")
