# -------------------------------------------- #
# Script: Delete Druid segments except historical
# Features:
#   - Disable datasource
#   - Read & list all segments
#   - Keep segments with timestamp = 1998-01-01
#   - Delete all other segments
#   - Enable datasource
#   - No sleep delays
#   - No Slack notifications
# -------------------------------------------- #

import json
import sys
from configparser import ConfigParser, ExtendedInterpolation
import os
import requests
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import datetime


# --------------------------------------------------
# Load configuration
# --------------------------------------------------
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')

# SUCCESS LOG
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successHandler = logging.handlers.RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'), when="w0", backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

# ERROR LOG
errorLogger = logging.getLogger('error log')
errorLogger.setLevel(logging.ERROR)
errorHandler = logging.handlers.RotatingFileHandler(config.get('LOGS', 'project_error'))
errorBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_error'), when="w0", backupCount=1)
errorHandler.setFormatter(formatter)
errorLogger.addHandler(errorHandler)
errorLogger.addHandler(errorBackuphandler)

# DRUID Ingestion Spec → Get datasource name
payload = json.loads(config.get("DRUID","project_injestion_spec_agg"))
datasource = payload["spec"]["dataSchema"]["dataSource"]

headers = {'Content-Type': 'application/json'}
druid_base_url = config.get("DRUID", "metadata_url")
druid_segments_endpoint = f"{druid_base_url}{datasource}/segments"
druid_datasource_endpoint = f"{druid_base_url}{datasource}"


# --------------------------------------------------
# 1. Disable datasource
# --------------------------------------------------
disable_res = requests.delete(druid_datasource_endpoint, headers=headers)

if disable_res.status_code == 200:
    successLogger.debug(f"Datasource disabled: {datasource}")
else:
    errorLogger.error(f"Failed to disable datasource {datasource}")
    errorLogger.error(f"Status: {disable_res.status_code}")
    errorLogger.error(disable_res.text)
    sys.exit(1)


# --------------------------------------------------
# 2. Fetch All Segments
# --------------------------------------------------
res = requests.get(druid_segments_endpoint, headers=headers)

if res.status_code != 200:
    errorLogger.error(f"Failed to fetch segments for datasource {datasource}")
    errorLogger.error(f"Status Code: {res.status_code}")
    errorLogger.error(res.text)
    sys.exit(1)

segments_info = res.json()
all_segments = segments_info

successLogger.debug(f"Total segments found: {len(all_segments)}")
for seg in all_segments:
    successLogger.debug(f"SEGMENT: {json.dumps(seg)}")


# --------------------------------------------------
# 3. Filter historical vs deletable segments
# --------------------------------------------------
HISTORICAL_DATE = "1998-01-01"
historical_segments = []
deletable_segments = []

for seg in all_segments:
    interval = seg.get("interval", "")
    start_time = interval.split("/")[0][:10]  # YYYY-MM-DD
    if start_time == HISTORICAL_DATE:
        historical_segments.append(seg)
    else:
        deletable_segments.append(seg)

successLogger.debug(f"Historical segments (kept): {len(historical_segments)}")
successLogger.debug(f"Deletable segments: {len(deletable_segments)}")


# --------------------------------------------------
# 4. Delete NON-Historical segments
# --------------------------------------------------
# for seg in deletable_segments:
#     seg_id = seg.get("identifier")
#     delete_url = f"{druid_segments_endpoint}/{seg_id}"

#     del_res = requests.delete(delete_url, headers=headers)

#     if del_res.status_code == 200:
#         successLogger.debug(f"Deleted segment: {seg_id}")
#     else:
#         errorLogger.error(f"Failed to delete segment {seg_id}")
#         errorLogger.error(f"Status: {del_res.status_code}")
#         errorLogger.error(del_res.text)


# --------------------------------------------------
# 5. Re-enable datasource
# --------------------------------------------------
enable_res = requests.get(druid_datasource_endpoint, headers=headers)

if enable_res.status_code in (200, 204):
    successLogger.debug(f"Datasource enabled: {datasource}")
else:
    errorLogger.error(f"Failed to enable datasource {datasource}")
    errorLogger.error(f"Status: {enable_res.status_code}")
    errorLogger.error(enable_res.text)


successLogger.debug("Segment cleanup process completed successfully.")
