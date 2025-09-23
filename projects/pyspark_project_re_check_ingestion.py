# -----------------------------------------------------------------
# Name : pyspark_project_re_check_ingestion.py
# Author :Vivek
# Description : This script verifies missing program IDs across Druid datasources and 
# automatically triggers ingestion using updated specs.
# -----------------------------------------------------------------

import requests
import json
from configparser import ConfigParser, ExtendedInterpolation
import os
import re
import sys
import logging 
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

formatter = logging.Formatter('%(asctime)s - %(levelname)s')
successLogger = logging.getLogger('success log')
successLogger.setLevel(logging.DEBUG)
successHandler = RotatingFileHandler(config.get('LOGS', 'project_success'))
successBackuphandler = TimedRotatingFileHandler(config.get('LOGS','project_success'), when="w0",backupCount=1)
successHandler.setFormatter(formatter)
successLogger.addHandler(successHandler)
successLogger.addHandler(successBackuphandler)

DRUID_SQL_ENDPOINT = config.get("DRUID", "sql_url")
DRUID_BATCH_ENDPOINT = config.get("DRUID", "batch_url")
HEADERS = {"Content-Type": "application/json"}

# Data sources to query
DATASOURCES = [
    "sl-project",
    "ml-project-programLevel-status",
    "ml-project-status"
]

PROGRAM_FILE = config.get("OUTPUT_DIR", "program_text_file")

# ================== HELPERS ==================
def read_program_ids(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def construct_query(datasource):
    query = f"""
    SELECT DISTINCT program_id AS program_id
    FROM "{datasource}"
    """
    return {"query": query}

def run_query(payload):
    response = requests.post(DRUID_SQL_ENDPOINT, headers=HEADERS, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        successLogger.error(f"Error {response.status_code}: {response.text}")
        return []

# ================== INGESTION ==================
def ingest_from_spec(spec_str, program_unique_id):
    """
    Reads spec JSON, inspects inputSource type (local/s3/etc), and modifies accordingly.
    """
    spec = json.loads(spec_str)
    input_source = spec["spec"]["ioConfig"]["inputSource"]

    if input_source["type"] == "local":
        base_dir = input_source["baseDir"]
        filter_file = input_source["filter"]
        prefix = filter_file.replace(".json", "")
        spec["spec"]["ioConfig"]["inputSource"]["filter"] = f"{prefix}_{program_unique_id}.json"
        spec['spec']['ioConfig'].update({"appendToExisting": True})

    elif input_source["type"] in ("s3", "gcs", "http", "https"):
        uri = input_source["uris"][0]
        prefix = uri.replace(".json", "")
        current_cloud = re.split("://+", prefix)[0]
        path = re.split("://+", prefix)[1]
        spec["spec"]["ioConfig"]["inputSource"]["uris"][0] = f"{current_cloud}://{path}_{program_unique_id}.json"
        spec['spec']['ioConfig'].update({"appendToExisting": True})

    else:
        successLogger.error(f"⚠️ Unknown inputSource type: {input_source['type']}")
        return None

    return requests.post(DRUID_BATCH_ENDPOINT, data=json.dumps(spec), headers=HEADERS)

# ================== DATASOURCE MAPPING ==================
def ingest_sl_projects(program_unique_id):
    spec_str = config.get("DRUID", "project_injestion_spec")
    return ingest_from_spec(spec_str, program_unique_id)

def ingest_ml_projects_status(program_unique_id):
    spec_str = config.get("DRUID","ml_distinctCnt_projects_status_spec")
    return ingest_from_spec(spec_str, program_unique_id)

def ingest_ml_prgmlevel_projects(program_unique_id):
    spec_str = config.get("DRUID","ml_distinctCnt_prglevel_projects_status_spec")
    return ingest_from_spec(spec_str, program_unique_id)

# ================== MAIN ==================
if __name__ == "__main__":
    successLogger.info("🚀 Running re-check ingestion (auto-detect mode from spec)")

    program_ids = set(read_program_ids(PROGRAM_FILE))
    successLogger.info(f"Found {len(program_ids)} program IDs in {PROGRAM_FILE}")

    ingestion_map = {
        "sl-project": ingest_sl_projects,
        "ml-project-programLevel-status": ingest_ml_prgmlevel_projects,
        "ml-project-status": ingest_ml_projects_status
    }

    for datasource in DATASOURCES:
        payload = construct_query(datasource)
        result = run_query(payload)

        datasource_programs = set(row["program_id"] for row in result if row.get("program_id"))
        successLogger.info(f"\n=== Datasource: {datasource} ===")
        successLogger.info(f"Total program IDs in datasource: {len(datasource_programs)}")

        missing_in_ds = program_ids - datasource_programs
        if missing_in_ds:
            successLogger.error(f"❌ Missing in {datasource}: {sorted(missing_in_ds)}")

            for pid in missing_in_ds:
                resp = ingestion_map[datasource](pid)
                if resp and resp.status_code == 200:
                    successLogger.info(f"✅ Ingestion started for {pid} in {datasource}")
                else:
                    status = resp.status_code if resp else "N/A"
                    text = resp.text if resp else "Invalid ingestion spec"
                    successLogger.error(f"⚠️ Failed ingestion for {pid} in {datasource}, {status}: {text}")
        else:
            successLogger.info(f"✅ All program IDs are present in {datasource}")