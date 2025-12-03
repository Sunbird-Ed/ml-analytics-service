import logging
import os
import glob
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
import os
from configparser import ConfigParser, ExtendedInterpolation

# -------------------------------
# Config Setup
# -------------------------------
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

log_dir = config.get("LOGS", "program_update_log_dir")

mongo_url = config.get("MONGO", "url")
mongo_db = config.get("MONGO", "database_name")
programs_collection = config.get("MONGO", "programs_collection")
solutions_collection = config.get("MONGO", "solutions_collection")

# -------------------------------
# Logging Setup
# -------------------------------
def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    retention_period = timedelta(days=7)
    cutoff_date = datetime.now() - retention_period
    
    for log_file in glob.glob(os.path.join(log_dir, "program_update_*.log")):
        try:
            file_date_str = log_file.split("_")[-1].replace(".log", "")
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
            if file_date < cutoff_date:
                os.remove(log_file)
                logging.info(f"Deleted old log file: {log_file}")
        except Exception as e:
            logging.error(f"Error checking/deleting log file {log_file}: {e}")

    today_str = datetime.now().strftime("%Y-%m-%d")
    log_filename = os.path.join(log_dir, f"program_update_{today_str}.log")
    
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info(f"Logging to: {log_filename}")

setup_logging()

# -------------------------------
# MongoDB connection
# -------------------------------
client = MongoClient(mongo_url)
db = client[mongo_db]

programs_col = db["programs"]
solutions_col = db["solutions"]

# -------------------------------
# STEP 1: Get distinct program_ids
# -------------------------------
program_ids = programs_col.distinct(
    "_id",
    {
        "isAPrivateProgram": False,
        "deleted": False
    }
)

program_solution_map = {}   
logging.info(f"Total Programs fetched from mongo: {len(program_ids)}")

# -------------------------------
# STEP 2: Loop on program_ids
# -------------------------------
updated_programs_count = 0
for pid in program_ids:
    matching_solutions = solutions_col.find({
            "programId": pid,
            "isAPrivateProgram": False,
            "isReusable": False,
        }, {"_id": 1})

    solution_ids = [sol["_id"] for sol in matching_solutions]

    program_solution_map[str(pid)] = [str(sid) for sid in solution_ids] 

    logging.info("----------------------------------")
    logging.info(f"Processing for program id: {pid}, Solutions found: {len(solution_ids)}")
    logging.info(f"Solutions Ids:{solution_ids}")

    # -------------------------------
    # STEP 3: Update program.components
    # -------------------------------
    if solution_ids:
        result = programs_col.update_one(
            {"_id": pid},
            {"$addToSet": {"components": {"$each": solution_ids}}}
        )
        
        if result.modified_count > 0:
            updated_programs_count += 1
            logging.info(f"Solution Ids updated in the program components.")
        else:
            logging.info(f"No changes for Program {pid} (components already up to date)")
    else:
        logging.info(f"No solutions found for Program {pid}, skipping update")
    
# -------------------------------
# Final Program → Solutions map
# -------------------------------
logging.info("\n ----------------------------------")
logging.info(f"Total Programs updated: {updated_programs_count}")
