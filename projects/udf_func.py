from bson.objectid import ObjectId
import os, json
import datetime
from configparser import ConfigParser, ExtendedInterpolation

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


def task_detail(task):
   taskObj = {}
   taskObj["_id"] = task["_id"]
   taskObj["tasks"] = task["name"]
   try:
      taskObj["deleted_flag"] = task["isDeleted"]
   except:
      taskObj["deleted_flag"] = False
   try:
      if len(task["attachments"]) > 0:
          taskObj["task_evidence_status"] = True
      else:
          taskObj["task_evidence_status"] = False
   except:
      taskObj["task_evidence_status"] =  False
   try:
     taskObj["remarks"] = task["remarks"]
   except:
     taskObj["remarks"] = ''
   try: 
     taskObj["assignee"] = task["assignee"]
   except KeyError:
     taskObj["assignee"] =''
   try:
     taskObj["startDate"] = task["startDate"]
   except KeyError:
     taskObj["startDate"] = ''
   try:
     taskObj["endDate"] = task["endDate"]
   except KeyError:
     taskObj["endDate"] = ''
   taskObj["syncedAt"] = task["syncedAt"]
   try:
     taskObj["status"] = task["status"]
   except:
     taskObj["status"] = ''
   return taskObj


def recreate_task_data(prj_data):
  prjarr = []
  for prj in prj_data:
    evidence = ''
    try :
      for evi in prj["attachments"]:
       try:
         if ((evidence == '') &  (evi["sourcePath"]!= None)):
           evidence = config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + evi["sourcePath"]
         else:
           evidence_url = config.get('ML_SURVEY_SERVICE_URL', 'evidence_base_url') + evi["sourcePath"]
           evidence = evidence + "," + evidence_url        
       except :
         pass
    except KeyError:
      pass 
    prj["evidence"] = evidence
    taskarr = []
    for task in prj["tasks"]:
        arr_len = 0
        try :
          attachLen = len(task["attachments"])
        except:
          attachLen = 0
        try :
          sub_tskLen = len(task["children"])
        except:
          sub_tskLen = 0
        if attachLen > sub_tskLen:
         arr_len = attachLen
        elif sub_tskLen > attachLen:
         arr_len = sub_tskLen
        elif ((sub_tskLen == attachLen) & (sub_tskLen == 0)):
         taskObj = task_detail(task)
         taskarr.append(taskObj)
         arr_len = sub_tskLen
        elif (sub_tskLen == attachLen):
         arr_len = sub_tskLen
        
        for i in range(arr_len):
           taskObj = task_detail(task)
           try :
             taskObj["task_evidence"] = task["attachments"][i]["sourcePath"]
           except :
             pass 
           try:
             taskObj["sub_task"] = task["children"][i]["name"]
           except :
             pass
           try :
            if "children":
             taskObj["sub_task_date"] = task["children"][i]["syncedAt"]
             taskObj["sub_task_id"] = task["children"][i]["_id"]
             taskObj["sub_task_status"] = task["children"][i]["status"]
             try:
              taskObj["sub_task_start_date"] = task["children"][i]["startDate"]
             except KeyError:
              taskObj["sub_task_start_date"] = ''
             try:
              taskObj["sub_task_end_date"] = task["children"][i]["endDate"]
             except KeyError:
              pass
             try :
              taskObj["sub_task_deleted_flag"] =task["children"][i]["isDeleted"]
             except :
              taskObj["sub_task_deleted_flag"] = False   
           except IndexError:
            pass 
           taskarr.append(taskObj)
    prj["taskarr"] = taskarr
    prjarr.append(prj)
  return prjarr


