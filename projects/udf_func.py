import os, json,sys
from configparser import ConfigParser, ExtendedInterpolation

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

## This Function adds the metaInformation of task
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

## This Function recreates the each doc with flattened task data
def recreate_task_data(prj_data):
  prjarr = []
  for prj in prj_data:
    prjinfo = []
    ## creating project level remarks and evidence obj to avoid repetition
    try:
      if prj["attachments"]:
        for cnt in range(len(prj["attachments"])):
            prjObj = {}
            if cnt == 0:
                try :
                    prjObj["prj_remarks"] = prj["remarks"]
                except :
                    KeyError
            try:
                prjObj["prj_evidence"] = prj["attachments"][cnt]["sourcePath"]
            except KeyError:
                pass
            prjinfo.append(prjObj)
    except KeyError:
      try :
        if prj["remarks"]:
          prjObj = {}
          prjObj["prj_remarks"] = prj["remarks"]
          prjinfo.append(prjObj)
      except KeyError:
        pass

    taskarr = []
    for  task in prj["tasks"]:        
        arr_len = 0
        try :
          attachLen = len(task["attachments"])
        except:
          attachLen = 0
        try :
          sub_tskLen = len(task["children"])
        except:
          sub_tskLen = 0
        ## To get greater length b/w evidence and subtask
        if attachLen > sub_tskLen:
         arr_len = attachLen        
        elif sub_tskLen > attachLen:
         arr_len = sub_tskLen        
        elif ((sub_tskLen == attachLen) & (sub_tskLen == 0)):
         taskObj = task_detail(task)        
         
         ## add remarks value when arrlen is 0
         try:
            taskObj["remarks"] = task["remarks"]
         except Exception as e:
            pass

         taskarr.append(taskObj)
         arr_len = sub_tskLen  
        elif (sub_tskLen == attachLen):
         arr_len = sub_tskLen
       
        ## creating task level remarks and evidence obj to avoid repetition
        for index in range(arr_len):
           taskObj = task_detail(task)
           try :
             taskObj["task_evidence"] = task["attachments"][index]["sourcePath"]
           except :
             pass 
           try:
             taskObj["sub_task"] = task["children"][index]["name"]
           except :
             pass
           if index == 0:
             try:
               taskObj["remarks"] = task["remarks"]
             except :
               pass
           
           ## Sub task data    
           try :
            if "children":
             taskObj["sub_task_date"] = task["children"][index]["syncedAt"]
             taskObj["sub_task_id"] = task["children"][index]["_id"]
             taskObj["sub_task_status"] = task["children"][index]["status"]
             try:
              taskObj["sub_task_start_date"] = task["children"][index]["startDate"]
             except KeyError:
              taskObj["sub_task_start_date"] = ''
             try:
              taskObj["sub_task_end_date"] = task["children"][index]["endDate"]
             except KeyError:
              pass
             try :
              taskObj["sub_task_deleted_flag"] =task["children"][index]["isDeleted"]
             except :
              taskObj["sub_task_deleted_flag"] = False   
           except IndexError:
            pass
    
           taskarr.append(taskObj)

    ## Formatting project level remarks and evidence
    prjinfo_len = len(prjinfo)
    taskarr_len = len(taskarr)
    if ((taskarr_len > prjinfo_len) & (prjinfo_len !=0)) | ((taskarr_len == prjinfo_len) & (prjinfo_len !=0)):
      for ind in range(len(prjinfo)):
        taskarr[ind].update(prjinfo[ind])
    elif (taskarr_len < prjinfo_len):
      try:
        for ind in range(len(prjinfo)):
          prjinfo[ind].update(taskarr[ind])
          del((taskarr[ind]))
          taskarr.append(prjinfo[ind])
      except IndexError:
        taskarr.append(prjinfo[ind])
    prj["taskarr"] = taskarr

    ## delete unwanted keys 
    del_keys = ["attachments","tasks"]
    for key in del_keys:
      try:
        del prj[key]         
      except KeyError:
        pass
    prjarr.append(prj)

  return prjarr

