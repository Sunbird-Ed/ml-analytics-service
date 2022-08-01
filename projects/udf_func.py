import os, json,sys
from configparser import ConfigParser, ExtendedInterpolation

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

## This Function adds the metaInformation of task
def task_detail(task,del_flg):
  if (type(task)==dict) :
   taskObj = {}
   taskObj["_id"] = task["_id"]
   taskObj["tasks"] = task["name"]
   taskObj["deleted_flag"] = del_flg

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
                prjObj["prjEvi_type"] = prj["attachments"][cnt]["type"]
                if prjObj["prjEvi_type"] == "link":
                  prjObj["prj_evidence"] = prj["attachments"][cnt]["name"]
                else:
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
        try:
          del_flg = task["isDeleted"]
        except:
          del_flg = False
        ## To get greater length b/w evidence and subtask
        if attachLen > sub_tskLen:
         arr_len = attachLen        
        elif sub_tskLen > attachLen:
         arr_len = sub_tskLen        
        elif ((sub_tskLen == attachLen) & (sub_tskLen == 0)):
          if del_flg == False:
            taskObj = task_detail(task,del_flg)
         
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
          if del_flg == False:
           taskObj = task_detail(task,del_flg)
           try :
             taskObj["taskEvi_type"] = task["attachments"][index]["type"]
             if taskObj["taskEvi_type"] == "link":
                 taskObj["task_evidence"] = task["attachments"][index]["name"]
             else:
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
                try:
                  sub_del_flg = task["children"][index]["isDeleted"]
                except:
                  sub_del_flg = False                  
                if sub_del_flg == False:
                  taskObj["sub_task_date"] = task["children"][index]["syncedAt"]
                  taskObj["sub_task_id"] = task["children"][index]["_id"]
                  taskObj["sub_task_status"] = task["children"][index]["status"]
                  taskObj["sub_task_deleted_flag"] = sub_del_flg
                  try:
                    taskObj["sub_task_start_date"] = task["children"][index]["startDate"]
                  except KeyError:
                    taskObj["sub_task_start_date"] = ''
                  try:
                    taskObj["sub_task_end_date"] = task["children"][index]["endDate"]
                  except KeyError:
                    pass
                  taskarr.append(taskObj)
           except IndexError:
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
        while(ind < prjinfo_len):
          taskarr.append(prjinfo[ind])
          ind = ind + 1
    prj["taskarr"] = taskarr
    
    ## delete unwanted keys 
    del_keys = ["tasks"]
    for key in del_keys:
      try:
        del prj[key]         
      except KeyError:
        pass
    prjarr.append(prj)
  return prjarr

