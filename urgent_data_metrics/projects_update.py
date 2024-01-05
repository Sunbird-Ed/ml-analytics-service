import os,csv
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId

config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")


clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
programsCollec = db['programs']
projectId = ObjectId("6423d06380b5f300083f1763")

def fetcProjectsFromMongo(project_Id):
    return projectsCollec.find({
        'programInformation._id' : project_Id
    })

projectIdsToUpdate = []

def fetchProgramDetails(project_Id):
    return programsCollec.find(
        {
            '_id' : project_Id
        })

def updateProjectsMongo(project_Ids_list , programName , programDescription):
    update_operation = {
        '$set': {
            'programInformation.name': programName,
            'programInformation.description': programDescription
            # add more fields to update as needed
        }
    }
    result = projectsCollec.update_many({'_id': {'$in': project_Ids_list}},update_operation)
    if result.modified_count > 0:
        print(f"Update successful: Matched {result.matched_count} documents and modified {result.modified_count} documents.")
    else:
        print("No documents were modified.")

def generateCsv(fileName , data):
    if not os.path.exists(fileName):
        with open(fileName, 'w', newline='') as file:
            writer = csv.writer(file)
            field = ["projectSubmissionId", "programName"]
            writer.writerow(field)
    with open(fileName, 'a', newline='') as file:
        writer = csv.writer(file)
        field = ["projectSubmissionId", "programName"]
        if not os.path.exists(fileName):
            writer.writerow(field)
        writer.writerow(data)

updatedProgramName = ""
updatedProgramDescription = ""
for program in fetchProgramDetails(projectId):
    updatedProgramName = program['name']
    updatedProgramDescription = program['description']


output_before_update = "urgent_data_metrics/before_data.csv"

for project in fetcProjectsFromMongo(projectId):
    projectIdsToUpdate.append(project['_id'])
    generateCsv(output_before_update , [str(project['_id']),project['programInformation']['name']])

print("Fetched ",len(projectIdsToUpdate)," Documents for updating")

updateProjectsMongo(projectIdsToUpdate , updatedProgramName , updatedProgramDescription)

output_after_update = "urgent_data_metrics/after_data.csv"
projectIdsToUpdate = []
for project in fetcProjectsFromMongo(projectId):
    projectIdsToUpdate.append(project['_id'])
    generateCsv(output_after_update , [str(project['_id']),project['programInformation']['name']])

print("Fetched ",len(projectIdsToUpdate)," Documents after updating")