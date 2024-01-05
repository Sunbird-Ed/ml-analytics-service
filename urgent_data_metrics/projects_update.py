import os,csv
from configparser import ConfigParser,ExtendedInterpolation
from pymongo import MongoClient
from bson.objectid import ObjectId

# fetch config file 
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path[0] + "/config.ini")

# connection to mongo db 
clientProd = MongoClient(config.get('MONGO', 'url'))
db = clientProd[config.get('MONGO', 'database_name')]
projectsCollec = db[config.get('MONGO', 'projects_collection')]
programsCollec = db['programs']

# _id of the program to update 
programId = ObjectId("6423d06380b5f300083f1763")

# function to fetch projects based on program id and isAPrivateProgram 
def fetcProjectsFromMongo(programId):
    return projectsCollec.find({
        'programInformation._id' : programId,
        'isAPrivateProgram' : False
    })

# list for project _ids to update 
projectIdsToUpdate = []

# fetch program details to update projects collection 
def fetchProgramDetails(programId):
    return programsCollec.find(
        {
            '_id' : programId
        })
# bulk update projects with updated programName and description 
def updateProjectsMongo(project_Ids_list , programName , programDescription):
    update_operation = {
        '$set': {
            'programInformation.name': programName,
            'programInformation.description': programDescription
        }
    }
    result = projectsCollec.update_many({'_id': {'$in': project_Ids_list}},update_operation)

    # success or failure message after update 
    if result.modified_count > 0:
        print(f"Update successful: Matched {result.matched_count} documents and modified {result.modified_count} documents.")
    else:
        print("No documents were modified.")

# function to generate output CSV 
def generateCsv(fileName , data):
    # create file and add the headers - for the first time 
    if not os.path.exists(fileName):
        with open(fileName, 'w', newline='') as file:
            writer = csv.writer(file)
            field = ["projectSubmissionId", "programName"]
            writer.writerow(field)
    # append the rows 
    with open(fileName, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(data)

updatedProgramName = ""
updatedProgramDescription = ""

# fetch program details from mongo 
for program in fetchProgramDetails(programId):
    updatedProgramName = program['name']
    updatedProgramDescription = program['description']

# output file path before update 
output_before_update = "urgent_data_metrics/before_data.csv"

# fetch project details from mongo 
for project in fetcProjectsFromMongo(programId):
    # append the project id to update 
    projectIdsToUpdate.append(project['_id'])
    # generate csv before update 
    generateCsv(output_before_update , [str(project['_id']),project['programInformation']['name']])

print("Fetched ",len(projectIdsToUpdate)," Documents for updating")

# update projects collection
updateProjectsMongo(projectIdsToUpdate , updatedProgramName , updatedProgramDescription)

# output file path after update 
output_after_update = "urgent_data_metrics/after_data.csv"
# clear the projects to update list - for final count 
projectIdsToUpdate = []

# fetch project details from mongo 
for project in fetcProjectsFromMongo(programId):
    # append the project ids for count
    projectIdsToUpdate.append(project['_id'])
    # generate csv after update 
    generateCsv(output_after_update , [str(project['_id']),project['programInformation']['name']])

print("Fetched ",len(projectIdsToUpdate)," Documents after updating")