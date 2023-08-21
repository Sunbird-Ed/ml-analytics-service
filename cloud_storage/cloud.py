import os,requests,json , cloud_storage.constants as constants
from configparser import ConfigParser,ExtendedInterpolation

# Reading the config file
config_path = os.path.split(os.path.dirname(os.path.abspath(__file__)))
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(f"{config_path[0]}/config.ini")

class MultiCloud:
    '''
    Class to initiate and store data in Cloud Service from AWS/GCP/AZURE/ORACLE
    '''

    def __init__(self):
        '''
        Captures all the available section in the config file and stores it into a list
        '''
        self.sections = config.sections()


        
    def upload_to_cloud(self, filesList ,folderPathName, local_Path):
        '''
        Function to upload the file to respective cloud using the core services
        :param: filesList - List of file names
        :param: folderPathName - Key in config file to fetch the cloud folder path
        :param: local_Path - The path where the file is in the local server

        '''

        url = str(config.get("ML_CORE_SERVICE_URL", "url")) + str(constants.pre_signed_url)

        payload = json.dumps({
          "request": {
            "files": filesList   
          }
          ,
          "action": "signedUrl",
          "folderPath": str(config.get("COMMON", folderPathName)),
          "bucketName": str(config.get("CLOUD", "container_name")),
          "expiresIn": constants.expiry,
          "operation" : "write"
        })
        headers = {
          'internal-access-token': str(config.get("API_HEADERS", "internal_access_token")),
          'Content-Type': 'application/json'
        }

        response = requests.request("POST", url, headers=headers, data=payload)


        preSignedResponse = {}

        if response.status_code in [constants.status_code_1 , constants.status_code_2]:
            preSignedResponse['status_code'] = response.status_code
            response = response.json()
            preSignedResponse['success'] = True
            preSignedResponse['folderPathName'] = str(config.get("CLOUD", folderPathName))
            for index in response['result']['files']:
                preSignedResponse['cloudStorage'] = index['cloudStorage'].lower()
                preSignedResponse['inputSource'] = index['inputSource']
                preSignedResponse['presigned'] = index['url']
            preSignedResponse['error'] = ""
        else:
            preSignedResponse['success'] = False
            preSignedResponse['error'] = response.text
            return preSignedResponse

        json_path = local_Path
        with open(json_path, 'rb') as json_file:
            json_data = json_file.read()

        payload = {}
        payload = json_data

        headers = {}

        headers = {
          'x-ms-blob-type': 'BlockBlob',
          'Content-Type': 'multipart/form-data'
        }
        response = {}
        response = requests.request("PUT", preSignedResponse['presigned'], headers=headers, data=payload)

        if response.status_code in [constants.status_code_1 , constants.status_code_2]:
            return preSignedResponse
        else:
            preSignedResponse['success'] = False
            preSignedResponse['error'] = response.text
            return preSignedResponse

