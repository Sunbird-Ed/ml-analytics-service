import os
from aws import AWS
from oracle import Oracle
from ms_azure import MSAzure
from gcp import GCP
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

    def upload_to_cloud(self, blob_Path, local_Path, file_Name):
        '''
        Function to upload the file to respective cloud based on the available section in the config file

        :param: blob_Path - The path where the file would be stored in the cloud 
        :param: local_Path - The path where the file is in the local server
        :param: file_Name - The name of the file that is being updated
        '''
        for elements in self.sections:
# AZURE
            if elements == "AZURE":
                accKey_exists=config.has_option("AZURE", "account_key")
                sasToken_exists=config.has_option("AZURE", "sas_token")
                if accKey_exists == True :
                    azure_service = MSAzure(
                     accountName = config.get("AZURE", "account_name"), 
                     sasToken = None, 
                     containerName = config.get("AZURE", "container_name"), 
                     accountKey = config.get("AZURE", "account_key")
                    )
                if sasToken_exists == True :
                   azure_service = MSAzure(
                     accountName = config.get("AZURE", "account_name"), 
                     sasToken = config.get("AZURE", "sas_token"), 
                     containerName = config.get("AZURE", "container_name"), 
                     accountKey = None
                   )
                azure_service.upload_files(blobPath = blob_Path, localPath = local_Path, fileName = file_Name)
# GCP
            elif elements == "GCP":
                gcp_service = GCP(
                    secret_data = config.get("GCP", "secret_data"), 
                    bucketName = config.get("GCP", "bucket_name")
                )
                gcp_service.upload_files(blobPath  = blob_Path, localPath = local_Path, fileName = file_Name)
# AWS            
            elif elements == "AWS":
                s3_service = AWS(
                    accessKey = config.get("AWS", "access_key"),
                    secretAccessKey = config.get("AWS", "secret_access_key"),
                    regionName = config.get("AWS", "region_name"),
                    bucketName = config.get("AWS", "bucket_name")
                )
                s3_service.upload_files(bucketPath = blob_Path, localPath = local_Path, fileName = file_Name)
# Oracle            
            elif elements == "ORACLE":
                oracle_service = Oracle(
                    regionName = config.get("ORACLE", "region_name"),
                    accessKey = config.get("ORACLE", "access_key"),
                    secretAccessKey = config.get("ORACLE", "secret_access_key"),
                    endpoint_url = config.get("ORACLE", "endpoint_url"),
                    bucketName = config.get("ORACLE", "bucket_name")
                )
                oracle_service.upload_files(bucketPath = blob_Path, localPath = local_Path, fileName = file_Name)

    def upload_to_NVSK_cloud(self, blob_Path, local_Path, file_Name,customBucket = False):
        '''
        Function to upload the file to respective cloud based on the available section in the config file

        :param: blob_Path - The path where the file would be stored in the cloud 
        :param: local_Path - The path where the file is in the local server
        :param: file_Name - The name of the file that is being updated
        '''

        oracle_service = Oracle(
        regionName = config.get("ORACLE", "region_name"),
        accessKey = config.get("ORACLE", "access_key"),
        secretAccessKey = config.get("ORACLE", "secret_access_key"),
        endpoint_url = config.get("ORACLE", "endpoint_url"),
        bucketName = "dev-diksha-privatereports"
                )     
        oracle_service.upload_files(bucketPath = blob_Path, localPath = local_Path, fileName = file_Name)