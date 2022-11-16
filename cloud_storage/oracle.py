import os
import boto3

class Oracle:
    '''
    Class to inititate and upload data in Oracle
    '''

    def __init__(self, regionName, accessKey, secretAccessKey, endpoint_url, bucketName):
        self.oracle = boto3.client(
            service_name = 's3',
            region_name = regionName,
            aws_access_key_id = accessKey,
            aws_secret_access_key = secretAccessKey,
            endpoint_url = endpoint_url
        )
        self.bucket = bucketName
        
    def upload_files(self, bucketPath, localPath, fileName):
        with open(f"{localPath}/{fileName}", "rb") as file:
            self.oracle.upload_fileobj(file, self.bucket, f"{bucketPath}/{fileName}")