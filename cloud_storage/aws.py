import os
import boto3

class AWS:
    '''
    Class to inititate and upload data in Amazon Web Services
    '''
    def __init__(self, accessKey, secretAccessKey, regionName, bucketName):
        self.aws_client = boto3.resource(
            service_name = 's3',
            aws_access_key_id = accessKey,
            aws_secret_access_key = secretAccessKey,
            region_name = regionName
        )
        self.bucket_name = bucketName

    def upload_files(self, bucketPath, localPath, fileName):
        self.aws_client.Bucket(f'{self.bucket_name}').upload_file(Filename=f"{localPath}/{fileName}", Key=f"{bucketPath}{fileName}")

