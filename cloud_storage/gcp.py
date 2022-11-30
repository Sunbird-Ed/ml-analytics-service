import os
from google.cloud import storage
   
class GCP:
    '''
    Class to inititate and upload data in Google Cloud
    '''
        
    def __init__(self, secret_data, bucketName):
        self.secret_data = secret_data
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']=secret_data
        self.storage_client = storage.Client(secret_data)
        self.bucket = self.storage_client.get_bucket(bucketName)

    def upload_files(self, blobPath, localPath, fileName):
        blob = self.bucket.blob(f"{blobPath}{fileName}")
        blob.upload_from_filename(f"{localPath}/{fileName}")
