import os
from azure.storage.blob import BlockBlobService

class MSAzure:
    '''
    Class to inititate and upload data in Microsoft Azure
    '''
    def __init__(self, accountName, sasToken, containerName, accountKey):
        if accountKey:
            self.blob_service_client = BlockBlobService(account_name= accountName, account_key= accountKey)
        if sasToken:
            self.blob_service_client = BlockBlobService(account_name= accountName, sas_token= sasToken)
        self.container_name = containerName

    def upload_files(self, blobPath, localPath, fileName):
        self.blob_service_client.create_blob_from_path(
            self.container_name,
            os.path.join(blobPath, fileName),
            f"{localPath}/{fileName}"
        )