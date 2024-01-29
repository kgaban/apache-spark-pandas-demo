from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


class AzureBlobStorageClient:
    def __init__(self, account_name, account_key, container_name):
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name

        self.blob_service_client = self._create_blob_service_client()

    def _create_blob_service_client(self):
        connect_str = f'DefaultEndpointsProtocol=https;AccountName={
            self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net'
        return BlobServiceClient.from_connection_string(connect_str)

    def get_blob_list(self):
        container_client = self.blob_service_client.get_container_client(
            self.container_name)
        return [blob.name for blob in container_client.list_blobs()]

    def generate_sas_url(self, blob_name):
        sas = generate_blob_sas(account_name=self.account_name,
                                container_name=self.container_name,
                                blob_name=blob_name,
                                account_key=self.account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))

        return f'https://{self.account_name}.blob.core.windows.net/{self.container_name}/{blob_name}?{sas}'

    def read_blob_to_dataframe(self, sas_url):
        return pd.read_csv(sas_url)
