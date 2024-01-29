from azure_client import AzureBlobStorageClient
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


def main():
    # Retrieve credentials from environment variables
    account_name = os.getenv('STORAGE_ACCOUNT_NAME')
    account_key = os.getenv('STORAGE_ACCOUNT_KEY')
    container_name = os.getenv('CONTAINER_NAME')

    # Create an instance of AzureBlobStorageClient
    azure_client = AzureBlobStorageClient(
        account_name, account_key, container_name)

    # Get a list of all blob files in the container
    blob_list = azure_client.get_blob_list()

    df_list = []

    # Generate a shared access signature for files and load them into Python
    for blob_name in blob_list:
        sas_url = azure_client.generate_sas_url(blob_name)
        df = azure_client.read_blob_to_dataframe(sas_url)
        df_list.append(df)

    df_combined = pd.concat(df_list, ignore_index=True)

    print(df_combined)


if __name__ == "__main__":
    main()
