from dotenv import load_dotenv
from io import BytesIO
import os
from azure.storage.filedatalake import DataLakeServiceClient, ContentSettings

load_dotenv()


def _create_storage_account_connection():
    # AZURE STORAGE CREDENTIALS AND CONTAINER CLIENT 
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    connection_string = (
        f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};"
        f"AccountKey={storage_account_key};EndpointSuffix=core.windows.net"
    )

    try:
        datalake_service_client = DataLakeServiceClient.from_connection_string(
            connection_string
        )

        return datalake_service_client
    except Exception as e:
        pass
        raise AzureConnectionError({"Azure Error": e})


# AZURE STORAGE CREDENTIALS AND CONTAINER CLIENT

def _create_filesystem_client_raw():
    datalake_service_client = _create_storage_account_connection()
    filesystem_raw = os.getenv("FILESYSTEM_RAW")
    filesystem_client_raw = datalake_service_client.get_file_system_client(filesystem_raw)
    return filesystem_client_raw


def _create_filesystem_client_silver():
    datalake_service_client = _create_storage_account_connection()
    filesystem_silver = os.getenv("FILESYSTEM_SILVER")
    filesystem_client_silver = datalake_service_client.get_file_system_client(filesystem_silver)
    return filesystem_client_silver


def _create_filesystem_client_gold():
    datalake_service_client = _create_storage_account_connection()
    filesystem_gold = os.getenv("FILESYSTEM_GOLD")
    filesystem_client_gold = datalake_service_client.get_file_system_client(filesystem_gold)
    return filesystem_client_gold


def download_file(file_directory, filesystem_client):
    file_client = filesystem_client.get_file_client(file_directory)
    file = file_client.download_file().readall()
    return file
