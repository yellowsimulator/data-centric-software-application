""" 
This module containes all the functions needed 
to interact with an azure datalake. 

The functions are: 
1 - create a container (create_container)
2 - create a directory inside a container (create_directory)
3 - list the content of a directory (list_directory_contents)
4 - upload file to a directory (upload_file_to_directory)
5 - download file to a directory
6 - rename a directory (rename_directory)

An Azure datalake has containers as top level
and directory as lower level. The datalake can 
have multiple containers and each container 
can contain multiple directories within which files 
are stored.

Each function above need a client object. A client object
contains credentials in order to access the datalake resources.
There are two functions that return a client objects:

1 - get_account_key_client
This function returns a client based on credentials such as
the account storge name, saved in the parameter STORAGE_ACCOUNT_NAME,
and the storage account key saved in the parameter STORAGE_ACCOUNT_KEY

2 - get_active_directory_client
This function returns a client based on azure active directory 
credentials such as a tenant id saved in the parameter TENENT_ID,
client id saved in the parmeter CLIENT_ID and client secrete
saved in the parameter CLIENT_SECRET.

"""
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake._models import ContentSettings
from azure.core._match_conditions import MatchConditions
from azure.identity import ClientSecretCredential
from progress.bar import IncrementalBar
from glob import glob
import pandas as pd
import emojis
import os


# Credentials saved in the environment variables
STORAGE_ACCOUNT_KEY = os.getenv('STORAGE_ACCOUNT_KEY')
STORAGE_ACCOUNT_NAME = os.getenv('STORAGE_ACCOUNT_NAME')



def get_service_client():
    """Returns azure service client.
    """
    url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    try:
        client = DataLakeServiceClient(account_url=url, \
                                    credential=STORAGE_ACCOUNT_KEY)
        return client
    except Exception as e:
        print("Your storage account name or/and key are invalid")
        print(" ")
        print(e)


def create_container(container_name: str):
    """Creates a container on the datalake.
    
    Args:
        container_name: Datalake container name.
    """
    try:
        SERVICE_CLIENT = get_service_client()
        SERVICE_CLIENT.create_file_system(file_system=container_name)
        print(f'container {container_name} created')
    except Exception as e:
        print(e)



def create_directory(container_name: str, directory_name: str):
    """Creates a directory inside a container.

    Args:
        container_name: Existing datalake container name.
        directory: Directory name you wish to create.
    """
    try:
        SERVICE_CLIENT = get_service_client()
        file_system_client = SERVICE_CLIENT.get_file_system_client(file_system=container_name)
        file_system_client.create_directory(directory_name)
        print(f'Directory {directory_name} created in {container_name} container')
    except Exception as e:
     print(e)


def list_directory_contents(container_name: str):
    """lists all file in a container.

    Args:
        container_name: datalake conainer name.
    """
    try:
        SERVICE_CLIENT = get_service_client()
        file_system_client = SERVICE_CLIENT.get_file_system_client(file_system=container_name)
        paths = file_system_client.get_paths()
        files = [file.name for file in paths]
        return files
    except Exception as e:
        print(e)


def upload_file_to_directory(container_name: str, directory_name: str, \
                             file_to_upload: str):
    """Uploads a file to Azure datalake.

    Args:
        container_name: Azure container name.
        directory_name: Directory name in the container.
        file_to_upload: path to the file to be uploaded.
    """
    try:
        SERVICE_CLIENT = get_service_client()
        file_system_client = SERVICE_CLIENT.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        file_name = os.path.basename(file_to_upload)
        file_client = directory_client.create_file(file_name)
        with open(file_to_upload,'rb') as local_file:
            file_contents = local_file.read()
            file_client.upload_data(file_contents, overwrite=True)
    except Exception as e:
        print(e)


def download_file_from_directory(container_name: str, directory_name:str,\
                                 file_to_download: str, output_folder: str):
    """Download a file from a directory.
    
    Args:
        container_name: Datalake container name.
        directory_name: Directory in the continer containing files.
        file_to_download: the name of the file to download.
        output_folder: the destination folder
    """
    try:
        SERVICE_CLIENT = get_service_client()
        file_system_client = SERVICE_CLIENT.get_file_system_client(file_system=container_name)
        directory_client = file_system_client.get_directory_client(directory_name)
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        with open(f"{output_folder}/{file_to_download}",'wb') as local_file:
            file_client = directory_client.get_file_client(file_to_download)
            download = file_client.download_file()
            downloaded_bytes = download.readall()
            local_file.write(downloaded_bytes)
    except Exception as e:
        pass


if __name__=='__main__': 
    container_name = 'music'
    # create_container(container_name)
    # create_directory(container_name, directory_name)
    # upload_file_to_directory(container_name, directory_name, file_to_upload)
   
    

    
    

    

   