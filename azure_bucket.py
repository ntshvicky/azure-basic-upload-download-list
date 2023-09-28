import os, uuid, base64
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

from tqdm import tqdm

import concurrent.futures

from dotenv import load_dotenv
load_dotenv()

ACCOUNT_URL = os.getenv('ACCOUNT_URL')
CONNECTION_STRING = os.getenv('CONNECTION_STRING')
CONTAINER_NAME = os.getenv('CONTAINER_NAME')

def returnBasePath():
    return "{}/{}".format(ACCOUNT_URL, CONTAINER_NAME)

# upload file from given path
# file_path - file path - string
# object_name - file_path in bucket - string
def upload_file_input(file_path, object_name, mimetype='image/jpeg'):
    #object_name #str(uuid.uuid4())
    blob = BlobClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME, blob_name=object_name)
    #print(blob_client)

    print("\nUploading to Azure Storage as blob:\n\t" + file_path)

    # Upload the created file
    with open(file=file_path, mode="rb") as data:
        blob.upload_blob(data)

    #print(blob.__dict__)
        
    return True, "%s/%s" % (returnBasePath(), blob.blob_name)

# upload base64 file
# bucket_file_name - file path in bucket - string
# image_base64 - base64 encoded string - string
# mimetype - mimetype of image

def upload_base64file(s3_file_name, image_base64, mimetype='image/jpeg'):
    blob = BlobClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME, blob_name=s3_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + s3_file_name)

    # Upload the created file
    base64Obj = base64.decodebytes(image_base64.encode())
    blob.upload_blob(base64Obj)
    #print(blob.__dict__)
        
    return True, "%s/%s" % (returnBasePath(), blob.blob_name)


# download file from bucket
# file_name - full path of bucket
def download_file(folder_id, file_name):
    output = os.path.join('downloads', folder_id, file_name)

    blob = BlobClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME, blob_name=file_name)

    #print("\nDownloading blob to \n\t" + output)

    with open(file=output, mode="wb") as download_file:
        blob_data = blob.download_blob()
        blob_data.readinto(download_file)

    return output


def download_blob(blob, container_client, local_directory):
    blob_name = blob.name
    blob_client = container_client.get_blob_client(blob_name)

    # Create the local directory structure if it doesn't exist
    local_path = os.path.join(local_directory, blob_name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    # Download the blob to the local directory with a progress bar
    with open(local_path, "wb") as my_blob:
        blob_data = blob_client.download_blob()
        for chunk in blob_data.chunks():
            my_blob.write(chunk)
            
def download_container_contents(local_directory):
    container_client = ContainerClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME)

    print("\nKindly wait! Downloading container contents to " + local_directory + "...")
    # List all blobs in the container
    blobs = container_client.list_blobs()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url  = {executor.submit(download_blob, blob, container_client, local_directory,): blob for blob in blobs}
        for future in concurrent.futures.as_completed(future_to_url):
            print(future.result())

    print("\nDone!")
    return True
        


# list files in s3 bucket
def list_files():
    contents = []
    container = ContainerClient.from_connection_string(conn_str=CONNECTION_STRING, container_name=CONTAINER_NAME)

    blob_list = container.list_blobs()
    for blob in blob_list:
        #print(blob)
        contents.append(blob.name)

    return contents
