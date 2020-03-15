from __future__ import print_function
import os, json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:
    print("Populating containers")
    # Quick start code goes here
    
    # get the connection string
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    
    # read in json config
    with open('./config.json') as f:
        data = json.load(f)
    
    # Create a list of all existing container names
    container_list = blob_service_client.list_containers()
    con_name_list = []
    for c in container_list:
        con_name_list.append(c.name)

    # iterate through json
    for obj in data:

        # get the container name from the json
        container_name = obj["container"]
        container_name = container_name.lower()

        if container_name in con_name_list:
            print("Container name already exists. Continuing to blob upload.")
            container_client = blob_service_client.get_container_client(container_name)
        else:
            # Create the container
            print("\nCreating Container: \n\t" + container_name)
            container_client = blob_service_client.create_container(container_name)
            con_name_list.append(container_name)
        
        # create a list of existing blob names
        blob_list = container_client.list_blobs()
        blob_name_list = []
        for blob in blob_list:
            blob_name_list.append(blob.name)

        # set local data directory
        local_path = "./data"

        # loop through objects to upload as blobs 
        for o in obj["objects"]:
            o = o.lower()
            if o in blob_name_list:
                print("Blob name already exists. Skipping.")
                continue
            
            # create file path
            local_file_name = o
            blob_name_list.append(local_file_name)
            upload_file_path = os.path.join(local_path, local_file_name)
            
            #print(upload_file_path)

            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

            # Upload the created file
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)

    print("Done")
except Exception as ex:
    print('Exception:')
    print(ex)