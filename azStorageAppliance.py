from __future__ import print_function
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

try:

    # get the connection string
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # main menu loop
    while True:
        request = input("Do you wish to [(s)earch] or [(d)ownload] or [exit]? > ")
        request = request.strip().lower()
        print("")

        # exit the loop
        if request == "exit":
            break

        #search for records
        elif request == "search" or request == "s":
            choice = input("SEARCH> All containers [all], a container [con], or an object [obj]? > ")
            choice = choice.strip().lower()
            print("")
            # list all containers and blobs 
            if choice == "all":
                container_list = blob_service_client.list_containers()
                for c in container_list:
                    container_client = blob_service_client.get_container_client(c.name)
                    blob_list = container_client.list_blobs()
                    print(c.name)
                    for blob in blob_list:
                        print("\t" + blob.name)
                print("")
            # list everything in a container
            elif choice == "con":
                con_name = input("CONTAINER> Name? > ")
                con_name = con_name.strip().lower()
                container_list = blob_service_client.list_containers()
                con_name_list = []
                for c in container_list:
                    con_name_list.append(c.name)
                if not(con_name in con_name_list):
                    print("That container does not exist. Returning to main menu.")
                    print("")
                    continue
                else:
                    print("Listing Blobs:")
                    container_client = blob_service_client.get_container_client(con_name)
                    blob_list = container_client.list_blobs()
                    for blob in blob_list:
                        print("\t" + blob.name)
                    
                print("")
            # list the containter of an object
            elif choice == "obj":
                obj_name = input("OBJECT> Name? > ")
                obj_name = obj_name.strip().lower()
                obj_found = False
                for container in blob_service_client.list_containers():
                    for blob in blob_service_client.get_container_client(container['name']).list_blobs():
                        if blob['name'] == obj_name:
                            print('Object found in container: \n\t ' + container['name'])
                            obj_found = True        
                if not(obj_found):
                     print("No objects matching the name \"" + obj_name + "\" were found.")
                print("")
            else:
                print("Invalid choice. Returning to the main menu.")
                continue

        # download records
        elif request == "download" or request == "d":
            con_name = input("CONTAINER> Name? > ")
            con_name = con_name.strip().lower()

            container_list = blob_service_client.list_containers()
            con_name_list = []
            for c in container_list:
                con_name_list.append(c.name)
            if not(con_name in con_name_list):
                print("That container \"" + con_name + "\" does not exist. Returning to main menu.")
                print("")
                continue
            else:
                obj_name = input("OBJECT> Name? > ")
                obj_name = obj_name.strip().lower()
                container_client = blob_service_client.get_container_client(con_name)
                blob_list = container_client.list_blobs()
                blob_name_list = []
                for blob in blob_list:
                    blob_name_list.append(blob.name)
                if not(obj_name in blob_name_list):
                    print("There is no object named \"" + obj_name + "\" in the container \"" + con_name + "\". Returning to main menu.")
                    print("")
                    continue
                else:
                    local_path = "./download"
                    download_file_path = os.path.join(local_path,obj_name)
                    blob_client = blob_service_client.get_blob_client(container=con_name, blob=obj_name)
                    print("\nDownloading blob to \n\t" + download_file_path)
                    with open(download_file_path, "wb") as download_file:
                        download_file.write(blob_client.download_blob().readall())
            print("")

        # loop back to main menu
        else:
            print("Invalid choice. Please select one of the three provided options.")



    print("Exiting")
except Exception as ex:
    print('Exception:')
    print(ex)