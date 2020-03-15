from __future__ import print_function
import boto3, os

try:

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

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
                for bucket in s3_resource.buckets.all():
                    print(bucket.name)
                    for obj in bucket.objects.all():
                        print("\t" + obj.key)

                print("")
                pass
            # list everything in a container
            elif choice == "con":
                buck_name = input("CONTAINER> Name? > ")
                buck_name = buck_name.strip().lower()
                buck_name_list = []
                for bucket in s3_resource.buckets.all():
                    buck_name_list.append(bucket.name)
                if not(buck_name in buck_name_list):
                    print("The bucket name \"" + buck_name + "\" does not exist. Returning to main menu.")
                    print("") 
                    continue
                else:
                    print("Listing Objects:")
                    bucket = s3_resource.Bucket(buck_name)
                    for obj in bucket.objects.all():
                        print("\t" + obj.key)
                print("")
            # list the containter of an object
            elif choice == "obj":
                obj_name = input("OBJECT> Name? > ")
                obj_name = obj_name.strip().lower()

                obj_found = False
                for bucket in s3_resource.buckets.all():
                    for obj in bucket.objects.all():
                        if obj.key == obj_name:
                            print('Object found in bucket: \n\t ' + bucket.name)
                            obj_found = True        
                if not(obj_found):
                     print("No objects matching the name \"" + obj_name + "\" were found.")
                print("")
                pass
            else:
                print("Invalid choice. Returning to the main menu.")
                continue

        # download records
        elif request == "download" or request == "d":
            buck_name = input("CONTAINER> Name? > ")
            buck_name = buck_name.strip().lower()

            buck_name_list = []
            for bucket in s3_resource.buckets.all():
                buck_name_list.append(bucket.name)
            if not(buck_name in buck_name_list):
                print("The bucket name \"" + buck_name + "\" does not exist. Returning to main menu.")
                print("") 
                continue
            else:
                obj_name = input("OBJECT> Name? > ")
                obj_name = obj_name.strip().lower()
                obj_name_list = []
                bucket = s3_resource.Bucket(buck_name)
                for obj in bucket.objects.all():
                    obj_name_list.append(obj.key)
                if not(obj_name in obj_name_list):
                    print("There is no object named \"" + obj_name + "\" in the bucket \"" + buck_name + "\". Returning to main menu.")
                    print("")
                    continue
                else:
                    local_path = "./download"
                    download_file_path = os.path.join(local_path,obj_name)
                    print("\nDownloading object to \n\t" + download_file_path)
                    s3_client.download_file(buck_name, obj_name, download_file_path)
            
            print("")
            pass
        else:
            print("Invalid choice. Please select one of the three provided options.")



    print("Exiting")
except Exception as ex:
    print('Exception:')
    print(ex)