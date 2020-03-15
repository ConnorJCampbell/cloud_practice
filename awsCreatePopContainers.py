from __future__ import print_function
import boto3, os, json

try:

    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    # read in json config
    with open('./config.json') as f:
        data = json.load(f)

    buck_name_list = []
    for bucket in s3_resource.buckets.all():
        buck_name_list.append(bucket.name)
    
    # iterate through json
    for j in data:

        # get the container name from the json
        bucket_name = j["container"]
        bucket_name = bucket_name.lower()
        bucket_name = "ccampb21-" + bucket_name

        if bucket_name in buck_name_list:
            print("Bucket name already exists. Continuing to object upload.")
            bucket = s3_resource.Bucket(bucket_name)  
        else:
            #create the bucket with the container name
            print("\nCreating Bucket: \n\t" + bucket_name)
            bucket = s3_resource.create_bucket(Bucket=bucket_name)
            buck_name_list.append(bucket_name)

        obj_name_list = []
        for obj in bucket.objects.all():
            obj_name_list.append(obj.key)

        # set local data directory
        local_path = "./data"

        # loop through objects to upload as blobs 
        for o in j["objects"]:
            
            # create file path
            local_file_name = o.lower()

            if local_file_name in obj_name_list:
                print("Object key already exists. Skipping.")
                continue

            obj_name_list.append(local_file_name)
            upload_file_path = os.path.join(local_path, local_file_name)

            # Create a blob client using the local file name as the name for the blob
            #data = open(upload_file_path, 'rb')
            #s3_resource.Bucket(bucket_name).put_object(Key=o, Body=data)
            s3_client.upload_file(upload_file_path, bucket_name, local_file_name)

            print("\nUploading to AWS:\n\t" + local_file_name)

except Exception as ex:
    print('Exception:')
    print(ex)