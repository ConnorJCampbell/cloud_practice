from __future__ import print_function
import boto3, os, json

def main ():
    try:
        # Establish a connection to the AWS resource dynamodb
        dynamodb_res = boto3.resource('dynamodb')
        dynamodb_cli = boto3.client('dynamodb')        

        request = input("Do you wish to search by (p)artition or (s)ort key? > ")
        request = request.strip().lower()
        if request == "p":
            pass
        elif request == "s":
            pass
        else:
            print("Invalid selection. defaulting to")
        print("")


    except Exception as ex:
        print('Exception:')
        print(ex)

def

if __name__== "__main__":
    main()