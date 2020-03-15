from __future__ import print_function
from decimal import Decimal
import boto3, os, json

def main ():
    try:
        
        # Establish a connection to the AWS resource dynamodb
        dynamodb_res = boto3.resource('dynamodb')
        dynamodb_cli = boto3.client('dynamodb')        

        print("starting the create table operation")
        # create the table
        table = create_table(dynamodb_res, dynamodb_cli)

        
        # read in json config
        with open('./moviedata.json') as f:
            data = json.load(f, parse_float=Decimal)

        print("Starting the populate table operation...")
        # populate the table
        populate_db(table, data, dynamodb_res)

    except Exception as ex:
        print('Exception:')
        print(ex)

def create_table(dynamodb_res, dynamodb_cli):
    
    # Create a new table called MovieInfo
    try:
        table = dynamodb_res.create_table(
            TableName='Movies',
            KeySchema=[
                {
                    'AttributeName': 'year',
                    'KeyType': 'HASH'  #Partition key
                },
                {
                    'AttributeName': 'title',
                    'KeyType': 'RANGE'  #Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'year',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'title',
                    'AttributeType': 'S'
                },

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        print("Table status:", table.table_status, table.table_name)
        return table

    except dynamodb_cli.exceptions.ResourceInUseException:
        print('The table Movies already exists, and does not need to be created' )
        table = dynamodb_res.Table('Movies')
        return table

def populate_db(table, data, dynamodb_res):
    for j in data:
        year = j["year"]
        title = j["title"]
        info = j["info"]
        table.put_item(
            Item={
                'year': year,
                'title': title,
                'info': info
            }
        )
    print("Data upload complete.")



if __name__== "__main__":
    main()