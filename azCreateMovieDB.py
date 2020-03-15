from __future__ import print_function
import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import os, json

def main ():
    try:
        
        uri = os.environ['ACCOUNT_URI']
        key = os.environ['ACCOUNT_KEY']

        # <create_cosmos_client>
        #client = cosmos_client.CosmosClient(uri, {'masterKey': key})
        
        # </create_cosmos_client>
        client = CosmosClient(uri, key)

        database_name = 'movieDatabase'
        try:
            database = client.CreateDatabase({'id': database_name})
        except errors.HTTPFailure:
            database = client.ReadDatabase("dbs/" + database_name)

        container_definition = {'id': 'movies',
                                'partitionKey':
                                            {
                                                'paths': ['/year'],
                                                'kind': documents.PartitionKind.Hash
                                            },
                                'partitionKey':
                                            {
                                                'paths': ['/title'],
                                                'kind': documents.PartitionKind.Range
                                            }
                                }

        try:
            container = client.CreateContainer("dbs/" + database['id'], container_definition, {'offerThroughput': 400})
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                container = client.ReadContainer("dbs/" + database['id'] + "/colls/" + container_definition['id'])
            else:
                raise e

        # read in json config
        with open('./moviedata.json') as f:
            data = json.load(f)

    except Exception as ex:
        print('Exception:')
        print(ex)




if __name__== "__main__":
  main()