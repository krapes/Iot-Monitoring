import boto3

# Get the service resource.
session = boto3.session.Session()
dynamodb = boto3.resource('dynamodb', region_name=session.region_name, endpoint_url='http://localhost:8000')


def create_IotStaging():

    # Create the DynamoDB table.
    table_name = 'IotStaging2'
    print("creating table {}".format(table_name))
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'IotId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'datetime',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'datetime',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'IotId',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    print("waiting")
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    print(table.item_count)

def delete_table(table_name):
    table = dynamodb.Table(table_name)
    table.delete()

def create_IotStaggingProgress():
    # Create the DynamoDB table.
    table_name = 'IotStagingProgress'
    print("creating table {}".format(table_name))
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'IotId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'datetime',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'datetime',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'IotId',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Wait until the table exists.
    print("waiting")
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    # Print out some data about the table.
    print(table.item_count)

#delete_table("IotStaging")
create_IotStaging()
#create_IotStaggingProgress()
#delete_table("IotStagging")

print(boto3.client('dynamodb', session.region_name, endpoint_url='http://localhost:8000').list_tables()['TableNames'])