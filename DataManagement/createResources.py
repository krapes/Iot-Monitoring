import boto3

# Get the service resource.
session = boto3.session.Session()


local = False
print("DB location local: {}".format(local))
if local:
    dynamodb = boto3.resource('dynamodb', region_name=session.region_name, endpoint_url='http://localhost:8000')
    client = boto3.client('dynamodb', session.region_name, endpoint_url='http://localhost:8000')
else:
    dynamodb = boto3.resource('dynamodb', region_name=session.region_name)
    client = boto3.client('dynamodb', session.region_name)



def create_IotStaging2():

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
    print("Deleting table {}".format(table_name))
    table = dynamodb.Table(table_name)
    table.delete()
    print("waiting")
    table.meta.client.get_waiter('table_not_exists').wait(TableName=table_name)
    print(client.list_tables()['TableNames'])


def create_IotStagingProgress():
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

        ],
        AttributeDefinitions=[
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

print(client.list_tables()['TableNames'])

delete_table("IotStaging2")
create_IotStaging2()
delete_table("IotStagingProgress")
create_IotStagingProgress()
#delete_table("IotStagging")

print(client.list_tables()['TableNames'])