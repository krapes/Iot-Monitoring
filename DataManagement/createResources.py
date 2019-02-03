import boto3
import dataStaging

# Get the service resource.
session = boto3.session.Session()
stage = 'test'
region = session.region_name
sfn = session.client('stepfunctions', region_name=session.region_name)


local = True
print("DB location local: {}".format(local))
if local:
    dynamodb = boto3.resource('dynamodb', region_name=session.region_name, endpoint_url='http://localhost:8000')
    client = boto3.client('dynamodb', session.region_name, endpoint_url='http://localhost:8000')
else:
    dynamodb = boto3.resource('dynamodb', region_name=session.region_name)
    client = boto3.client('dynamodb', session.region_name)

def createStepFunction():
    sfn_name = 'consumptionSF-'+stage

    result = sfn.create_state_machine(
        name=sfn_name,
        definition = '{'
                     '"Comment": "Lythium-consumptionSF",'
                     '"StartAt": "dataStaging",'
                     '"States": {'
                     '"dataStaging": {'
                     '"Type": "Task",'
                     '"Resource": "arn:aws:lambda:'+region+':410775198449:function:DataManagement-'+stage+'-dataStaging",'
                        '"Catch": ['
                            '{'
                                '"ErrorEquals": ['
                                    '"States.TaskFailed"'
                                '],'
                                '"Next": "consumption"'
                            '}'
                        '],'
                        '"Next": "consumption"'
                     '},'
                     '"consumption": {'
                            '"Type": "Task",'
                            '"Resource": "arn:aws:lambda:'+region+':410775198449:function:DataManagement-'+stage+'-calcConsumption",'
                            '"End": true'
                      '}}}',
        roleArn='arn:aws:iam::410775198449:role/service-role/StatesExecutionRole'
    )

    print(result)

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


def fill_IotStaging2(event):
    response = dataStaging.main(event, "local")
    print("IotStaging2 filled with {} data points".format(len(response['liter'])))

print(client.list_tables()['TableNames'])


#createStepFunction()
delete_table("IotStaging2")
create_IotStaging2()
delete_table("IotStagingProgress")
create_IotStagingProgress()
fill_IotStaging2({"IotId": "francisco"})
#delete_table("IotStagging")

print(client.list_tables()['TableNames'])