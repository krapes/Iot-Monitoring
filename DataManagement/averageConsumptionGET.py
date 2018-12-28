# dependencies
import boto3
import json
import os
from boto3.dynamodb.conditions import Key

# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]

def getDataFromDynamo(event):
    # set the client to connect to aws dynamo
    table_name = "IotStaging"
    dynamodb_resource = boto3.resource('dynamodb',region_name = region)
    table = dynamodb_resource.Table(table_name)
    #@TODO revoew the starDate and endDate values because they are a string and should be a datetime integer
    iotStaging = table.query(
        KeyConditionExpression=Key('IotId').eq(str(event['iotId'])) & Key('datetime').between(str(event["startDate"]), str(event['endDate']))
    )

    # pagination
    while 'LastEvaluatedKey' in iotStaging:
        paged_result = table.query(
            KeyConditionExpression=Key('IotId').eq(str(event['iotId'])) & Key('datetime').between(
                str(event["startDate"]), str(event['endDate'])),
            ExclusiveStartKey=iotStaging.get('LastEvaluatedKey')
        )

        if 'LastEvaluatedKey' in paged_result:
            iotStaging['LastEvaluatedKey'] = paged_result.get('LastEvaluatedKey')
        else:
            del iotStaging['LastEvaluatedKey']

        iotStaging['Items'] = iotStaging['Items'] + paged_result.get('Items')
        iotStaging['Count'] = iotStaging['Count'] + paged_result.get('Count')

    return iotStaging

def isValidInput(event):

    # check if all data is in the request
    if "startDate" not in event or "endDate" not in event or "iotId" not in event:
        return False

    #@TODO check datetime types
    return True




'''
  @description => Main execution point for the api request
  @param object event => Input parameters received from the call
         object context => Environment data related to the compute layer
  @return void
'''
def main(event, context):
    print("Received Event")
    print(json.dumps(event))
    try:
       if isValidInput(event):
           records = getDataFromDynamo(event)



    except Exception as e:
        print(e)
        raise Exception(e)


