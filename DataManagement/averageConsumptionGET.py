# dependencies
import boto3
import json
import os
from boto3.dynamodb.conditions import Key
from json import JSONEncoder
import decimal

# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]

def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        else:
            raise TypeError

def getDataFromDynamo(event):
    # set the client to connect to aws dynamo
    table_name = "IotStaging2"
    dynamodb_resource = boto3.resource('dynamodb',region_name = region)
    table = dynamodb_resource.Table(table_name)
    #@TODO revoew the starDate and endDate values because they are a string and should be a datetime integer

    startDate = int(event["startDate"].replace("-","").replace(":","").replace(" ",""))
    endDate = int(event["endDate"].replace("-","").replace(":","").replace(" ",""))
    iotStaging = table.query(
        KeyConditionExpression= Key('datetime').between(startDate, endDate) & Key('IotId').eq(str(event['iotId'])),
        ScanIndexForward = True
    )

    # pagination
    while 'LastEvaluatedKey' in iotStaging:
        paged_result = table.query(
            KeyConditionExpression=Key('IotId').eq(str(event['iotId'])) & Key('datetime').between(startDate, endDate),
            ScanIndexForward=True,
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

           if event['operation'] == "avg":
               avg =  records["Items"][0]["ADC"] - records["Items"][-1]["ADC"]
               response = {"code": 200, "data": {"avg":avg}}
           elif event['operation'] == "max":
               max = 0
               time = ""
               for re in records["Items"]:
                    if re["ADC"]>max:
                        max = re["ADC"]
                        time = str(re["dateStart"])
               response = {"code": 200, "data": {"max": max,"timestamp":time}}

       else:
           response = {"code": 400, "data": {"clientError": "Invalid Parameters"}}

       return JSONEncoder(default=date_handler).encode(response)
    except Exception as e:
        print(e)
        return json.dumps({"code": 500, "data": {"serverError": e.message}})


