# dependencies
import boto3
import json
import os
from boto3.dynamodb.conditions import Key
from json import JSONEncoder
import decimal
import logging
import ast
import random
random.seed(a=0)

log = logging.getLogger()
log.setLevel(logging.INFO)

import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'

# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]


def randomCharge(time):
    charge = []
    while random.random() < 0.5 or len(charge) == 0:
        start = random.randint(1, len(time) - 1)
        end = start + random.randrange(5, len(time) - (start))
        charge.append({"startDate": time[start], "endDate": time[end]})
    log.info(charge)
    return charge

def calcDischarge(liter, time):
    return randomCharge(time)

def calcRecharge(liter, time):
    return randomCharge(time)

def calcAbnormalcharge(liter, time):
    return randomCharge(time)


def date_handler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        else:
            raise TypeError

def getDataFromDynamo(event):
    def tableQuery(LastEvaluatedKey=None):
        if LastEvaluatedKey:
            return table.query(
                                KeyConditionExpression=Key('datetime').between(startDate, endDate) & Key('IotId').eq(
                                                        str(event['IotId'])),
                                ScanIndexForward=True,
                                ExclusiveStartKey=iotStaging.get('LastEvaluatedKey'),
                                ProjectionExpression="ADC,  #dt",
                                ExpressionAttributeNames={"#dt": "datetime"},
                            )
        else:
            return table.query(
                                KeyConditionExpression=Key('datetime').between(startDate, endDate) & Key('IotId').eq(
                                    str(event['IotId'])),
                                ScanIndexForward=True,
                                ProjectionExpression="ADC,  #dt",
                                ExpressionAttributeNames={"#dt": "datetime"},
                            )


    # set the client to connect to aws dynamo
    table_name = "IotStaging2"
    #dynamodb_resource = boto3.resource('dynamodb',region_name = region, endpoint_url='http://localhost:8000')
    dynamodb_resource = boto3.resource('dynamodb', region_name=region)
    table = dynamodb_resource.Table(table_name)

    startDate = event["startDate"]
    endDate = event["endDate"]
    iotStaging = tableQuery()

    # pagination
    while 'LastEvaluatedKey' in iotStaging:
        paged_result = tableQuery(iotStaging.get('LastEvaluatedKey'))

        if 'LastEvaluatedKey' in paged_result:
            iotStaging['LastEvaluatedKey'] = paged_result.get('LastEvaluatedKey')
        else:
            del iotStaging['LastEvaluatedKey']

        iotStaging['Items'] = iotStaging['Items'] + paged_result.get('Items')
        iotStaging['Count'] = iotStaging['Count'] + paged_result.get('Count')
    print("Item Count: {}".format(iotStaging['Count']))

    return iotStaging

def validate_packet(packet):
    def verify(packet, required_keys, keys_dict):
        for key in packet.keys():
            try:
                packet[key] = ast.literal_eval(packet[key])

            except Exception as e:
                log.info("Key: {}   packet[key]: {}   Exception: {}".format(key, packet[key], e))
                packet[key] = packet[key]

        if len(required_keys) > 0:
            exist = (lambda x: True if x in packet.keys() else x)
            required_validation = filter(lambda x: x is not True, map(exist, required_keys))
        else:
            required_validation = []


        log.info("required validation: {}".format(required_validation))

        type_check = (lambda x: True if x in keys_dict.keys()
                                        and type(packet[x]) in keys_dict[
                                            x if x in keys_dict.keys() else "default"] else x)
        validation = filter(lambda x: x is not True, map(type_check, packet.keys()))

        log.info("validation: {}".format(validation))
        return required_validation, validation

    log.info("----Start validate_packet-----")

    log.info(packet)

    required_keys = ["startDate", "endDate", "IotId"]

    keys_dict = { "startDate": [int], "endDate": [int],
            "IotId": [unicode, str],  "default": []}

    required_validation, validation = verify(packet, required_keys, keys_dict)

    if len(required_validation) > 0:
        packet = "The follow required elements are not present: {}".format(required_validation)

    elif len(validation) > 0:
        packet = "The follow keys are not accepted or are of the wrong type: {}".format(validation)


    return packet




'''
  @description => Main execution point for the api request
  @param object event => Input parameters received from the call
         object context => Environment data related to the compute layer
  @return void
'''
def main(event, context):
    print("Received Event")
    print("Event {}".format(event))
    try:

        packet = validate_packet(event["queryStringParameters"])
        log.info("packet: {}".format(packet))
        if type(packet) == str:
            return {
                        "isBase64Encoded": False,
                        "statusCode": 400,
                        "headers": {},
                        "body": json.dumps({"clientError": packet})
                    }

        records = getDataFromDynamo(packet)
        liter = [int(record['ADC']) for record in records['Items']]
        time = [int(record['datetime']) for record in records['Items']]

        data = {"liter": liter,
                "time": time,
                "discharge": calcDischarge(liter, time),
                "recharge": calcRecharge(liter, time),
                "abnormalcharge": calcAbnormalcharge(liter, time),
                "averageconsumption": {}
                }



        response = {
                   "isBase64Encoded": False,
                   "statusCode": 200,
                   "headers": {},
                   "body": json.dumps(data)
                  }





    except Exception as e:
        print(e)
        response = {
                    "isBase64Encoded": False,
                    "statusCode": 500,
                    "headers": {},
                    "body": json.dumps({"serverError": e.message})
                    }
    return response


