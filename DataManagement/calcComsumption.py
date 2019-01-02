
from __future__ import print_function

import boto3
import datetime
from statistics import median
from boto3.dynamodb.conditions import Key, Attr

session = boto3.session.Session()
dynamodb = boto3.resource('dynamodb', region_name=session.region_name)
credentials = session.get_credentials().get_frozen_credentials()



def getData(IotId, startDate, endDate):
    table_name = 'IotStaging2'
    print("Reading the {} table.".format(table_name))

    table = dynamodb.Table(table_name)
    print("Connected to table: {}".format(table))

    try:

        response = table.scan(
            ProjectionExpression="ADC,  #dt",
            ExpressionAttributeNames={"#dt": "datetime"},
            FilterExpression=Key('datetime').between(startDate, endDate)
        )

        items = response['Items']
        while True:
            print("Items length {}".format(len(items) + len(response['Items'])))
            if response.get('LastEvaluatedKey'):
                response = table.scan(
                    ProjectionExpression="ADC,  #dt",
                    ExpressionAttributeNames={"#dt": "datetime"},
                    FilterExpression=Key('datetime').between(startDate, endDate),
                    ExclusiveStartKey=response['LastEvaluatedKey'])
                items += response['Items']
            else:
                break

    except Exception as e:
        print("Error: {}".format(e))


    return items


def main(event, context):

    print(event)
    startDate = event['startDate']
    endDate = event['endDate']
    IotId = event['IotId']
    timestep = event['timestep']

    try:

        # convert dates to dynamo key equalivent
        incomingDateFormat = '%d/%m/%Y %H:%M:%S'
        dynamoDateFormat = "%Y%m%d%H%M%S"
        dates = {}
        for sDate, dateKey in zip([startDate, endDate], ['startDate', 'endDate']):
            sDate = datetime.datetime.strptime(sDate, incomingDateFormat)
            sDate = int(sDate.strftime(dynamoDateFormat))
            dates[dateKey] = sDate

        # extract data between those dates
        data = getData(IotId, dates['startDate'], dates['endDate'])
        print(len(data))

        # calculate consumption for each timestep
        startACD = data[0]['ADC']
        startTime = data[0]['datetime']
        comsumptions = []
        for entry in data:
            time = entry['datetime']
            if time - startTime >= timestep:
                comsumptions.append(float(entry['ADC']) - float(startACD))
                startACD = entry['ADC']
                startTime = entry['datetime']
        print(comsumptions)
        print(data[0]['datetime'] - data[-1]['datetime'])

        # Find max, min, and mean
        response = event
        response['max'] = max(comsumptions)
        response['min'] = min(comsumptions)
        response['median'] = median(comsumptions)
        response['mean'] = sum(comsumptions) / len(comsumptions)
        response['mode'] = max(set(comsumptions), key=comsumptions.count)

    except Exception as e:
        print(e)
        raise Exception(e)

    print(response)
    return response
