# Run get info from Elasticsearch  from AWS Lambda.
from __future__ import print_function

import boto3
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth
import json
import datetime

session = boto3.session.Session()
#dynamodb = boto3.resource('dynamodb', region_name=session.region_name, endpoint_url='http://localhost:8000')
dynamodb = boto3.resource('dynamodb', region_name=session.region_name)
credentials = session.get_credentials().get_frozen_credentials()
# NOTE: REMOVE https://
esendpoint = 'search-lythium-petroleo-mef30-r6mjhvjxxdnuscouv53hulfrie.us-west-2.es.amazonaws.com'
#credentials = session.get_credentials().get_frozen_credentials()

awsauth = AWSRequestsAuth(
    aws_access_key=credentials.access_key,
    aws_secret_access_key=credentials.secret_key,
    aws_token=credentials.token,
    aws_host=esendpoint,
    aws_region=session.region_name,
    aws_service='es'
)


def getResponses(event):
    # Put the user query into the query DSL for more accurate search results.

    host = esendpoint  # For example, search-mydomain-id.us-west-1.es.amazonaws.com
    index = event['IotId']  # 'francisco'
    url = 'https://' + host + '/' + index + '/_search'

    query = {
        "sort": [{"datetime": {"order": "asc"}}],
        "size": event['size'],
        "query": {"match_all": {}}
    }

    if event['startDT'] != None:
        query['query'] = {"bool": {"filter": [{"range": {"datetime": {"gte": event['startDT']}}}]}}

    # ES 6.x requires an explicit Content-Type header
    headers = {"Content-Type": "application/json"}

    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": '*'},
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = json.loads(r.text)
    return response


def calculateItems(entries, IotId, timestep=60):
    startDT = entries[0]['_source']['datetime']
    startDT_i = 0
    acd_sum = 0
    items = []
    for i, entry in enumerate(entries):
        entry = entry['_source']
        if entry['ADC'] != None:
            acd_sum += entry['ADC']
            try:
                startDate = entries[startDT_i]['_source']['date']
                endDate = entry['date']
                sDate = datetime.datetime.strptime(startDate, '%d/%m/%Y %H:%M:%S')
                eDate = datetime.datetime.strptime(endDate, '%d/%m/%Y %H:%M:%S')

                if (eDate - sDate).total_seconds() >= timestep:
                    count = i - startDT_i
                    avg = int(acd_sum / count)

                    stagingDatetime = sDate + (eDate - sDate) / 2
                    stagingDatetime = int(stagingDatetime.strftime("%Y%m%d%H%M%S"))

                    item = {'ADC': avg,
                            'esdatetimeStart': startDT,
                            'esdatetimeEnd': entry['datetime'],
                            'esdatetime': int((startDT + entry['datetime']) / 2),
                            'datetime': stagingDatetime,
                            'dateStart': startDate,
                            'dateEnd': endDate,
                            'IotId': IotId
                            }
                    items.append(item)
                    startDT = entry['datetime']
                    startDT_i = i
                    acd_sum = 0
                    # print(items[-2]['dateEnd'], items[-1]['dateStart'])
            except Exception as e:
                print("Exception")
                print(e)
                print(i, entry)
        else:
            print(i, entry)

    print("Created {} Items".format(len(items)))
    return items


def putToTable(table_name, items):
    print("Writing in the {} table.".format(table_name))

    table = dynamodb.Table(table_name)
    print("Connected to table: {}".format(table))

    for item in items:
        table.put_item(
            Item=item
        )
    print("Put {} items to table {}".format(len(items), table_name))


def validCheck(key, event):
    if key in event and event[key] != None:
        return True
    else:
        return False


def getStartDT(IotId):
    table_name = 'IotStagingProgress'
    print("Reading the {} table.".format(table_name))

    table = dynamodb.Table(table_name)
    print("Connected to table: {}".format(table))

    try:
        response = table.get_item(Key={'IotId': IotId})
        startDT = int(response['Item']['datetime'])
    except Exception as e:
        print(e)
        startDT = None
        print("startDT = {}".format(startDT))

    return startDT


def main(event, context):
    print(event)
    try:
        keyDefault = [('size', 10000), ('startDT', getStartDT(event['IotId']))]
        for key, default in keyDefault:
            event[key] = event[key] if validCheck(key, event) else default

        print("event: {}".format(event))
        paginate = True
        itterations = 0
        liters = []
        time = []

        esliters = []
        estime = []

        while paginate == True:
            print("Starting Iteration {}".format(itterations))
            itterations += 1
            response = getResponses(event)
            # print(response)

            entries = response['body']['hits']['hits']
            try:
                esliters += [int(entry['_source']['ADC']) for entry in entries]
                estime += [int(
                    datetime.datetime.strptime(entry['_source']['date'], '%d/%m/%Y %H:%M:%S').strftime("%Y%m%d%H%M%S"))
                           for entry in entries]
            except Exception as e:
                print("es exception: {}".format(e))
                pass

            if len(entries) < 10000:
                paginate = False
            items = calculateItems(entries, event['IotId'])
            try:
                liters += [int(entry['ADC']) for entry in items]
                time += [int(entry['datetime']) for entry in items]
            except Exception as e:
                print("items exception: {}".format(e))
                pass

            if len(items) >= 1:
                # print(event['startDT'], items[-1]['datetimeEnd'])
                event['startDT'] = items[-1]['esdatetimeEnd']
                putToTable("IotStaging2", items)
                progressItem = {'IotId': event['IotId'],
                                'datetime': event['startDT']}
                putToTable('IotStagingProgress', [progressItem])

        print("Done in {} itterations".format(itterations))
    except Exception as e:
        print(e)
        raise Exception(e)

    response = event
    response['time'] = time
    response['liter'] = liters
    response['estime'] = estime
    response['esliter'] = esliters
    return response

