# dependencies
import boto3
import json
import os
from boto3.dynamodb.conditions import Key
from json import JSONEncoder
import decimal
import logging
import ast
import math
import pandas as pd
import numpy as np



log = logging.getLogger()
log.setLevel(logging.INFO)

import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'

# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]


def identify_outliers(df, numB=100):

    df['bin'] = pd.cut(df.time, bins=numB, labels=False)
    bins = df.groupby(['bin'])['slope'].agg(['mean', 'count', 'std', 'sum'])
    bins['bin'] = df.bin.unique()

    ci_hi = []
    ci_lo = []
    for i in bins.index:
        m, c, s, summ, binn = bins.loc[i]
        ci_hi.append(m + 100.0*s/math.sqrt(c))
        ci_lo.append(m - 100.0*s/math.sqrt(c))

    bins['ci_hi'] = ci_hi
    bins['ci_lo'] = ci_lo

    labels = []
    color =[]
    previous_bin = -1

    try:
        for i, row  in df.iterrows():
            slope = row.slope
            if not np.isnan(slope):
                binn = row.bin
                if binn != previous_bin:
                    ci_hi = bins[bins['bin'] == binn].ci_hi.values[0]
                    ci_lo = bins[bins['bin'] == binn].ci_lo.values[0]
                    previous_bin = binn

                if slope >= ci_lo and slope <= ci_hi:
                    labels.append("normal")
                    color.append('b')

                else:
                    labels.append("outlier")
                    color.append('r')

            else:
                labels.append(np.nan)
                color.append('r')
    except Exception as e:
        print(i)
        raise(e)


    df['label'] = labels
    df['color'] = color
    
    return df, bins

def label_charges(df, bins, threshold=0.25):
    labels = list(df.label)
    color = list(df.color)
    for i, row in df.iterrows():
        binn = row.bin
        if row.label != 'outlier':
            if bins['mean'][binn] >= threshold:
                labels[i] = 'recharge'
                color[i] = 'g'
            elif bins['mean'][binn] <= -threshold:
                labels[i] = 'discharge'
                color[i] = 'm'

    df['label'] = labels
    df['color'] = color
    return df

def model_averageDischarge(df, numB=5000, timestep=3600):
    
    
    df['bin'] = pd.cut(df.time, bins=numB, labels=False)
    df = df[df['label'] == 'discharge']
    
    dt = []
    dl = []
    previous_b = -1
    for i, row in df.iterrows():
        if row.bin != previous_b:
            stime = df[df['bin'] == row['bin']]['time'].values[0]
            sliter = df[df['bin'] == row['bin']]['liter'].values[0]
        dt.append(row['time'] - stime)
        dl.append(row['liter'] - sliter)
        previous_b = row.bin
    df['refTime'] = dt
    df['refLiter'] = dl
    

    pcoeff1 = np.polyfit(list(df.refTime), list(df.refLiter), 1)

    return pcoeff1[0]*timestep

def create_timeRanges(df, bins, target):
    
    def merge_bins(bins):
        bins.reset_index(drop=True, inplace=True)
        intervals = []
        previous_bin =  bins['bin'][0]
        for i, row in bins.iterrows():
            binn = row.bin
            if binn == previous_bin + 1:
                intervals[-1].append(binn)
            else:
                intervals.append([binn])
            previous_bin = binn

        return intervals

    def extract_edges(df, intervals):
        t_intervals = []
        for interval in intervals:
            start, stop = df[df['bin'].isin(interval)].time.agg(['min', 'max'])
            t_intervals.append((start, stop))
        return t_intervals
    
    def reformat(intervals):
        master = []
        for interval in intervals:
            data = {"startDate": interval[0],
                     "endDate": interval[1]}
            master.append(data)
        return master

    target_bins = bins[bins['bin'].isin(df[df['label'] == target].bin.unique())]
    if len(target_bins) > 0:
        intervals = merge_bins(target_bins)
        t_interval = extract_edges(df, intervals)
        t_interval = reformat(t_interval)
    else:
        t_interval = []
    return t_interval


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
    print("Retrieving Data From {}".format(table_name))
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
            required_validation = list(required_validation)
        else:
            required_validation = []


        log.info("required validation: {}".format(required_validation))

        type_check = (lambda x: True if x in keys_dict.keys()
                                        and type(packet[x]) in keys_dict[
                                            x if x in keys_dict.keys() else "default"] else x)
        validation = list(filter(lambda x: x is not True, map(type_check, packet.keys())))

        log.info("validation: {}".format(validation))

        return required_validation, validation

    log.info("----Start validate_packet-----")

    log.info(packet)

    required_keys = ["startDate", "endDate", "IotId"]

    keys_dict = { "startDate": [int], "endDate": [int],
            "IotId": [str],  "default": []}

    required_validation, validation = verify(packet, required_keys, keys_dict)

    if len(required_validation) > 0:
        packet = "The follow required elements are not present: {}".format(required_validation)

    elif len(validation) > 0:
        pass
        #packet = "The follow keys are not accepted or are of the wrong type: {}".format(validation)


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

        packet = validate_packet(event)
        log.info("packet: {}".format(packet))
        if type(packet) == str:
            return {
                        "isBase64Encoded": False,
                        "statusCode": 400,
                        "headers": {},
                        "body": json.dumps({"clientError": packet})
                    }

        records = getDataFromDynamo(packet)
        log.info("records from dynamo: {}".format(records))
        liter = [int(record['ADC']) for record in records['Items']]
        time = [int(record['datetime']) for record in records['Items']]
        log.info("liter: {}  time: {}".format(liter, time))

        if len(liter) == 0:
            data =  {"liter": liter,
                    "time": time,
                    "discharge": [],
                    "recharge": [],
                    "abnormalcharge": [],
                    "averageconsumption": []
                    }
        else:
            df = pd.DataFrame(data={'liter': liter, 'time': time})
            df['slope'] = df.liter.diff()

            df, bins = identify_outliers(df)
            df = df[df['label'] != 'outlier']
            df.reset_index()
            
            df = label_charges(df, bins)

            data = {"liter": liter,
                    "time": time,
                    "discharge": create_timeRanges(df, bins, 'discharge'),
                    "recharge": create_timeRanges(df, bins, 'recharge'),
                    "abnormalcharge": create_timeRanges(df, bins, 'abnormal'),
                    "averageconsumption": model_averageDischarge(df.copy(), numB=5000)
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


