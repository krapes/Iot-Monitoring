import boto3
import json
import os
from boto3.dynamodb.conditions import Key
from json import JSONEncoder
import decimal
import logging
import ast
import signal
import time
from lambda_decorators import cors_headers

resource = "arn:aws:states:us-west-2:410775198449:stateMachine:consumptionSF-test"
executionArn_base = "arn:aws:states:us-west-2:410775198449:stateMachine:consumptionSF-test:"

def sigalrm_handler(signum, frame):
    # We get signal!
    raise TimeoutException()


log = logging.getLogger()
log.setLevel(logging.INFO)


# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]

sfn = boto3.client('stepfunctions', region_name=region)
s3_client = boto3.resource('s3')

def load_from_s3(url):
	url = url.split('/')
	bucket = url[0]
	key = '/'.join(url[1:])
	results = s3_client.Bucket(bucket).Object(key=key).get()['Body'].read().decode('utf-8')
	results = json.loads(results)
	return results


def start_SF(event, resource):
    log.info("-----Start SF------")
    log.info(event)

    

    response = sfn.start_execution(
        stateMachineArn=resource,
        input=json.JSONEncoder().encode(event)
    )

    return response['executionArn']#.split(":")[-1]

def monitor_SF(executionArn):
	log.info("-----MONITOR_SF: {} ------".format(executionArn))

	def describeSNF(executionArn):
	    response = sfn.describe_execution(
	        executionArn=executionArn
	    )
	    return response
	#executionArn = executionArn_base + executionArn
	print(executionArn)
	response = describeSNF(executionArn)
	log.info(response)
	while response['status'] == 'RUNNING':
		print(response['status'])
		time.sleep(1)
		response = describeSNF(executionArn)
	print(response['status'])
	log.info(response)
	data, name = (False, response['executionArn']) if response["status"] != "SUCCEEDED" else (
	response['output'], response['name'].split(":")[-1])
	data = ast.literal_eval(data)
	return data, name


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
        packet = "The follow keys are not accepted or are of the wrong type: {}".format(validation)


    return packet

@cors_headers
def main(event, context):
	start_time = time.time()
	print("Received Event")
	print("Event {}".format(event))
	try:

		packet = validate_packet(event["queryStringParameters"])
		log.info("packet: {}".format(packet))
		if type(packet) == str:
			return {
					"isBase64Encoded": False,
					"statusCode": 400,
					"headers": { 'Content-Type': 'application/json' },
					"body": json.dumps({"clientError": packet})
					}

		signal.signal(signal.SIGALRM, sigalrm_handler)
		time_limit = int(29 - (time.time() - start_time))
		signal.alarm(time_limit)
		print("Begining Exe with time limit of {}".format(time_limit))
		try:
			executionArn = (start_SF(packet, resource) if 'executionArn' not in packet.keys()
			 											else packet['executionArn'])
			url, name = monitor_SF(executionArn)

			print("Returned Data URL: {}".format(url))
			data = load_from_s3(url)


		except Exception as e:
			print(e)
			print("TimeoutException Triggered")
			data = {'executionArn': executionArn}
			response = {
						"isBase64Encoded": False,
						"statusCode": 200,
						"headers": { 'Content-Type': 'application/json' },
						"body": json.dumps(data)
						}
			print(response)
			return response
		
		data = {"liter": data['liter'],
				"time": data['time'],
				"discharge": data['discharge'],
				"recharge": data['recharge'],
				"abnormalcharge": data['abnormalcharge']
				}

		response = {
					"isBase64Encoded": False,
					"statusCode": 200,
					"headers": { 'Content-Type': 'application/json' },
					"body": json.dumps(data)
					}





	except Exception as e:
	    print(e)
	    response = {
	                "isBase64Encoded": False,
	                "statusCode": 500,
	                "headers": { 'Content-Type': 'application/json' },
	                "body": json.dumps({"serverError": e})
	                }
	return response