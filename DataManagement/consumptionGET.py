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
from lambda_decorators import  LambdaDecorator, cors_headers
from validation import validate_packet

resource = "arn:aws:states:us-west-2:410775198449:stateMachine:consumptionSF-test"
executionArn_base = "arn:aws:states:us-west-2:410775198449:execution:consumptionSF-test:"

log = logging.getLogger()
log.setLevel(logging.INFO)


# constants
global region, stage
region = os.environ["region"]
stage = os.environ["stage"]

sfn = boto3.client('stepfunctions', region_name=region)
s3_client = boto3.resource('s3')


class validate_and_return(LambdaDecorator):

	@staticmethod
	@cors_headers
	def build_outgoing(statusCode, body):
		outgoing = {
					"isBase64Encoded": False,
					"statusCode": statusCode,
					"headers": { 'Content-Type': 'application/json'},
					"body": json.dumps(body)
					}
		log.info("Outgoing Message: {}".format(outgoing))
		return outgoing

	def before(self, event, context):
		print("Event {}".format(event))
		required_keys = ["startDate", "endDate", "IotId"]
		keys_dict = { "startDate": [int], "endDate": [int],
						"IotId": [str], "executionId": [str]}
		packet = validate_packet(event["queryStringParameters"], required_keys, keys_dict)
		log.info(packet)
		if type(packet) == str:
			raise Exception("ClientError: " + packet)
		return packet, context

	
	def after(self, retval):
		return self.build_outgoing(200, retval)

	def on_exception(self, exception):
		if "ClientError: " in str(exception):
			return self.build_outgoing(400, str(exception))
		else:
			return self.build_outgoing(500, str(exception))

class Timeout():
    """Timeout class using ALARM signal."""
    class Timeout(Exception):
        pass
 
    def __init__(self, sec):
        self.sec = sec
 
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)
        log.info("Begining Exe with time limit of {}".format(self.sec))
 
    def __exit__(self, *args):
        signal.alarm(0)    # disable alarm
 
    def raise_timeout(self, *args):
        raise Timeout.Timeout()

def load_from_s3(url):
	log.info("------- Loading Files from s3------")
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

    return response['executionArn'].split(":")[-1]

def monitor_SF(executionId):
	log.info("-----MONITOR_SF: {} ------".format(executionId))

	def describeSNF(executionArn):
	    response = sfn.describe_execution(
	        executionArn=executionArn
	    )
	    return response

	executionArn = executionArn_base + executionId
	response = describeSNF(executionArn)
	log.info(response)
	while response['status'] == 'RUNNING':
		print(response['status'])
		time.sleep(1)
		response = describeSNF(executionArn)
	print(response['status'])
	log.info(response)
	data, name = (False, executionId) if response["status"] != "SUCCEEDED" else (
	response['output'], executionId)
	data = ast.literal_eval(data)
	return data, name






@validate_and_return
def main(event, context):
	start_time = time.time()
	print("-----Start Main-----")
	
	try:

		log.info("packet: {}".format(event))
		time_limit = int(28 - (time.time() - start_time))
		try:
			with Timeout(time_limit):
				executionId = (start_SF(event, resource) if 'executionId' not in event.keys()
				 											else event['executionId'])
				url, name = monitor_SF(executionId)

				print("Returned Data URL: {}".format(url))
				data = load_from_s3(url)
				print("Data Retrieved: Keys: {}".format(data.keys()))

		except Timeout.Timeout:
			print("TimeoutException Triggered")
			data = {'executionId': executionId}
			return data
		
		data = {"liter": data['liter'],
				"time": data['time'],
				"discharge": data['discharge'],
				"recharge": data['recharge'],
				"abnormalcharge": data['abnormalcharge']
				}

	except Exception as e:
	    print(e)

	return data