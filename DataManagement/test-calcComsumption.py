import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import calcConsumption
import json
import boto3

s3_client = boto3.resource('s3')

class context:
   def __init__(self):
       setattr(self, "function_name", "DataManagement")
       # setattr(self, "invoked_function_arn", ":receive_mail")
       setattr(self, "invoked_function_arn", ":dataStaging")


response = calcConsumption.main(
								json.loads(open('event-calcComsumption.json').read()),
	 							context())
print(response)
response = response.split('/')
bucket = response[0]
key = '/'.join(response[1:])
print(bucket, key)
results = s3_client.Bucket(bucket).Object(key=key).get()['Body'].read()

print(results)


