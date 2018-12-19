import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import dataStaging
import json

class context:
   def __init__(self):
       setattr(self, "function_name", "DataManagement")
       # setattr(self, "invoked_function_arn", ":receive_mail")
       setattr(self, "invoked_function_arn", ":dataStaging")


print(dataStaging.main(json.loads(open('event-dataStaging.json').read()), context()))