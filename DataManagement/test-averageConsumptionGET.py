import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import averageConsumptionGET
import json

class context:
   def __init__(self):
       setattr(self, "function_name", "averageConsumptionGET")
       setattr(self, "invoked_function_arn", ":averageConsumptionGET")


print(averageConsumptionGET.main(json.loads(open('event-averageConsumptionGET.json').read()), context()))