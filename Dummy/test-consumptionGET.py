import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import consumptionGET
import json

class context:
   def __init__(self):
       setattr(self, "function_name", "consumptionGET")
       setattr(self, "invoked_function_arn", ":consumptionGET")


print(consumptionGET.main(json.loads(open('event-consumptionGET.json').read()), context()))