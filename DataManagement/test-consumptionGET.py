import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import consumptionGET
import json

class context:
   def __init__(self):
       setattr(self, "function_name", "averageConsumptionGET")
       setattr(self, "invoked_function_arn", ":averageConsumptionGET")


results = consumptionGET.main(json.loads(open('event-consumptionGET.json').read()), context())

print("Status code: {}".format(results['statusCode']))
for key in results.keys():
	print(key, type(results[key]))
	print(results[key])
print("Process ended with exit code 0")