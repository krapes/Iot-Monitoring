import os
os.environ['stage'] = 'test'
os.environ['region'] = 'us-west-2'


import consumptionGET
import json
import time

class context:
   def __init__(self):
       setattr(self, "function_name", "consumptionGET")
       setattr(self, "invoked_function_arn", ":consumptionGET")

start = time.time()
response = consumptionGET.main(json.loads(open('event-consumptionGET.json').read()), context())
print("\n\n")
print("Total Working Time: {}".format(time.time() - start))
for key in response.keys():
	print(key)
	if key != "body":
		print(response[key])
	print("")
