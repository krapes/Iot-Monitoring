# dependencies
import json
import logging
from validation import validate_packet
from lambda_decorators import  LambdaDecorator, cors_headers



log = logging.getLogger()
log.setLevel(logging.INFO)

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
    required_keys = []
    keys_dict = {}
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



@validate_and_return
def main(event, context):
    print("----- Start IotNames -------")
    try:

        response = {"names": ["francisco"]}
 
    except Exception as e:
        print(e)
        response = {"serverError": e.message}
                    
    return response


