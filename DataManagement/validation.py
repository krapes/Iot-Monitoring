import logging
import ast

log = logging.getLogger()
log.setLevel(logging.INFO)

def validate_packet(packet, required_keys, keys_dict):
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

    keys_dict["default"] = []

    required_validation, validation = verify(packet, required_keys, keys_dict)

    if len(required_validation) > 0:
        packet = "The follow required elements are not present: {}".format(required_validation)

    elif len(validation) > 0:
        packet = "The follow keys are not accepted or are of the wrong type: {}".format(validation)


    return packet