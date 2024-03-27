import json

def flatten_json(y):
    # Adapted from
    # https://www.geeksforgeeks.org/flattening-json-objects-in-python/

    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def replace_ip_with_hostname(flattened_msg, ip_mapping):
    for key in list(flattened_msg.keys()):
        if 'ip_address' in key:
            ip_address = flattened_msg[key]
            if ip_address in ip_mapping:
                flattened_msg[key] = ip_mapping[ip_address]
    return flattened_msg

def add_topic_value(topic, msg):
    msg['topic_name'] = topic
    return msg

def process(msg, ip_mapping):
    curr_json = json.loads(msg.value().decode('utf-8'))

    # Step 1: Flatten
    flattened_msg = flatten_json(curr_json)

    # Step 2: Replace IPs with hostnames
    ip_replaced_msg = replace_ip_with_hostname(flattened_msg, ip_mapping)

    # Step 3: Add topic name to output message
    processed_msg = add_topic_value(msg.topic(), ip_replaced_msg)

    print('Received message: {}'.format(curr_json))
    print('Returning ', processed_msg)
    return processed_msg