import yaml

def read_config(config_file):
    # Adapted from
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/consumer.py
    
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    
    kafka_config = config.get('kafka', {})
    input_topics = kafka_config.get('input_topics', [])
    output_topic = kafka_config.get('output_topic', '')
    ip_mapping_file = config.get('ip_mapping_file', '')
    
    with open(ip_mapping_file, 'r') as file:
        ip_mapping = yaml.safe_load(file)
    
    return (input_topics, output_topic, ip_mapping)