from confluent_kafka import Producer
import json

def write_kafka(output_topic, json_data, kafka_config):
    # Adapted from
    # https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/producer.py

    producer = Producer(**kafka_config)
    message = json.dumps(json_data)
    def delivery_report(err, msg):
        if err is not None:
            print('Message delivery failed', err)
        else:
            print('Message delivered to', msg.topic())

    producer.produce(output_topic, value=message.encode('utf-8'), callback=delivery_report)
    producer.flush()