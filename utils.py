# Setting a hard limit for max threads based on performance on my system. YMMV.
HARD_LIMIT_MAX_THREADS = 20

config_file = 'input.yaml'

# Adapted examples from
# https://docs.confluent.io/kafka-clients/python/current/overview.html 
# Decided to hardcode kafka_config for convenience, your usage might differ
kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true',
        'auto.commit.interval.ms': 5000
    }