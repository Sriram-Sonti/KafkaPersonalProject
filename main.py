import sys
import utils
from confluent_kafka import Consumer, KafkaError
from read import read_config
from process_input import process
from write_to_topic import write_kafka
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_and_write(topic, kafka_config, config):
    # Adapted from
    # https://github.com/confluentinc/confluent-kafka-python
    c = Consumer(kafka_config)
    c.subscribe(topic)
    
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Process message
            processed_msg = process(msg, config[2])

            # Write to output topic
            write_kafka(config[1], processed_msg, kafka_config)
    
    except KeyboardInterrupt:
        print('Stopped consuming topic:', topic)
    finally:
        c.close()

def main(max_threads):
    print('Using', max_threads, ' threads')

    # Get data for topics, ip_mapping etc from config file
    config = read_config(utils.config_file)
    print(config)
    
    # Decided to go for round robin approach if # of input topics > max_threads
    topics_per_thread = {i: [] for i in range(max_threads)}
    for i, topic in enumerate(config[0]):
        topics_per_thread[i % max_threads].append(topic)
    
    # Referenced https://gist.github.com/angad/9800379 
    # and https://superfastpython.com/threadpoolexecutor-as-completed/
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        # futures = [executor.submit(process_and_write, topics_per_thread[i], utils.kafka_config, config) for i in range(max_threads)]
        futures = [executor.submit(process_and_write, topic, utils.kafka_config, config) for topic in topics_per_thread.values()]

        try:
            for future in as_completed(futures):
                future.result()
        except KeyboardInterrupt:
            print('Stopped by user')

if __name__ == "__main__":
    max_threads = 1
    if len(sys.argv) != 2:
        main(max_threads)
    else:
        if not sys.argv[1].isdigit():
            print('Invalid input. Number of threads must be a number. Exiting.')
            sys.exit(1)
        
        max_threads = int(sys.argv[1])
        if max_threads > utils.HARD_LIMIT_MAX_THREADS:
            max_threads = utils.HARD_LIMIT_MAX_THREADS
            print('WARNING: Number of threads cannot exceed', utils.HARD_LIMIT_MAX_THREADS)

        main(max_threads)
