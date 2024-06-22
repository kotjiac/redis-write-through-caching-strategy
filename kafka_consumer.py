import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer
from redis import Redis

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = "score-changes"
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                message_key = msg.key().decode('utf-8')
                score = json.loads(msg.value().decode("utf-8"))
                r = Redis(host='localhost', port=6379, decode_responses=True)
                r.hset('__{users:'+ str(message_key) +'}', mapping={'score': score})
                # Extract the (optional) key and value, and print.
                print("Consumed message from topic {topic}: partition=[{partition}] key={key:8} score={value:6}".format(topic=msg.topic(), partition=msg.partition(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()