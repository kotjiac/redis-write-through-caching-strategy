from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import json
import random
from time import sleep

SLEEP_BUFFER = 1

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])

    # Create Producer instance
    producer = Producer(config)
    # Subscribe to topic
    topic = "score-changes"

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced message to topic {topic}: partition=[{partition}] key={key:8} score={value:6}".format(topic=msg.topic(), partition=msg.partition(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Generate a random score
    def generate_randon_score(score):
        randon_score = score
        return randon_score

def main():
    try:
        while True:
            sleep(SLEEP_BUFFER)
            SCORE = random.randint(1000, 999999)
            #MESSAGE_KEY = random.choice(["fulano", "beltrano", "sicrano"])
            MESSAGE_KEY = str(random.randint(1, 3))
            randon_score = generate_randon_score(SCORE)
            randon_score_json = json.dumps(randon_score)
            producer.produce(
                topic, randon_score_json.encode("utf-8"), MESSAGE_KEY, callback=delivery_callback
            )
            producer.poll(1)  # wait UP TO 1 sec, callbacks invoked here

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()

if __name__ == "__main__":
    main()