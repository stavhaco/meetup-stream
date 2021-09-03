from kafka import KafkaProducer
import configparser
import requests
import logging

def generate_data(request_url):
    try:
        rq = requests.get(request_url, stream=True)
        if rq.status_code == 200:
            for chunk in rq.iter_lines():
                yield chunk
    except Exception as ex:
        logging.error(f'Connection to meetup data could not established, {ex}')

def publish_message(producer_instance, topic_name, request_url):
    try:
        data = generate_data(request_url)
        for item in data:
            producer_instance.send(topic_name, item)
            logging.info('Message published successfully.')
    except Exception as ex:
        logging.error(f'Message could not sent, {ex}')

if __name__ == '__main__':
    config = configparser.ConfigParser()
    path = 'conf.ini'
    config.read(path)

    server = config['DEFAULT']['server']
    topic_name = config['DEFAULT']['topic_name']
    request_url = config['DEFAULT']['request_url']
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
    producer = KafkaProducer(bootstrap_servers=[server])
    if producer:
        publish_message(producer, topic_name, request_url)