import requests
import json
import time
import hashlib
from confluent_kafka import Producer
import logging
import yaml

# Logger
def create_logger(name):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(name)
    return logger

def load_cfg(cfg_file):
    cfg = None
    with open(cfg_file, "r") as f:
        try:
            cfg = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    return cfg

logger = create_logger("kafka_logger")

def retrieve_user_data(url:str) -> dict:
    """Fetches random user data from the provided API endpoint."""
    response = requests.get(url)
    data = response.json()["results"][0]
    if data:
        logger.info("Retrieve data successfully!")
        return data
    else:
        logger.info("Can not fetch data from API!")

def transform_user_data(data: dict) -> dict:
    """Formats the fetched user data for Kafka streaming."""
    return {
        "name": f"{data['name']['title']}. {data['name']['first']} {data['name']['last']}",
        "gender": data["gender"],
        "address": f"{data['location']['street']['number']}, {data['location']['street']['name']}",  
        "city": data['location']['city'],
        "nation": data['location']['country'],  
        "zip": data['location']['postcode'],  
        "latitude": float(data['location']['coordinates']['latitude']),
        "longitude": float(data['location']['coordinates']['longitude']),
        "email": data["email"]
    }

def configure_kafka(servers: str):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': servers,
        'client.id': 'producer_instance'  
    }
    logger.info("Configure kafka")
    return Producer(**settings)

def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    logger.info(f"Publising data to topic {topic}...")
    try:
        producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_status)
        producer.flush()
    except Exception as e:
        logger.error(f"Error when publish to {topic}: {e}")

def delivery_status(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), '[Partition: {}]'.format(msg.partition()))

def initiate_stream():
    """Initiates the process to stream user data to Kafka."""
    cfg = load_cfg("/opt/airflow/sys_conf/config.yaml")
    if cfg:
        logger.info("Read system config successully")
    else:
        logger.error("Error with config URL")

    API_ENDPOINT = cfg['source']['api_endpoint']
    KAFKA_BOOTSTRAP_SERVERS = cfg['kafka']['brokers']

    KAFKA_TOPIC = cfg['kafka']['topic']
    PAUSE_INTERVAL = 10  
    STREAMING_DURATION = 120

    kafka_producer = configure_kafka(KAFKA_BOOTSTRAP_SERVERS)

    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        raw_data = retrieve_user_data(API_ENDPOINT)
        formatted_data = transform_user_data(raw_data)
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, formatted_data)
        time.sleep(PAUSE_INTERVAL)

initiate_stream()